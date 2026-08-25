package main

import (
	"context"
	"io"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	boxCertificate "github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/certificate"
	"github.com/sagernet/sing-box/common/httpclient"
	"github.com/sagernet/sing-box/common/netns"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type trafficStatisticsMigrationRuntime struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	logFactory          log.Factory
	logger              log.ContextLogger
	certificateStore    *certificate.Store
	networkNamespace    *netns.Manager
	network             *route.NetworkManager
	endpoint            *endpoint.Manager
	outbound            *outbound.Manager
	dnsTransport        *dns.TransportManager
	certificateProvider *boxCertificate.Manager
	dnsRouter           *dns.Router
	connection          *route.ConnectionManager
	router              *route.Router
	httpClient          *httpclient.Manager
	started             bool
	closeOnce           sync.Once
	closeErr            error
}

func newTrafficStatisticsMigrationRuntime(
	parent context.Context,
	options option.Options,
	stderr io.Writer,
) (*trafficStatisticsMigrationRuntime, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	ctx = include.Context(service.ContextWithDefaultRegistry(ctx))
	ctx = pause.WithDefaultManager(ctx)
	if service.PtrFromContext[urltest.HistoryStorage](ctx) == nil {
		ctx = service.ContextWithPtr(ctx, urltest.NewHistoryStorage())
	}
	logFactory, err := log.New(log.Options{
		Context:       ctx,
		Options:       common.PtrValueOrDefault(options.Log),
		DefaultWriter: stderr,
	})
	if err != nil {
		cancel()
		return nil, E.Cause(err, "create traffic migration log factory")
	}
	service.MustRegister[log.Factory](ctx, logFactory)
	runtime := &trafficStatisticsMigrationRuntime{
		ctx:        ctx,
		cancel:     cancel,
		logFactory: logFactory,
		logger:     logFactory.NewLogger("traffic-statistics-migration"),
	}
	constructed := false
	defer func() {
		if !constructed {
			_ = runtime.Close()
		}
	}()

	routeOptions := common.PtrValueOrDefault(options.Route)
	dnsOptions := common.PtrValueOrDefault(options.DNS)
	certificateOptions := common.PtrValueOrDefault(options.Certificate)
	if C.IsAndroid ||
		certificateOptions.Store != "" &&
			certificateOptions.Store != C.CertificateStoreSystem ||
		len(certificateOptions.Certificate) > 0 ||
		len(certificateOptions.CertificatePath) > 0 ||
		len(certificateOptions.CertificateDirectoryPath) > 0 {
		runtime.certificateStore, err = certificate.NewStore(
			ctx,
			logFactory.NewLogger("certificate"),
			certificateOptions,
		)
		if err != nil {
			return nil, err
		}
		service.MustRegister[adapter.CertificateStore](ctx, runtime.certificateStore)
	}
	runtime.networkNamespace, err = netns.NewManager(
		logFactory.NewLogger("netns"),
		options.NetworkNamespaces,
		[]string{"/proc/self/exe", commandNetnsHolder.Use},
	)
	if err != nil {
		return nil, err
	}
	service.MustRegister[adapter.NetworkNamespaceManager](ctx, runtime.networkNamespace)

	endpointRegistry := service.FromContext[adapter.EndpointRegistry](ctx)
	inboundRegistry := service.FromContext[adapter.InboundRegistry](ctx)
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	dnsTransportRegistry := service.FromContext[adapter.DNSTransportRegistry](ctx)
	certificateProviderRegistry := service.FromContext[adapter.CertificateProviderRegistry](ctx)
	if endpointRegistry == nil ||
		inboundRegistry == nil ||
		outboundRegistry == nil ||
		dnsTransportRegistry == nil ||
		certificateProviderRegistry == nil {
		return nil, E.New("missing traffic migration runtime registry")
	}
	runtime.endpoint = endpoint.NewManager(
		logFactory.NewLogger("endpoint"),
		endpointRegistry,
	)
	emptyInbound := inbound.NewManager(
		logFactory.NewLogger("inbound"),
		inboundRegistry,
		runtime.endpoint,
	)
	runtime.outbound = outbound.NewManager(
		logFactory.NewLogger("outbound"),
		outboundRegistry,
		runtime.endpoint,
		routeOptions.Final,
	)
	runtime.dnsTransport = dns.NewTransportManager(
		logFactory.NewLogger("dns/transport"),
		dnsTransportRegistry,
		runtime.outbound,
		dnsOptions.Final,
	)
	runtime.certificateProvider = boxCertificate.NewManager(
		logFactory.NewLogger("certificate-provider"),
		certificateProviderRegistry,
	)
	service.MustRegister[adapter.EndpointManager](ctx, runtime.endpoint)
	service.MustRegister[adapter.InboundManager](ctx, emptyInbound)
	service.MustRegister[adapter.OutboundManager](ctx, runtime.outbound)
	service.MustRegister[adapter.DNSTransportManager](ctx, runtime.dnsTransport)
	service.MustRegister[adapter.CertificateProviderManager](
		ctx,
		runtime.certificateProvider,
	)
	runtime.dnsRouter, err = dns.NewRouter(ctx, logFactory, dnsOptions)
	if err != nil {
		return nil, E.Cause(err, "initialize traffic migration DNS router")
	}
	service.MustRegister[adapter.DNSRouter](ctx, runtime.dnsRouter)
	service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, runtime.dnsRouter)
	runtime.network, err = route.NewNetworkManager(
		ctx,
		logFactory.NewLogger("network"),
		routeOptions,
		dnsOptions,
	)
	if err != nil {
		return nil, E.Cause(err, "initialize traffic migration network manager")
	}
	service.MustRegister[adapter.NetworkManager](ctx, runtime.network)
	runtime.connection = route.NewConnectionManager(
		logFactory.NewLogger("connection"),
	)
	service.MustRegister[adapter.ConnectionManager](ctx, runtime.connection)
	if trafficStatisticsMigrationNeedsHTTPClient(options) {
		runtime.httpClient = httpclient.NewManager(
			ctx,
			logFactory.NewLogger("httpclient"),
			options.HTTPClients,
			routeOptions.DefaultHTTPClient,
		)
		service.MustRegister[adapter.HTTPClientManager](ctx, runtime.httpClient)
		runtime.httpClient.Initialize(func() (*httpclient.ManagedTransport, error) {
			var httpClientOptions option.HTTPClientOptions
			httpClientOptions.DefaultOutbound = true
			return httpclient.NewTransport(
				ctx,
				logFactory.NewLogger("httpclient"),
				"",
				httpClientOptions,
			)
		})
	}
	runtime.router = route.NewRouter(ctx, logFactory, routeOptions, dnsOptions)
	service.MustRegister[adapter.Router](ctx, runtime.router)
	if err = runtime.router.Initialize(nil, nil); err != nil {
		return nil, E.Cause(err, "initialize traffic migration router")
	}
	for index, serverOptions := range dnsOptions.Servers {
		tag := serverOptions.Tag
		if tag == "" {
			tag = F.ToString(index)
		}
		if err = runtime.dnsTransport.Create(
			ctx,
			logFactory.NewLogger(F.ToString(
				"dns/",
				serverOptions.Type,
				"[",
				tag,
				"]",
			)),
			tag,
			serverOptions.Type,
			serverOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize traffic migration DNS server[", index, "]")
		}
	}
	if err = runtime.dnsRouter.Initialize(dnsOptions.Rules); err != nil {
		return nil, E.Cause(err, "initialize traffic migration DNS rules")
	}
	for index, endpointOptions := range options.Endpoints {
		tag := endpointOptions.Tag
		if tag == "" {
			tag = F.ToString(index)
		}
		endpointCtx := ctx
		if tag != "" {
			endpointCtx = adapter.WithContext(
				endpointCtx,
				&adapter.InboundContext{Outbound: tag},
			)
		}
		if err = runtime.endpoint.Create(
			endpointCtx,
			runtime.router,
			logFactory.NewLogger(F.ToString(
				"endpoint/",
				endpointOptions.Type,
				"[",
				tag,
				"]",
			)),
			tag,
			endpointOptions.Type,
			endpointOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize traffic migration endpoint[", index, "]")
		}
	}
	for index, outboundOptions := range options.Outbounds {
		tag := outboundOptions.Tag
		if tag == "" {
			tag = F.ToString(index)
		}
		outboundCtx := ctx
		if tag != "" {
			outboundCtx = adapter.WithContext(
				outboundCtx,
				&adapter.InboundContext{Outbound: tag},
			)
		}
		if err = runtime.outbound.Create(
			outboundCtx,
			runtime.router,
			logFactory.NewLogger(F.ToString(
				"outbound/",
				outboundOptions.Type,
				"[",
				tag,
				"]",
			)),
			tag,
			outboundOptions.Type,
			outboundOptions.Options,
		); err != nil {
			return nil, E.Cause(err, "initialize traffic migration outbound[", index, "]")
		}
	}
	for index, providerOptions := range options.CertificateProviders {
		tag := providerOptions.Tag
		if tag == "" {
			tag = F.ToString(index)
		}
		if err = runtime.certificateProvider.Create(
			ctx,
			logFactory.NewLogger(F.ToString(
				"certificate-provider/",
				providerOptions.Type,
				"[",
				tag,
				"]",
			)),
			tag,
			providerOptions.Type,
			providerOptions.Options,
		); err != nil {
			return nil, E.Cause(
				err,
				"initialize traffic migration certificate provider[",
				index,
				"]",
			)
		}
	}
	runtime.outbound.Initialize(func() (adapter.Outbound, error) {
		return direct.NewOutbound(
			ctx,
			runtime.router,
			logFactory.NewLogger("outbound/direct"),
			"direct",
			option.DirectOutboundOptions{},
		)
	})
	runtime.dnsTransport.Initialize(func() (adapter.DNSTransport, error) {
		return dnsTransportRegistry.CreateDNSTransport(
			ctx,
			logFactory.NewLogger("dns/local"),
			"local",
			C.DNSTypeLocal,
			&option.LocalDNSServerOptions{},
		)
	})
	constructed = true
	return runtime, nil
}

func trafficStatisticsMigrationNeedsHTTPClient(options option.Options) bool {
	for _, outboundOptions := range options.Outbounds {
		if outboundOptions.Type != C.TypeHysteria2 {
			continue
		}
		switch hysteriaOptions := outboundOptions.Options.(type) {
		case *option.Hysteria2OutboundOptions:
			if hysteriaOptions != nil && hysteriaOptions.Realm != nil {
				return true
			}
		case option.Hysteria2OutboundOptions:
			if hysteriaOptions.Realm != nil {
				return true
			}
		}
	}
	return false
}

func (r *trafficStatisticsMigrationRuntime) Context() context.Context {
	return r.ctx
}

func (r *trafficStatisticsMigrationRuntime) Logger() log.ContextLogger {
	return r.logger
}

func (r *trafficStatisticsMigrationRuntime) Start() error {
	if r.started {
		return nil
	}
	if err := r.logFactory.Start(); err != nil {
		return E.Cause(err, "start traffic migration logger")
	}
	if r.certificateStore != nil {
		if err := adapter.StartNamed(
			r.ctx,
			r.logger,
			adapter.StartStateInitialize,
			[]adapter.LifecycleService{r.certificateStore},
		); err != nil {
			return err
		}
	}
	if err := adapter.StartNamed(
		r.ctx,
		r.logger,
		adapter.StartStateInitialize,
		[]adapter.LifecycleService{r.networkNamespace},
	); err != nil {
		return err
	}
	if err := adapter.Start(
		r.ctx,
		r.logger,
		adapter.StartStateInitialize,
		r.network,
		r.dnsTransport,
		r.dnsRouter,
		r.connection,
		r.router,
		r.outbound,
		r.endpoint,
		r.certificateProvider,
	); err != nil {
		return err
	}
	if r.certificateStore != nil {
		if err := adapter.StartNamed(
			r.ctx,
			r.logger,
			adapter.StartStateStart,
			[]adapter.LifecycleService{r.certificateStore},
		); err != nil {
			return err
		}
	}
	if err := adapter.Start(
		r.ctx,
		r.logger,
		adapter.StartStateStart,
		r.outbound,
		r.dnsTransport,
		r.network,
		r.connection,
	); err != nil {
		return err
	}
	if r.httpClient != nil {
		if err := adapter.StartNamed(
			r.ctx,
			r.logger,
			adapter.StartStateStart,
			[]adapter.LifecycleService{r.httpClient},
		); err != nil {
			return err
		}
	}
	if err := adapter.Start(
		r.ctx,
		r.logger,
		adapter.StartStateStart,
		r.router,
		r.dnsRouter,
		r.endpoint,
		r.certificateProvider,
	); err != nil {
		return err
	}
	if err := adapter.Start(
		r.ctx,
		r.logger,
		adapter.StartStatePostStart,
		r.outbound,
		r.network,
		r.dnsTransport,
		r.dnsRouter,
		r.connection,
		r.router,
		r.endpoint,
		r.certificateProvider,
	); err != nil {
		return err
	}
	if err := adapter.Start(
		r.ctx,
		r.logger,
		adapter.StartStateStarted,
		r.network,
		r.dnsTransport,
		r.dnsRouter,
		r.connection,
		r.router,
		r.outbound,
		r.endpoint,
		r.certificateProvider,
	); err != nil {
		return err
	}
	r.started = true
	return nil
}

type trafficStatisticsMigrationRuntimeCloseItem struct {
	name      string
	lifecycle adapter.Lifecycle
}

func (r *trafficStatisticsMigrationRuntime) closeLifecycleItems() []trafficStatisticsMigrationRuntimeCloseItem {
	items := make([]trafficStatisticsMigrationRuntimeCloseItem, 0, 11)
	appendItem := func(name string, lifecycle adapter.Lifecycle) {
		if lifecycle != nil {
			items = append(items, trafficStatisticsMigrationRuntimeCloseItem{
				name:      name,
				lifecycle: lifecycle,
			})
		}
	}
	if r.certificateProvider != nil {
		appendItem("certificate provider", r.certificateProvider)
	}
	if r.endpoint != nil {
		appendItem("endpoint", r.endpoint)
	}
	if r.httpClient != nil {
		appendItem("HTTP client", r.httpClient)
	}
	if r.outbound != nil {
		appendItem("outbound", r.outbound)
	}
	if r.router != nil {
		appendItem("router", r.router)
	}
	if r.connection != nil {
		appendItem("connection", r.connection)
	}
	if r.dnsRouter != nil {
		appendItem("DNS router", r.dnsRouter)
	}
	if r.dnsTransport != nil {
		appendItem("DNS transport", r.dnsTransport)
	}
	if r.network != nil {
		appendItem("network", r.network)
	}
	if r.networkNamespace != nil {
		appendItem("network namespace manager", r.networkNamespace)
	}
	if r.certificateStore != nil {
		appendItem("certificate store", r.certificateStore)
	}
	return items
}

func (r *trafficStatisticsMigrationRuntime) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		closeLifecycle := func(name string, lifecycle adapter.Lifecycle) {
			if lifecycle == nil {
				return
			}
			r.closeErr = E.Append(r.closeErr, lifecycle.Close(), func(err error) error {
				return E.Cause(err, "close traffic migration ", name)
			})
		}
		for _, item := range r.closeLifecycleItems() {
			closeLifecycle(item.name, item.lifecycle)
		}
		if r.logFactory != nil {
			r.closeErr = E.Append(
				r.closeErr,
				r.logFactory.Close(),
				func(err error) error {
					return E.Cause(err, "close traffic migration logger")
				},
			)
		}
	})
	return r.closeErr
}
