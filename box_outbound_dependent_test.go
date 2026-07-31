package box

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	boxCertificate "github.com/sagernet/sing-box/adapter/certificate"
	boxEndpoint "github.com/sagernet/sing-box/adapter/endpoint"
	boxInbound "github.com/sagernet/sing-box/adapter/inbound"
	boxOutbound "github.com/sagernet/sing-box/adapter/outbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/dns"
	dnsLocal "github.com/sagernet/sing-box/dns/transport/local"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestAppendTrafficHistoryService(t *testing.T) {
	history := trafficcontrol.NewHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("traffic-test"),
		trafficcontrol.HistoryOptions{Path: t.TempDir() + "/traffic.db"},
	)
	for _, test := range []struct {
		name      string
		traffic   option.ResolvedTrafficStatisticsOptions
		dependent bool
	}{
		{
			name: "bolt",
			traffic: option.ResolvedTrafficStatisticsOptions{
				StorageType: option.TrafficStatisticsStorageTypeBolt,
			},
		},
		{
			name: "direct postgres",
			traffic: option.ResolvedTrafficStatisticsOptions{
				StorageType: option.TrafficStatisticsStorageTypePostgres,
			},
		},
		{
			name: "detoured postgres",
			traffic: option.ResolvedTrafficStatisticsOptions{
				StorageType: option.TrafficStatisticsStorageTypePostgres,
				Dialer:      option.DialerOptions{Detour: "proxy"},
			},
			dependent: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			internal, dependent := appendTrafficHistoryService(
				nil,
				nil,
				history,
				test.traffic,
			)
			if test.dependent {
				if len(internal) != 0 || len(dependent) != 1 || dependent[0] != history {
					t.Fatalf("unexpected service buckets: internal=%#v dependent=%#v", internal, dependent)
				}
			} else if len(internal) != 1 || internal[0] != history || len(dependent) != 0 {
				t.Fatalf("unexpected service buckets: internal=%#v dependent=%#v", internal, dependent)
			}
		})
	}
}

func TestBoxOutboundDependentLifecycleOrder(t *testing.T) {
	recorder := new(boxLifecycleRecorder)
	instance := newBoxLifecycleTestInstance(t, recorder)
	instance.internalService = append(
		instance.internalService,
		&boxLifecycleProbe{name: "ordinary", recorder: recorder},
	)
	instance.outboundDependentService = append(
		instance.outboundDependentService,
		&boxLifecycleProbe{name: "dependent", recorder: recorder},
	)
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}

	events := recorder.snapshot()
	assertBoxLifecycleBefore(t, events, "ordinary.initialize", "dependent.initialize")
	assertBoxLifecycleBefore(t, events, "dependent.initialize", "outbound.initialize")
	assertBoxLifecycleBefore(t, events, "outbound.start", "ordinary.start")
	assertBoxLifecycleBefore(t, events, "ordinary.start", "dependent.start")
	assertBoxLifecycleBefore(t, events, "ordinary.post-start", "dependent.post-start")
	assertBoxLifecycleBefore(t, events, "ordinary.finish-start", "dependent.finish-start")
	assertBoxLifecycleBefore(t, events, "ordinary.close", "dependent.close")
	assertBoxLifecycleBefore(t, events, "dependent.close", "outbound.close")
}

func TestBoxOutboundDependentStartFailureClosesBeforeOutbound(t *testing.T) {
	recorder := new(boxLifecycleRecorder)
	instance := newBoxLifecycleTestInstance(t, recorder)
	instance.internalService = append(
		instance.internalService,
		&boxLifecycleProbe{name: "ordinary", recorder: recorder},
	)
	instance.outboundDependentService = append(
		instance.outboundDependentService,
		&boxLifecycleProbe{
			name:      "dependent",
			recorder:  recorder,
			fail:      true,
			failStage: adapter.StartStateStart,
		},
	)
	if err := instance.Start(); err == nil {
		t.Fatal("outbound-dependent start failure unexpectedly succeeded")
	}

	events := recorder.snapshot()
	assertBoxLifecycleBefore(t, events, "outbound.start", "ordinary.start")
	assertBoxLifecycleBefore(t, events, "ordinary.start", "dependent.start")
	assertBoxLifecycleBefore(t, events, "ordinary.close", "dependent.close")
	assertBoxLifecycleBefore(t, events, "dependent.close", "outbound.close")
}

func newBoxLifecycleTestInstance(
	t *testing.T,
	recorder *boxLifecycleRecorder,
) *Box {
	t.Helper()
	outboundRegistry := boxOutbound.NewRegistry()
	boxOutbound.Register[option.StubOptions](
		outboundRegistry,
		"lifecycle-probe",
		func(
			_ context.Context,
			_ adapter.Router,
			_ log.ContextLogger,
			tag string,
			_ option.StubOptions,
		) (adapter.Outbound, error) {
			return &boxLifecycleOutbound{
				Adapter:  boxOutbound.NewAdapter("lifecycle-probe", tag, []string{N.NetworkTCP}, nil),
				recorder: recorder,
			}, nil
		},
	)
	dnsRegistry := dns.NewTransportRegistry()
	dnsLocal.RegisterTransport(dnsRegistry)
	ctx := Context(
		context.Background(),
		boxInbound.NewRegistry(),
		outboundRegistry,
		boxEndpoint.NewRegistry(),
		dnsRegistry,
		boxService.NewRegistry(),
		boxCertificate.NewRegistry(),
	)
	instance, err := New(Options{
		Context: ctx,
		Options: option.Options{
			Log: &option.LogOptions{Disabled: true},
			Outbounds: []option.Outbound{
				{
					Type:    "lifecycle-probe",
					Tag:     "probe",
					Options: &option.StubOptions{},
				},
			},
			Route: &option.RouteOptions{Final: "probe"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = instance.Close()
	})
	return instance
}

type boxLifecycleRecorder struct {
	access sync.Mutex
	events []string
}

func (r *boxLifecycleRecorder) append(event string) {
	r.access.Lock()
	r.events = append(r.events, event)
	r.access.Unlock()
}

func (r *boxLifecycleRecorder) snapshot() []string {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]string(nil), r.events...)
}

type boxLifecycleProbe struct {
	name      string
	recorder  *boxLifecycleRecorder
	fail      bool
	failStage adapter.StartStage
}

func (s *boxLifecycleProbe) Name() string {
	return s.name
}

func (s *boxLifecycleProbe) Start(stage adapter.StartStage) error {
	s.recorder.append(s.name + "." + stage.String())
	if s.fail && stage == s.failStage {
		return errors.New("injected outbound-dependent lifecycle failure")
	}
	return nil
}

func (s *boxLifecycleProbe) Close() error {
	s.recorder.append(s.name + ".close")
	return nil
}

type boxLifecycleOutbound struct {
	boxOutbound.Adapter
	recorder *boxLifecycleRecorder
}

func (o *boxLifecycleOutbound) Start(stage adapter.StartStage) error {
	o.recorder.append("outbound." + stage.String())
	return nil
}

func (o *boxLifecycleOutbound) Close() error {
	o.recorder.append("outbound.close")
	return nil
}

func (o *boxLifecycleOutbound) DialContext(
	context.Context,
	string,
	M.Socksaddr,
) (net.Conn, error) {
	return nil, errors.New("lifecycle probe cannot dial")
}

func (o *boxLifecycleOutbound) ListenPacket(
	context.Context,
	M.Socksaddr,
) (net.PacketConn, error) {
	return nil, errors.New("lifecycle probe cannot listen")
}

func assertBoxLifecycleBefore(
	t *testing.T,
	events []string,
	before string,
	after string,
) {
	t.Helper()
	beforeIndex := boxLifecycleEventIndex(events, before)
	afterIndex := boxLifecycleEventIndex(events, after)
	if beforeIndex == -1 || afterIndex == -1 || beforeIndex >= afterIndex {
		t.Fatalf(
			"expected %q before %q, got %#v",
			before,
			after,
			events,
		)
	}
}

func boxLifecycleEventIndex(events []string, event string) int {
	for index, currentEvent := range events {
		if currentEvent == event {
			return index
		}
	}
	return -1
}

var _ adapter.Outbound = (*boxLifecycleOutbound)(nil)
var _ adapter.LifecycleService = (*boxLifecycleProbe)(nil)
