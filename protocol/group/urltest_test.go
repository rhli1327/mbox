package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestURLTestSelectedOutboundConcurrentAccess(t *testing.T) {
	first := newURLTestRaceOutbound("first")
	second := newURLTestRaceOutbound("second")
	history := urltest.NewHistoryStorage()
	group := &URLTestGroup{
		outbounds:      []adapter.Outbound{first, second},
		history:        history,
		interruptGroup: interrupt.NewGroup(),
	}
	urlTestOutbound := &URLTest{
		tags:  []string{first.Tag(), second.Tag()},
		group: group,
	}

	history.StoreURLTestHistory(first.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
	history.StoreURLTestHistory(second.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
	group.performUpdateCheck()

	const iterations = 1000
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				history.StoreURLTestHistory(first.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
				history.StoreURLTestHistory(second.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
			} else {
				history.StoreURLTestHistory(first.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
				history.StoreURLTestHistory(second.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
			}
			group.performUpdateCheck()
		}
	}()
	for range 2 {
		go func() {
			defer waitGroup.Done()
			<-start
			for range iterations {
				if urlTestOutbound.Now() == "" {
					t.Error("Now returned an empty selection")
					return
				}
				if urlTestOutbound.NowForNetwork(N.NetworkTCP) == "" {
					t.Error("NowForNetwork(tcp) returned an empty selection")
					return
				}
				if urlTestOutbound.NowForNetwork(N.NetworkUDP) == "" {
					t.Error("NowForNetwork(udp) returned an empty selection")
					return
				}
				if selected, _ := group.Select(N.NetworkTCP); selected == nil {
					t.Error("Select(tcp) returned nil")
					return
				}
				if selected, _ := group.Select(N.NetworkUDP); selected == nil {
					t.Error("Select(udp) returned nil")
					return
				}
			}
		}()
	}
	close(start)
	waitGroup.Wait()
}

type urlTestRaceOutbound struct {
	outbound.Adapter
}

func newURLTestRaceOutbound(tag string) *urlTestRaceOutbound {
	return &urlTestRaceOutbound{
		Adapter: outbound.NewAdapter(
			C.TypeDirect,
			tag,
			[]string{N.NetworkTCP, N.NetworkUDP},
			nil,
		),
	}
}

func (o *urlTestRaceOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (o *urlTestRaceOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}
