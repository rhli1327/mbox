//go:build with_clash_api

package box_test

import (
	"os"
	"path/filepath"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/experimental/clashmode"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"
)

func TestTrafficStatisticsWithClashMode(t *testing.T) {
	basePath := t.TempDir()
	ctx := newBoxTestContext(basePath)
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: option.Options{
			Experimental: &option.ExperimentalOptions{
				ClashAPI: &option.ClashAPIOptions{
					DefaultMode: "Global",
				},
				TrafficStatistics: &option.TrafficStatisticsOptions{
					Enabled: true,
				},
			},
		},
	})
	if err != nil {
		t.Fatal("create Box with traffic statistics and Clash API:", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Error("close Box:", err)
		}
	})
	if err := instance.Start(); err != nil {
		t.Fatal("start Box:", err)
	}
	mode := service.PtrFromContext[clashmode.Manager](ctx)
	if mode == nil || mode.Mode() != "Global" {
		t.Fatal("Clash mode manager did not retain the configured default mode")
	}
	if service.FromContext[trafficcontrol.HistoryReader](ctx) == nil {
		t.Fatal("traffic history is unavailable to the API")
	}
	if _, err := os.Stat(filepath.Join(basePath, "traffic.db")); err != nil {
		t.Fatal("traffic database was not initialized:", err)
	}
}
