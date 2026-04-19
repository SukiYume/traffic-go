package collector

import (
	"context"
	"log"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type mockCollector struct {
	cfg    config.Config
	store  *store.Store
	logger *log.Logger
}

type noopCollector struct{}

func (n *noopCollector) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (n *noopCollector) ActiveProcesses() []model.ProcessListItem {
	return nil
}

func (n *noopCollector) ActiveStats() model.ActiveStats {
	return model.ActiveStats{}
}

func (m *mockCollector) Start(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		now := time.Now().UTC().Truncate(time.Minute)
		usage := map[model.UsageKey]model.UsageDelta{
			{
				MinuteTS:    now.Unix(),
				Proto:       "tcp",
				Direction:   model.DirectionOut,
				PID:         4242,
				Comm:        "mock-proxy",
				Exe:         "/usr/local/bin/mock-proxy",
				LocalPort:   8388,
				RemoteIP:    "104.26.2.33",
				RemotePort:  443,
				Attribution: model.AttributionExact,
			}: {
				BytesUp:   128 * 1024,
				BytesDown: 512 * 1024,
				PktsUp:    120,
				PktsDown:  210,
				FlowCount: 1,
			},
		}
		if err := m.store.FlushMinute(ctx, now.Unix(), usage, nil); err != nil {
			m.logger.Printf("mock flush failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (m *mockCollector) ActiveProcesses() []model.ProcessListItem {
	return []model.ProcessListItem{
		{
			PID:  4242,
			Comm: "mock-proxy",
			Exe:  "/usr/local/bin/mock-proxy",
		},
	}
}

func (m *mockCollector) ActiveStats() model.ActiveStats {
	return model.ActiveStats{
		Connections: 1,
		Processes:   1,
	}
}
