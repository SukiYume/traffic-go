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

func (n *noopCollector) Diagnostics() model.CollectorDiagnostics {
	return model.CollectorDiagnostics{
		AttributionCounts: make(map[string]int64),
	}
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
		if err := m.store.FlushInterfaceMinute(ctx, now.Unix(), map[string]model.InterfaceUsageDelta{
			"eth0": {
				RxBytes: 640 * 1024,
				TxBytes: 192 * 1024,
			},
		}); err != nil {
			m.logger.Printf("mock interface flush failed: %v", err)
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

func (m *mockCollector) Diagnostics() model.CollectorDiagnostics {
	return model.CollectorDiagnostics{
		ActiveConnections: 1,
		ActiveProcesses:   1,
		AttributionCounts: map[string]int64{
			string(model.AttributionExact): 1,
		},
		SnapshotReady: true,
	}
}
