package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"traffic-go/internal/api"
	"traffic-go/internal/collector"
	"traffic-go/internal/config"
	embedfs "traffic-go/internal/embed"
	"traffic-go/internal/store"
)

type App struct {
	cfg       config.Config
	logger    *log.Logger
	store     *store.Store
	collector collector.Runner
	apiServer *api.Server
	server    *http.Server
}

func New(cfg config.Config, logger *log.Logger) (*App, error) {
	cfg = config.Derive(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	trafficStore, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}

	trafficCollector := collector.New(cfg, trafficStore, logger)
	apiServer := api.NewServer(trafficStore, trafficCollector, logger, embedfs.StaticFS(), cfg.ProcessLogDirs)
	return &App{
		cfg:       cfg,
		logger:    logger,
		store:     trafficStore,
		collector: trafficCollector,
		apiServer: apiServer,
		server: &http.Server{
			Addr:    cfg.Listen,
			Handler: apiServer.Handler(),
		},
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 3)

	go func() {
		errCh <- a.serveHTTP()
	}()
	go func() {
		errCh <- a.collector.Start(ctx)
	}()
	go func() {
		errCh <- a.runMaintenance(ctx)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.server.Shutdown(shutdownCtx)
	return nil
}

func (a *App) Close() error {
	return a.store.Close()
}

func (a *App) serveHTTP() error {
	a.logger.Printf("HTTP listening on %s", a.cfg.Listen)
	err := a.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (a *App) runMaintenance(ctx context.Context) error {
	aggregationTicker := time.NewTicker(time.Minute)
	cleanupTicker := time.NewTicker(time.Hour)
	var prefetchTicker *time.Ticker
	if a.cfg.Prefetch.Enabled && len(a.cfg.ProcessLogDirs) > 0 {
		prefetchTicker = time.NewTicker(a.cfg.Prefetch.Interval)
	}
	defer aggregationTicker.Stop()
	defer cleanupTicker.Stop()
	if prefetchTicker != nil {
		defer prefetchTicker.Stop()
	}

	a.runAggregation(ctx)
	a.runCleanup(ctx)
	a.runPrefetch(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-aggregationTicker.C:
			a.runAggregation(ctx)
		case <-cleanupTicker.C:
			a.runCleanup(ctx)
		case <-prefetchTick(prefetchTicker):
			a.runPrefetch(ctx)
		}
	}
}

func (a *App) runAggregation(ctx context.Context) {
	before := time.Now().UTC().Truncate(time.Hour)
	latestCompleteHour := before.Add(-time.Hour)
	if err := a.store.AggregateHour(ctx, latestCompleteHour); err != nil {
		a.logger.Printf("refresh latest completed hour %s failed: %v", latestCompleteHour.Format(time.RFC3339), err)
	}

	lastHour, ok, err := a.store.LastAggregatedHour(ctx)
	if err != nil {
		a.logger.Printf("read aggregation cursor failed: %v", err)
	}
	var cursor *time.Time
	if ok {
		cursor = &lastHour
	}

	for {
		nextHour, exists, err := a.store.NextPendingAggregationHour(ctx, cursor, before)
		if err != nil {
			a.logger.Printf("scan pending aggregation hours failed: %v", err)
			return
		}
		if !exists {
			return
		}
		if err := a.store.AggregateHour(ctx, nextHour); err != nil {
			a.logger.Printf("aggregate hour %s failed: %v", nextHour.Format(time.RFC3339), err)
			return
		}
		if err := a.store.SetLastAggregatedHour(ctx, nextHour); err != nil {
			a.logger.Printf("persist aggregation cursor %s failed: %v", nextHour.Format(time.RFC3339), err)
			return
		}
		cursor = &nextHour
	}
}

func (a *App) runCleanup(ctx context.Context) {
	if err := a.store.Cleanup(ctx); err != nil {
		a.logger.Printf("cleanup failed: %v", err)
		return
	}

	lastVacuum, ok, err := a.store.LastVacuum(ctx)
	if err != nil {
		a.logger.Printf("read vacuum cursor failed: %v", err)
		return
	}
	now := time.Now().UTC()
	if ok && now.Sub(lastVacuum) < 7*24*time.Hour {
		return
	}
	if err := a.store.Vacuum(ctx); err != nil {
		a.logger.Printf("vacuum failed: %v", err)
		return
	}
	if err := a.store.SetLastVacuum(ctx, now); err != nil {
		a.logger.Printf("persist vacuum cursor failed: %v", err)
	}
}

func (a *App) runPrefetch(ctx context.Context) {
	if a.apiServer == nil || !a.cfg.Prefetch.Enabled || len(a.cfg.ProcessLogDirs) == 0 {
		return
	}
	summary := a.apiServer.RunBackgroundPrefetch(ctx, api.BackgroundPrefetchOptions{
		Enabled:             a.cfg.Prefetch.Enabled,
		Now:                 time.Now().UTC(),
		EvidenceLookback:    a.cfg.Prefetch.EvidenceLookback,
		ChainLookback:       a.cfg.Prefetch.ChainLookback,
		ScanBudget:          a.cfg.Prefetch.ScanBudget,
		MaxScanFiles:        a.cfg.Prefetch.MaxScanFiles,
		MaxScanLinesPerFile: a.cfg.Prefetch.MaxScanLinesPerFile,
	})
	if summary.Sources == 0 && summary.UsageRows == 0 && summary.Errors == 0 {
		return
	}
	a.logger.Printf(
		"prefetch sources=%d evidence_rows=%d usage_rows=%d chain_rows=%d partial_sources=%d errors=%d",
		summary.Sources,
		summary.EvidenceRows,
		summary.UsageRows,
		summary.ChainRows,
		summary.PartialSources,
		summary.Errors,
	)
}

func prefetchTick(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
