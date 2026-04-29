package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
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

	prefetchMu      sync.Mutex
	prefetchRunning bool
	prefetchWG      sync.WaitGroup
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
	enableSSJournalFallback := true
	if cfg.ShadowsocksJournalFallback != nil {
		enableSSJournalFallback = *cfg.ShadowsocksJournalFallback
	}
	apiServer := api.NewServer(
		trafficStore,
		trafficCollector,
		logger,
		embedfs.StaticFS(),
		cfg.ProcessLogDirs,
		api.BasicAuthConfig{
			Username: cfg.Auth.Username,
			Password: cfg.Auth.Password,
		},
		enableSSJournalFallback,
	)
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
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 3)
	var wg sync.WaitGroup

	startTask := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- fn()
		}()
	}

	startTask(a.serveHTTP)
	startTask(func() error {
		return a.collector.Start(runCtx)
	})
	startTask(func() error {
		return a.runMaintenance(runCtx)
	})

	var runErr error

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}

	cancel()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.server.Shutdown(shutdownCtx)
	wg.Wait()
	a.prefetchWG.Wait()
	return runErr
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
	a.runCleanup(ctx, false)
	a.runPrefetchAsync(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-aggregationTicker.C:
			a.runAggregation(ctx)
		case <-cleanupTicker.C:
			a.runCleanup(ctx, true)
		case <-prefetchTick(prefetchTicker):
			a.runPrefetchAsync(ctx)
		}
	}
}

func (a *App) runAggregation(ctx context.Context) {
	before := time.Now().UTC().Truncate(time.Hour)
	latestCompleteHour := before.Add(-time.Hour)
	if err := a.store.AggregateHour(ctx, latestCompleteHour); err != nil {
		a.logger.Printf("refresh latest completed hour %s failed: %v", latestCompleteHour.Format(time.RFC3339), err)
	}

	for {
		dirtyHour, exists, err := a.store.NextDirtyChainHour(ctx, before)
		if err != nil {
			a.logger.Printf("scan dirty chain hours failed: %v", err)
			return
		}
		if !exists {
			break
		}
		if err := a.store.AggregateHour(ctx, dirtyHour); err != nil {
			a.logger.Printf("aggregate dirty chain hour %s failed: %v", dirtyHour.Format(time.RFC3339), err)
			return
		}
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

func (a *App) runCleanup(ctx context.Context, allowVacuum bool) {
	if err := a.store.Cleanup(ctx); err != nil {
		a.logger.Printf("cleanup failed: %v", err)
		return
	}
	if err := a.store.Optimize(ctx); err != nil {
		a.logger.Printf("optimize failed: %v", err)
	}
	if !allowVacuum {
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

func (a *App) runPrefetchAsync(ctx context.Context) {
	if a.apiServer == nil || !a.cfg.Prefetch.Enabled || len(a.cfg.ProcessLogDirs) == 0 {
		return
	}
	a.prefetchMu.Lock()
	if a.prefetchRunning {
		a.prefetchMu.Unlock()
		a.logger.Printf("prefetch skipped: previous run still active")
		return
	}
	a.prefetchRunning = true
	a.prefetchWG.Add(1)
	a.prefetchMu.Unlock()

	go func() {
		defer a.prefetchWG.Done()
		defer func() {
			a.prefetchMu.Lock()
			a.prefetchRunning = false
			a.prefetchMu.Unlock()
		}()
		a.runPrefetch(ctx)
	}()
}

func prefetchTick(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
