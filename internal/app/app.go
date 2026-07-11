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

	prefetchMu              sync.Mutex
	prefetchRunning         bool
	prefetchWG              sync.WaitGroup
	lastRefreshedLatestHour time.Time
	lastLatestRefreshMinute time.Time
	lastFinalizedLatestHour time.Time
}

const latestCompletedHourRefreshGrace = 5 * time.Minute
const maxHTTPHeaderBytes = 64 << 10

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
		cfg.ShadowsocksJournalFallback,
		cfg.Cache,
	)
	return &App{
		cfg:       cfg,
		logger:    logger,
		store:     trafficStore,
		collector: trafficCollector,
		apiServer: apiServer,
		server: &http.Server{
			Addr:           cfg.Listen,
			Handler:        apiServer.Handler(),
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   60 * time.Second,
			IdleTimeout:    120 * time.Second,
			MaxHeaderBytes: maxHTTPHeaderBytes,
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

func (a *App) RunMaintenanceOnce(ctx context.Context, allowVacuum bool) error {
	a.runAggregation(ctx)
	return a.cleanup(ctx, allowVacuum, allowVacuum)
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
			// A full SQLite VACUUM needs roughly one database-sized block of free
			// disk and an exclusive lock. Running it inside the live service can
			// stall collection for minutes and repeatedly fail on space-constrained
			// hosts. Keep periodic cleanup online-safe; operators can still request
			// an explicit offline-style vacuum with -maintenance-once -vacuum.
			a.runCleanup(ctx, false)
		case <-prefetchTick(prefetchTicker):
			a.runPrefetchAsync(ctx)
		}
	}
}

func (a *App) runAggregation(ctx context.Context) {
	a.runAggregationAt(ctx, time.Now().UTC())
}

func (a *App) runAggregationAt(ctx context.Context, now time.Time) {
	now = now.UTC()
	before := now.Truncate(time.Hour)
	latestCompleteHour := before.Add(-time.Hour)
	withinGrace := now.Sub(before) <= latestCompletedHourRefreshGrace
	catchupBefore := before
	if withinGrace {
		// Do not finalize the latest hour/day while the collector may still be
		// flushing its final minute. Grace refreshes below update only hour tables.
		catchupBefore = latestCompleteHour
	}
	aggregated := make(map[int64]struct{})

	dirtyHours, err := a.store.DirtyChainHours(ctx, catchupBefore)
	if err != nil {
		a.logger.Printf("scan dirty chain hours failed: %v", err)
		return
	}
	if len(dirtyHours) > 0 {
		if err := a.store.AggregateHours(ctx, dirtyHours); err != nil {
			a.logger.Printf("aggregate %d dirty chain hours failed: %v", len(dirtyHours), err)
			return
		}
		for _, dirtyHour := range dirtyHours {
			aggregated[dirtyHour.Unix()] = struct{}{}
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

	pending := make([]time.Time, 0)
	scanCursor := cursor
	for {
		nextHour, exists, err := a.store.NextPendingAggregationHour(ctx, scanCursor, catchupBefore)
		if err != nil {
			a.logger.Printf("scan pending aggregation hours failed: %v", err)
			return
		}
		if !exists {
			break
		}
		if _, alreadyAggregated := aggregated[nextHour.Unix()]; !alreadyAggregated {
			pending = append(pending, nextHour)
		}
		nextCopy := nextHour
		scanCursor = &nextCopy
	}
	if len(pending) > 0 {
		if err := a.store.AggregateHours(ctx, pending); err != nil {
			a.logger.Printf("aggregate %d pending hours failed: %v", len(pending), err)
			return
		}
		for _, hour := range pending {
			aggregated[hour.Unix()] = struct{}{}
		}
	}
	if scanCursor != nil && (cursor == nil || scanCursor.After(*cursor)) {
		if err := a.store.SetLastAggregatedHour(ctx, *scanCursor); err != nil {
			a.logger.Printf("persist aggregation cursor %s failed: %v", scanCursor.Format(time.RFC3339), err)
			return
		}
	}

	if withinGrace {
		refreshMinute := now.Truncate(time.Minute)
		if a.lastRefreshedLatestHour.Equal(latestCompleteHour) && a.lastLatestRefreshMinute.Equal(refreshMinute) {
			return
		}
		if err := a.store.AggregateHourOnly(ctx, latestCompleteHour); err != nil {
			a.logger.Printf("refresh latest completed hour %s failed: %v", latestCompleteHour.Format(time.RFC3339), err)
			return
		}
		a.lastRefreshedLatestHour = latestCompleteHour
		a.lastLatestRefreshMinute = refreshMinute
		return
	}

	if _, refreshed := aggregated[latestCompleteHour.Unix()]; refreshed {
		a.lastRefreshedLatestHour = latestCompleteHour
		a.lastFinalizedLatestHour = latestCompleteHour
		return
	}
	if a.lastFinalizedLatestHour.Equal(latestCompleteHour) {
		return
	}
	if err := a.store.AggregateHour(ctx, latestCompleteHour); err != nil {
		a.logger.Printf("finalize latest completed hour %s failed: %v", latestCompleteHour.Format(time.RFC3339), err)
		return
	}
	a.lastRefreshedLatestHour = latestCompleteHour
	a.lastFinalizedLatestHour = latestCompleteHour
}

func (a *App) runCleanup(ctx context.Context, allowVacuum bool) {
	if err := a.cleanup(ctx, allowVacuum, false); err != nil {
		a.logger.Printf("cleanup failed: %v", err)
	}
}

func (a *App) cleanup(ctx context.Context, allowVacuum bool, forceVacuum bool) error {
	if err := a.store.Cleanup(ctx); err != nil {
		return err
	}
	if err := a.store.Optimize(ctx); err != nil {
		a.logger.Printf("optimize failed: %v", err)
	}
	if err := a.store.CheckpointWAL(ctx); err != nil {
		a.logger.Printf("wal checkpoint failed: %v", err)
	}
	if !allowVacuum {
		return nil
	}

	now := time.Now().UTC()
	if !forceVacuum {
		lastVacuum, ok, err := a.store.LastVacuum(ctx)
		if err != nil {
			return err
		}
		if !ok {
			// Existing installations may open a large database with no historical
			// vacuum marker. Establish a baseline first instead of unexpectedly
			// blocking startup-hour traffic with an immediate full VACUUM.
			return a.store.SetLastVacuum(ctx, now)
		}
		if ok && now.Sub(lastVacuum) < 7*24*time.Hour {
			return nil
		}
	}
	if err := a.store.Vacuum(ctx); err != nil {
		return err
	}
	if err := a.store.CheckpointWALTruncate(ctx); err != nil {
		a.logger.Printf("wal checkpoint after vacuum failed: %v", err)
	}
	if err := a.store.SetLastVacuum(ctx, now); err != nil {
		return err
	}
	return nil
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
