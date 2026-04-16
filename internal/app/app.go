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
	server    *http.Server
}

func New(cfg config.Config, logger *log.Logger) (*App, error) {
	trafficStore, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}

	trafficCollector := collector.New(cfg, trafficStore, logger)
	apiServer := api.NewServer(trafficStore, trafficCollector, logger, embedfs.StaticFS())
	return &App{
		cfg:       cfg,
		logger:    logger,
		store:     trafficStore,
		collector: trafficCollector,
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
	defer aggregationTicker.Stop()
	defer cleanupTicker.Stop()

	a.runAggregation(ctx)
	a.runCleanup(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-aggregationTicker.C:
			a.runAggregation(ctx)
		case <-cleanupTicker.C:
			a.runCleanup(ctx)
		}
	}
}

func (a *App) runAggregation(ctx context.Context) {
	before := time.Now().UTC().Truncate(time.Hour)
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
