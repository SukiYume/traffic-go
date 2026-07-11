package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"traffic-go/internal/app"
	"traffic-go/internal/buildinfo"
	"traffic-go/internal/config"
	embedassets "traffic-go/internal/embed"
)

func main() {
	if err := run(); err != nil {
		log.Printf("traffic-go exit with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var configPath string
	var maintenanceOnce bool
	var vacuum bool
	var showVersion bool
	var checkConfig bool
	var printDBPath bool
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.BoolVar(&maintenanceOnce, "maintenance-once", false, "run aggregation, cleanup, optimize, WAL checkpoint, and exit")
	flag.BoolVar(&vacuum, "vacuum", false, "allow VACUUM during maintenance-once")
	flag.BoolVar(&showVersion, "version", false, "print version and build provenance, then exit")
	flag.BoolVar(&checkConfig, "check-config", false, "validate configuration, print the effective listen address, then exit")
	flag.BoolVar(&printDBPath, "print-db-path", false, "validate configuration, print the configured database path, then exit")
	flag.Parse()
	if showVersion {
		info := buildinfo.Current()
		fmt.Printf("traffic-go version=%s commit=%s built=%s go=%s modified=%t\n", info.Version, info.Commit, info.BuildDate, info.GoVersion, info.Modified)
		return nil
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if checkConfig {
		fmt.Println(cfg.Listen)
		return nil
	}
	if printDBPath {
		fmt.Println(cfg.DBPath)
		return nil
	}
	if !maintenanceOnce {
		if err := embedassets.Validate(); err != nil {
			return err
		}
	}

	logger := log.New(os.Stdout, "traffic-go ", log.LstdFlags|log.Lmicroseconds)
	application, err := app.New(cfg, logger)
	if err != nil {
		return err
	}
	defer application.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if maintenanceOnce {
		return application.RunMaintenanceOnce(ctx, vacuum)
	}

	if err := application.Run(ctx); err != nil {
		return err
	}
	return nil
}
