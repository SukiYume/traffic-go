package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"traffic-go/internal/app"
	"traffic-go/internal/config"
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
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.BoolVar(&maintenanceOnce, "maintenance-once", false, "run aggregation, cleanup, optimize, WAL checkpoint, and exit")
	flag.BoolVar(&vacuum, "vacuum", false, "allow VACUUM during maintenance-once")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
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
