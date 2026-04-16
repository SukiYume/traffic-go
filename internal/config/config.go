package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen       = "127.0.0.1:8080"
	defaultDBPath       = "traffic.db"
	defaultTickInterval = 2 * time.Second
	defaultProcFS       = "/proc"
)

type Retention struct {
	MinuteDays int `yaml:"flows_days"`
	HourlyDays int `yaml:"hourly_days"`
}

type Config struct {
	Listen        string        `yaml:"listen"`
	DBPath        string        `yaml:"db_path"`
	TickInterval  time.Duration `yaml:"tick_interval"`
	ProcFS        string        `yaml:"proc_fs"`
	ConntrackPath string        `yaml:"conntrack_path"`
	MockData      bool          `yaml:"mock_data"`
	LogLevel      string        `yaml:"log_level"`
	Retention     Retention     `yaml:"retention"`
}

func Default() Config {
	return Config{
		Listen:       defaultListen,
		DBPath:       defaultDBPath,
		TickInterval: defaultTickInterval,
		ProcFS:       defaultProcFS,
		LogLevel:     "info",
		Retention: Retention{
			MinuteDays: 30,
			HourlyDays: 180,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return withDerivedDefaults(cfg), nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg = withDerivedDefaults(cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func withDerivedDefaults(cfg Config) Config {
	if cfg.Listen == "" {
		cfg.Listen = defaultListen
	}
	if cfg.DBPath == "" {
		cfg.DBPath = defaultDBPath
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.ProcFS == "" {
		cfg.ProcFS = defaultProcFS
	}
	if cfg.ConntrackPath == "" {
		cfg.ConntrackPath = filepath.Join(cfg.ProcFS, "net", "nf_conntrack")
	}
	if cfg.Retention.MinuteDays <= 0 {
		cfg.Retention.MinuteDays = 30
	}
	if cfg.Retention.HourlyDays <= 0 {
		cfg.Retention.HourlyDays = 180
	}
	return cfg
}

func (c Config) Validate() error {
	switch {
	case c.Listen == "":
		return errors.New("listen must not be empty")
	case c.DBPath == "":
		return errors.New("db_path must not be empty")
	case c.TickInterval <= 0:
		return errors.New("tick_interval must be positive")
	default:
		return nil
	}
}
