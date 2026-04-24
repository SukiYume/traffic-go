package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	Months int `yaml:"months"`
	// Legacy day-based settings are still parsed so existing config files load,
	// but retention is now enforced by UTC calendar months.
	MinuteDays int `yaml:"flows_days"`
	HourlyDays int `yaml:"hourly_days"`
}

type Prefetch struct {
	Enabled             bool          `yaml:"enabled"`
	Interval            time.Duration `yaml:"interval"`
	EvidenceLookback    time.Duration `yaml:"evidence_lookback"`
	ChainLookback       time.Duration `yaml:"chain_lookback"`
	ScanBudget          time.Duration `yaml:"scan_budget"`
	MaxScanFiles        int           `yaml:"max_scan_files"`
	MaxScanLinesPerFile int           `yaml:"max_scan_lines_per_file"`
}

type Config struct {
	Listen        string        `yaml:"listen"`
	DBPath        string        `yaml:"db_path"`
	TickInterval  time.Duration `yaml:"tick_interval"`
	ProcFS        string        `yaml:"proc_fs"`
	ConntrackPath string        `yaml:"conntrack_path"`
	// Legacy fields kept for backward compatibility. Their values are merged
	// into ProcessLogDirs when explicit per-process entries are not provided.
	NginxLogDir    string            `yaml:"nginx_log_dir"`
	SSLogDir       string            `yaml:"ss_log_dir"`
	ProcessLogDirs map[string]string `yaml:"process_log_dirs"`
	// Keep systemd journal fallback enabled by default for hosts where
	// shadowsocks still logs only to journald. Set false once rsyslog or the
	// service itself writes usable files under process_log_dirs.
	ShadowsocksJournalFallback *bool     `yaml:"shadowsocks_journal_fallback"`
	MockData                   bool      `yaml:"mock_data"`
	LogLevel                   string    `yaml:"log_level"`
	Retention                  Retention `yaml:"retention"`
	Prefetch                   Prefetch  `yaml:"prefetch"`
}

func Default() Config {
	enableShadowsocksJournalFallback := true
	return Config{
		Listen:                     defaultListen,
		DBPath:                     defaultDBPath,
		TickInterval:               defaultTickInterval,
		ProcFS:                     defaultProcFS,
		ShadowsocksJournalFallback: &enableShadowsocksJournalFallback,
		LogLevel:                   "info",
		Retention: Retention{
			Months: 3,
		},
		Prefetch: Prefetch{
			Enabled:             true,
			Interval:            time.Minute,
			EvidenceLookback:    20 * time.Minute,
			ChainLookback:       20 * time.Minute,
			ScanBudget:          8 * time.Second,
			MaxScanFiles:        6,
			MaxScanLinesPerFile: 250000,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return Derive(cfg), nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg = Derive(cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Derive applies runtime defaults and compatibility merges to a Config value.
// Use this when a Config is constructed in-memory (without Load) before wiring services.
func Derive(cfg Config) Config {
	return withDerivedDefaults(cfg)
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
	cfg.ProcessLogDirs = withConfiguredProcessLogDirs(cfg.ProcessLogDirs, cfg.NginxLogDir, cfg.SSLogDir)
	if cfg.ShadowsocksJournalFallback == nil {
		enableShadowsocksJournalFallback := true
		cfg.ShadowsocksJournalFallback = &enableShadowsocksJournalFallback
	}
	if cfg.Retention.Months <= 0 {
		cfg.Retention.Months = 3
	}
	if cfg.Prefetch.Interval <= 0 {
		cfg.Prefetch.Interval = time.Minute
	}
	if cfg.Prefetch.EvidenceLookback <= 0 {
		cfg.Prefetch.EvidenceLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ChainLookback <= 0 {
		cfg.Prefetch.ChainLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ScanBudget <= 0 {
		cfg.Prefetch.ScanBudget = 8 * time.Second
	}
	if cfg.Prefetch.MaxScanFiles <= 0 {
		cfg.Prefetch.MaxScanFiles = 6
	}
	if cfg.Prefetch.MaxScanLinesPerFile <= 0 {
		cfg.Prefetch.MaxScanLinesPerFile = 250000
	}
	return cfg
}

func normalizeProcessLogDirs(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		normalizedValue := strings.TrimSpace(value)
		if normalizedKey == "" || normalizedValue == "" {
			continue
		}
		result[normalizedKey] = normalizedValue
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func withConfiguredProcessLogDirs(values map[string]string, nginxLogDir string, ssLogDir string) map[string]string {
	result := normalizeProcessLogDirs(values)

	if normalizedNginxLogDir := strings.TrimSpace(nginxLogDir); normalizedNginxLogDir != "" {
		if result == nil {
			result = make(map[string]string, 2)
		}
		if _, ok := result["nginx"]; !ok {
			result["nginx"] = normalizedNginxLogDir
		}
	}
	if normalizedSSLogDir := strings.TrimSpace(ssLogDir); normalizedSSLogDir != "" {
		if result == nil {
			result = make(map[string]string, 2)
		}
		if _, ok := result["ss-server"]; !ok {
			result["ss-server"] = normalizedSSLogDir
		}
	}

	return normalizeProcessLogDirs(result)
}

func (c Config) Validate() error {
	switch {
	case c.Listen == "":
		return errors.New("listen must not be empty")
	case c.DBPath == "":
		return errors.New("db_path must not be empty")
	case c.TickInterval <= 0:
		return errors.New("tick_interval must be positive")
	case c.Prefetch.Interval <= 0:
		return errors.New("prefetch.interval must be positive")
	case c.Prefetch.EvidenceLookback <= 0:
		return errors.New("prefetch.evidence_lookback must be positive")
	case c.Prefetch.ChainLookback <= 0:
		return errors.New("prefetch.chain_lookback must be positive")
	case c.Prefetch.ScanBudget <= 0:
		return errors.New("prefetch.scan_budget must be positive")
	case c.Prefetch.MaxScanFiles <= 0:
		return errors.New("prefetch.max_scan_files must be positive")
	case c.Prefetch.MaxScanLinesPerFile <= 0:
		return errors.New("prefetch.max_scan_lines_per_file must be positive")
	default:
		return nil
	}
}
