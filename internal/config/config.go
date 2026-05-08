package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen              = "127.0.0.1:8080"
	defaultDBPath              = "traffic.db"
	defaultTickInterval        = 2 * time.Second
	defaultSocketIndexInterval = 10 * time.Second
	defaultProcFS              = "/proc"
	defaultCountCacheSize      = 256
	defaultResultCacheSize     = 16
	defaultSlidingTTL          = 60 * time.Second
	defaultArchivedTTL         = 3600 * time.Second
)

type Retention struct {
	MinuteDays   int `yaml:"minute_days"`
	HourMonths   int `yaml:"hour_months"`
	DayMonths    int `yaml:"day_months"`
	EvidenceDays int `yaml:"evidence_days"`
	ChainDays    int `yaml:"chain_days"`
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

type Cache struct {
	CountCacheSize  int           `yaml:"count_cache_size"`
	ResultCacheSize int           `yaml:"result_cache_size"`
	SlidingTTL      time.Duration `yaml:"sliding_ttl"`
	ArchivedTTL     time.Duration `yaml:"archived_ttl"`

	countCacheSizeSet  bool
	resultCacheSizeSet bool
	slidingTTLSet      bool
	archivedTTLSet     bool
}

func (c *Cache) UnmarshalYAML(value *yaml.Node) error {
	type rawCache struct {
		CountCacheSize  *int           `yaml:"count_cache_size"`
		ResultCacheSize *int           `yaml:"result_cache_size"`
		SlidingTTL      *time.Duration `yaml:"sliding_ttl"`
		ArchivedTTL     *time.Duration `yaml:"archived_ttl"`
	}
	var raw rawCache
	if err := value.Decode(&raw); err != nil {
		return err
	}
	if raw.CountCacheSize != nil {
		c.CountCacheSize = *raw.CountCacheSize
		c.countCacheSizeSet = true
	}
	if raw.ResultCacheSize != nil {
		c.ResultCacheSize = *raw.ResultCacheSize
		c.resultCacheSizeSet = true
	}
	if raw.SlidingTTL != nil {
		c.SlidingTTL = *raw.SlidingTTL
		c.slidingTTLSet = true
	}
	if raw.ArchivedTTL != nil {
		c.ArchivedTTL = *raw.ArchivedTTL
		c.archivedTTLSet = true
	}
	return nil
}

type Auth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Config struct {
	Listen              string            `yaml:"listen"`
	DBPath              string            `yaml:"db_path"`
	TickInterval        time.Duration     `yaml:"tick_interval"`
	SocketIndexInterval time.Duration     `yaml:"socket_index_interval"`
	ProcFS              string            `yaml:"proc_fs"`
	ConntrackPath       string            `yaml:"conntrack_path"`
	NetworkInterfaces   []string          `yaml:"network_interfaces"`
	ProcessLogDirs      map[string]string `yaml:"process_log_dirs"`
	// Keep systemd journal fallback disabled by default to avoid making
	// persistent journald retention a hidden requirement. Set true only on
	// hosts where shadowsocks still logs exclusively to journald.
	ShadowsocksJournalFallback *bool     `yaml:"shadowsocks_journal_fallback"`
	MockData                   bool      `yaml:"mock_data"`
	Auth                       Auth      `yaml:"auth"`
	Retention                  Retention `yaml:"retention"`
	Prefetch                   Prefetch  `yaml:"prefetch"`
	Cache                      Cache     `yaml:"cache"`
}

func Default() Config {
	enableShadowsocksJournalFallback := false
	return Config{
		Listen:                     defaultListen,
		DBPath:                     defaultDBPath,
		TickInterval:               defaultTickInterval,
		SocketIndexInterval:        defaultSocketIndexInterval,
		ProcFS:                     defaultProcFS,
		ShadowsocksJournalFallback: &enableShadowsocksJournalFallback,
		Retention: Retention{
			MinuteDays:   7,
			HourMonths:   3,
			DayMonths:    12,
			EvidenceDays: 14,
			ChainDays:    14,
		},
		Prefetch: Prefetch{
			Enabled:             true,
			Interval:            5 * time.Minute,
			EvidenceLookback:    20 * time.Minute,
			ChainLookback:       20 * time.Minute,
			ScanBudget:          2 * time.Second,
			MaxScanFiles:        3,
			MaxScanLinesPerFile: 80000,
		},
		Cache: Cache{
			CountCacheSize:  defaultCountCacheSize,
			ResultCacheSize: defaultResultCacheSize,
			SlidingTTL:      defaultSlidingTTL,
			ArchivedTTL:     defaultArchivedTTL,
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

// Derive applies runtime defaults to a Config value.
// Use this when a Config is constructed in-memory (without Load) before wiring services.
func Derive(cfg Config) Config {
	if cfg.Listen == "" {
		cfg.Listen = defaultListen
	}
	if cfg.DBPath == "" {
		cfg.DBPath = defaultDBPath
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.SocketIndexInterval <= 0 {
		cfg.SocketIndexInterval = defaultSocketIndexInterval
	}
	if cfg.ProcFS == "" {
		cfg.ProcFS = defaultProcFS
	}
	if cfg.ConntrackPath == "" {
		cfg.ConntrackPath = filepath.Join(cfg.ProcFS, "net", "nf_conntrack")
	}
	cfg.NetworkInterfaces = normalizeStringList(cfg.NetworkInterfaces)
	cfg.ProcessLogDirs = normalizeProcessLogDirs(cfg.ProcessLogDirs)
	if cfg.ShadowsocksJournalFallback == nil {
		enableShadowsocksJournalFallback := false
		cfg.ShadowsocksJournalFallback = &enableShadowsocksJournalFallback
	}
	if cfg.Retention.MinuteDays <= 0 {
		cfg.Retention.MinuteDays = 7
	}
	if cfg.Retention.HourMonths <= 0 {
		cfg.Retention.HourMonths = 3
	}
	if cfg.Retention.DayMonths <= 0 {
		cfg.Retention.DayMonths = 12
	}
	if cfg.Retention.EvidenceDays <= 0 {
		cfg.Retention.EvidenceDays = 14
	}
	if cfg.Retention.ChainDays <= 0 {
		cfg.Retention.ChainDays = 14
	}
	if cfg.Prefetch.Interval <= 0 {
		cfg.Prefetch.Interval = 5 * time.Minute
	}
	if cfg.Prefetch.EvidenceLookback <= 0 {
		cfg.Prefetch.EvidenceLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ChainLookback <= 0 {
		cfg.Prefetch.ChainLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ScanBudget <= 0 {
		cfg.Prefetch.ScanBudget = 2 * time.Second
	}
	if cfg.Prefetch.MaxScanFiles <= 0 {
		cfg.Prefetch.MaxScanFiles = 3
	}
	if cfg.Prefetch.MaxScanLinesPerFile <= 0 {
		cfg.Prefetch.MaxScanLinesPerFile = 80000
	}
	if cfg.Cache.CountCacheSize == 0 && !cfg.Cache.countCacheSizeSet {
		cfg.Cache.CountCacheSize = defaultCountCacheSize
	}
	if cfg.Cache.ResultCacheSize == 0 && !cfg.Cache.resultCacheSizeSet {
		cfg.Cache.ResultCacheSize = defaultResultCacheSize
	}
	if cfg.Cache.SlidingTTL == 0 && !cfg.Cache.slidingTTLSet {
		cfg.Cache.SlidingTTL = defaultSlidingTTL
	}
	if cfg.Cache.ArchivedTTL == 0 && !cfg.Cache.archivedTTLSet {
		cfg.Cache.ArchivedTTL = defaultArchivedTTL
	}
	cfg.Auth.Username = strings.TrimSpace(cfg.Auth.Username)
	cfg.Auth.Password = strings.TrimSpace(cfg.Auth.Password)
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

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func (c Config) Validate() error {
	authConfigured := c.Auth.Username != "" || c.Auth.Password != ""
	switch {
	case c.Listen == "":
		return errors.New("listen must not be empty")
	case c.DBPath == "":
		return errors.New("db_path must not be empty")
	case c.TickInterval <= 0:
		return errors.New("tick_interval must be positive")
	case c.SocketIndexInterval <= 0:
		return errors.New("socket_index_interval must be positive")
	case c.Retention.MinuteDays <= 0:
		return errors.New("retention.minute_days must be positive")
	case c.Retention.HourMonths <= 0:
		return errors.New("retention.hour_months must be positive")
	case c.Retention.DayMonths <= 0:
		return errors.New("retention.day_months must be positive")
	case c.Retention.EvidenceDays <= 0:
		return errors.New("retention.evidence_days must be positive")
	case c.Retention.ChainDays <= 0:
		return errors.New("retention.chain_days must be positive")
	case authConfigured && (c.Auth.Username == "" || c.Auth.Password == ""):
		return errors.New("auth.username and auth.password must both be set when auth is configured")
	case !authConfigured && !isLoopbackListenAddress(c.Listen):
		return errors.New("auth.username and auth.password are required when listen is not loopback")
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
	case c.Cache.CountCacheSize < 0:
		return errors.New("cache.count_cache_size must be >= 0")
	case c.Cache.ResultCacheSize < 0:
		return errors.New("cache.result_cache_size must be >= 0")
	case c.Cache.SlidingTTL < 0:
		return errors.New("cache.sliding_ttl must be >= 0")
	case c.Cache.ArchivedTTL < 0:
		return errors.New("cache.archived_ttl must be >= 0")
	default:
		return nil
	}
}

func isLoopbackListenAddress(listen string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		host = strings.TrimSpace(listen)
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
