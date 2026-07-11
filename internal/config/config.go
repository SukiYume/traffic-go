package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
	defaultPrefetchInterval    = 30 * time.Minute
	defaultPrefetchScanBudget  = 500 * time.Millisecond
	defaultPrefetchMaxFiles    = 2
	defaultPrefetchMaxLines    = 10000
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
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("cache must be a mapping")
	}
	allowed := map[string]struct{}{
		"count_cache_size":  {},
		"result_cache_size": {},
		"sliding_ttl":       {},
		"archived_ttl":      {},
	}
	for index := 0; index+1 < len(value.Content); index += 2 {
		key := value.Content[index]
		if _, ok := allowed[key.Value]; !ok {
			return fmt.Errorf("field %s not found in type config.Cache", key.Value)
		}
	}
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
	ShadowsocksJournalFallback bool      `yaml:"shadowsocks_journal_fallback"`
	MockData                   bool      `yaml:"mock_data"`
	Auth                       Auth      `yaml:"auth"`
	Retention                  Retention `yaml:"retention"`
	Prefetch                   Prefetch  `yaml:"prefetch"`
	Cache                      Cache     `yaml:"cache"`
}

func Default() Config {
	return Config{
		Listen:              defaultListen,
		DBPath:              defaultDBPath,
		TickInterval:        defaultTickInterval,
		SocketIndexInterval: defaultSocketIndexInterval,
		ProcFS:              defaultProcFS,
		Retention: Retention{
			MinuteDays:   7,
			HourMonths:   3,
			DayMonths:    12,
			EvidenceDays: 14,
			ChainDays:    14,
		},
		Prefetch: Prefetch{
			Enabled:             true,
			Interval:            defaultPrefetchInterval,
			EvidenceLookback:    20 * time.Minute,
			ChainLookback:       20 * time.Minute,
			ScanBudget:          defaultPrefetchScanBudget,
			MaxScanFiles:        defaultPrefetchMaxFiles,
			MaxScanLinesPerFile: defaultPrefetchMaxLines,
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
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := validateExplicitValues(cfg); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	cfg = Derive(cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validateExplicitValues runs before Derive so negative values supplied by a
// configuration file cannot be silently replaced with runtime defaults. Zero
// retains the existing documented meaning of "use the default" (except for
// cache sizes and TTLs, where an explicit zero disables that cache).
func validateExplicitValues(c Config) error {
	switch {
	case c.TickInterval < 0:
		return errors.New("tick_interval must not be negative")
	case c.SocketIndexInterval < 0:
		return errors.New("socket_index_interval must not be negative")
	case c.Retention.MinuteDays < 0:
		return errors.New("retention.minute_days must not be negative")
	case c.Retention.HourMonths < 0:
		return errors.New("retention.hour_months must not be negative")
	case c.Retention.DayMonths < 0:
		return errors.New("retention.day_months must not be negative")
	case c.Retention.EvidenceDays < 0:
		return errors.New("retention.evidence_days must not be negative")
	case c.Retention.ChainDays < 0:
		return errors.New("retention.chain_days must not be negative")
	case c.Prefetch.Interval < 0:
		return errors.New("prefetch.interval must not be negative")
	case c.Prefetch.EvidenceLookback < 0:
		return errors.New("prefetch.evidence_lookback must not be negative")
	case c.Prefetch.ChainLookback < 0:
		return errors.New("prefetch.chain_lookback must not be negative")
	case c.Prefetch.ScanBudget < 0:
		return errors.New("prefetch.scan_budget must not be negative")
	case c.Prefetch.MaxScanFiles < 0:
		return errors.New("prefetch.max_scan_files must not be negative")
	case c.Prefetch.MaxScanLinesPerFile < 0:
		return errors.New("prefetch.max_scan_lines_per_file must not be negative")
	default:
		return nil
	}
}

// Derive applies runtime defaults to a Config value.
// Use this when a Config is constructed in-memory (without Load) before wiring services.
func Derive(cfg Config) Config {
	cfg.Listen = strings.TrimSpace(cfg.Listen)
	if cfg.Listen == "" {
		cfg.Listen = defaultListen
	}
	if cfg.DBPath == "" {
		cfg.DBPath = defaultDBPath
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.SocketIndexInterval == 0 {
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
	if cfg.Retention.MinuteDays == 0 {
		cfg.Retention.MinuteDays = 7
	}
	if cfg.Retention.HourMonths == 0 {
		cfg.Retention.HourMonths = 3
	}
	if cfg.Retention.DayMonths == 0 {
		cfg.Retention.DayMonths = 12
	}
	if cfg.Retention.EvidenceDays == 0 {
		cfg.Retention.EvidenceDays = 14
	}
	if cfg.Retention.ChainDays == 0 {
		cfg.Retention.ChainDays = 14
	}
	if cfg.Prefetch.Interval == 0 {
		cfg.Prefetch.Interval = defaultPrefetchInterval
	}
	if cfg.Prefetch.EvidenceLookback == 0 {
		cfg.Prefetch.EvidenceLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ChainLookback == 0 {
		cfg.Prefetch.ChainLookback = 20 * time.Minute
	}
	if cfg.Prefetch.ScanBudget == 0 {
		cfg.Prefetch.ScanBudget = defaultPrefetchScanBudget
	}
	if cfg.Prefetch.MaxScanFiles == 0 {
		cfg.Prefetch.MaxScanFiles = defaultPrefetchMaxFiles
	}
	if cfg.Prefetch.MaxScanLinesPerFile == 0 {
		cfg.Prefetch.MaxScanLinesPerFile = defaultPrefetchMaxLines
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
	username := strings.TrimSpace(c.Auth.Username)
	password := strings.TrimSpace(c.Auth.Password)
	authProvided := c.Auth.Username != "" || c.Auth.Password != ""
	authConfigured := username != "" && password != ""
	if err := validateListenAddress(c.Listen); err != nil {
		return err
	}
	switch {
	case c.DBPath == "":
		return errors.New("db_path must not be empty")
	case strings.ContainsAny(c.DBPath, "\x00\r\n"):
		return errors.New("db_path must not contain NUL or line breaks")
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
	case authProvided && !authConfigured:
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

func validateListenAddress(listen string) error {
	trimmed := strings.TrimSpace(listen)
	if trimmed == "" {
		return errors.New("listen must not be empty")
	}
	host, portText, err := net.SplitHostPort(trimmed)
	if err != nil {
		return fmt.Errorf("listen must be a valid host:port address: %w", err)
	}
	for _, char := range portText {
		if char < '0' || char > '9' {
			return errors.New("listen port must contain decimal digits only")
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("listen port must be an integer between 1 and 65535")
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil
	}
	addressHost := host
	if zoneIndex := strings.LastIndexByte(addressHost, '%'); zoneIndex > 0 {
		zone := addressHost[zoneIndex+1:]
		if !strings.Contains(addressHost[:zoneIndex], ":") || !validZone(zone) {
			return errors.New("listen IPv6 zone is invalid")
		}
		addressHost = addressHost[:zoneIndex]
	}
	if net.ParseIP(addressHost) == nil && !validHostname(host) {
		return errors.New("listen host must be an IP address or valid DNS hostname")
	}
	return nil
}

func validZone(zone string) bool {
	if zone == "" {
		return false
	}
	for _, char := range zone {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '.' && char != '-' {
			return false
		}
	}
	return true
}

func validHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func isLoopbackListenAddress(listen string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(listen))
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if zoneIndex := strings.LastIndexByte(host, '%'); zoneIndex > 0 {
		host = host[:zoneIndex]
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
