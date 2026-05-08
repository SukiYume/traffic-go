package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsZeroRetentionValues(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("retention:\n  minute_days: 0\n  hour_months: 0\n  day_months: 0\n  evidence_days: 0\n  chain_days: 0\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Retention.MinuteDays != 7 {
		t.Fatalf("expected default minute retention, got %d", cfg.Retention.MinuteDays)
	}
	if cfg.Retention.HourMonths != 3 {
		t.Fatalf("expected default hour retention, got %d", cfg.Retention.HourMonths)
	}
	if cfg.Retention.DayMonths != 12 {
		t.Fatalf("expected default day retention, got %d", cfg.Retention.DayMonths)
	}
	if cfg.Retention.EvidenceDays != 14 {
		t.Fatalf("expected default evidence retention, got %d", cfg.Retention.EvidenceDays)
	}
	if cfg.Retention.ChainDays != 14 {
		t.Fatalf("expected default chain retention, got %d", cfg.Retention.ChainDays)
	}
	if len(cfg.ProcessLogDirs) != 0 {
		t.Fatalf("expected no default process_log_dirs entries, got %v", cfg.ProcessLogDirs)
	}
	if cfg.ShadowsocksJournalFallback == nil || *cfg.ShadowsocksJournalFallback {
		t.Fatalf("expected shadowsocks_journal_fallback disabled by default, got %+v", cfg.ShadowsocksJournalFallback)
	}
	if !cfg.Prefetch.Enabled {
		t.Fatalf("expected prefetch enabled by default")
	}
	if cfg.Prefetch.Interval != 5*time.Minute {
		t.Fatalf("expected default prefetch interval 5m, got %s", cfg.Prefetch.Interval)
	}
	if cfg.Prefetch.EvidenceLookback != 20*time.Minute {
		t.Fatalf("expected default evidence lookback 20m, got %s", cfg.Prefetch.EvidenceLookback)
	}
	if cfg.Prefetch.ChainLookback != 20*time.Minute {
		t.Fatalf("expected default chain lookback 20m, got %s", cfg.Prefetch.ChainLookback)
	}
	if cfg.Prefetch.ScanBudget != 2*time.Second {
		t.Fatalf("expected default scan budget 2s, got %s", cfg.Prefetch.ScanBudget)
	}
	if cfg.Prefetch.MaxScanFiles != 3 {
		t.Fatalf("expected default max scan files 3, got %d", cfg.Prefetch.MaxScanFiles)
	}
	if cfg.Prefetch.MaxScanLinesPerFile != 80000 {
		t.Fatalf("expected default max scan lines 80000, got %d", cfg.Prefetch.MaxScanLinesPerFile)
	}
	if cfg.SocketIndexInterval != 10*time.Second {
		t.Fatalf("expected default socket index interval 10s, got %s", cfg.SocketIndexInterval)
	}
}

func TestLoadRetentionOverrides(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("retention:\n  minute_days: 10\n  hour_months: 4\n  day_months: 6\n  evidence_days: 3\n  chain_days: 5\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Retention.MinuteDays != 10 || cfg.Retention.HourMonths != 4 || cfg.Retention.DayMonths != 6 || cfg.Retention.EvidenceDays != 3 || cfg.Retention.ChainDays != 5 {
		t.Fatalf("unexpected retention override: %+v", cfg.Retention)
	}
}

func TestLoadProcessLogDirsOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("process_log_dirs:\n  frps: /var/log/frps\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProcessLogDirs["frps"] != "/var/log/frps" {
		t.Fatalf("expected process_log_dirs.frps override, got %v", cfg.ProcessLogDirs)
	}
	if len(cfg.ProcessLogDirs) != 1 {
		t.Fatalf("expected only configured process_log_dirs entries, got %v", cfg.ProcessLogDirs)
	}
}

func TestLoadShadowsocksJournalFallbackOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("shadowsocks_journal_fallback: true\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ShadowsocksJournalFallback == nil {
		t.Fatalf("expected shadowsocks_journal_fallback to be set")
	}
	if !*cfg.ShadowsocksJournalFallback {
		t.Fatalf("expected shadowsocks_journal_fallback override true, got false")
	}
}

func TestLoadProcessLogDirsNormalizesKeysAndDropsEmptyValues(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("process_log_dirs:\n  FRPS: /var/log/frps\n  xray: '   '\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProcessLogDirs["frps"] != "/var/log/frps" {
		t.Fatalf("expected normalized key frps, got %v", cfg.ProcessLogDirs)
	}
	if _, exists := cfg.ProcessLogDirs["xray"]; exists {
		t.Fatalf("expected empty xray value to be dropped, got %v", cfg.ProcessLogDirs)
	}
	if len(cfg.ProcessLogDirs) != 1 {
		t.Fatalf("expected only explicit non-empty process_log_dirs entries, got %v", cfg.ProcessLogDirs)
	}
}

func TestLoadNetworkInterfacesNormalizesValues(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("network_interfaces:\n  - ' eth0 '\n  - ens3\n  - eth0\n  - '   '\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	expected := []string{"eth0", "ens3"}
	if len(cfg.NetworkInterfaces) != len(expected) {
		t.Fatalf("expected network interfaces %v, got %v", expected, cfg.NetworkInterfaces)
	}
	for index, value := range expected {
		if cfg.NetworkInterfaces[index] != value {
			t.Fatalf("expected network interface %q at %d, got %q", value, index, cfg.NetworkInterfaces[index])
		}
	}
}

func TestLoadSocketIndexIntervalOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("socket_index_interval: 3s\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.SocketIndexInterval != 3*time.Second {
		t.Fatalf("expected socket index interval 3s, got %s", cfg.SocketIndexInterval)
	}
}

func TestLoadPrefetchOverrides(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("prefetch:\n  enabled: false\n  interval: 2m\n  evidence_lookback: 45m\n  chain_lookback: 15m\n  scan_budget: 3s\n  max_scan_files: 9\n  max_scan_lines_per_file: 123456\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Prefetch.Enabled {
		t.Fatalf("expected prefetch disabled override")
	}
	if cfg.Prefetch.Interval != 2*time.Minute {
		t.Fatalf("expected prefetch interval 2m, got %s", cfg.Prefetch.Interval)
	}
	if cfg.Prefetch.EvidenceLookback != 45*time.Minute {
		t.Fatalf("expected evidence lookback 45m, got %s", cfg.Prefetch.EvidenceLookback)
	}
	if cfg.Prefetch.ChainLookback != 15*time.Minute {
		t.Fatalf("expected chain lookback 15m, got %s", cfg.Prefetch.ChainLookback)
	}
	if cfg.Prefetch.ScanBudget != 3*time.Second {
		t.Fatalf("expected scan budget 3s, got %s", cfg.Prefetch.ScanBudget)
	}
	if cfg.Prefetch.MaxScanFiles != 9 {
		t.Fatalf("expected max scan files 9, got %d", cfg.Prefetch.MaxScanFiles)
	}
	if cfg.Prefetch.MaxScanLinesPerFile != 123456 {
		t.Fatalf("expected max scan lines 123456, got %d", cfg.Prefetch.MaxScanLinesPerFile)
	}
}

func TestLoadAuthConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("auth:\n  username: admin\n  password: secret\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Auth.Username != "admin" || cfg.Auth.Password != "secret" {
		t.Fatalf("expected auth config to load, got %+v", cfg.Auth)
	}
}

func TestValidateRequiresAuthForNonLoopbackListen(t *testing.T) {
	cfg := Default()
	cfg.Listen = "0.0.0.0:18080"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "auth.username") {
		t.Fatalf("expected auth requirement for non-loopback listen, got %v", err)
	}

	cfg.Auth.Username = "admin"
	cfg.Auth.Password = "secret"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected auth-protected non-loopback listen to validate: %v", err)
	}
}

func TestValidateRequiresPositiveSocketIndexInterval(t *testing.T) {
	cfg := Default()
	cfg.SocketIndexInterval = -time.Second
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "socket_index_interval") {
		t.Fatalf("expected socket index interval validation error, got %v", err)
	}
}

func TestDeriveRestoresPrefetchDefaultsForZeroValues(t *testing.T) {
	cfg := Default()
	cfg.SocketIndexInterval = 0
	cfg.Prefetch.Interval = 0
	cfg.Prefetch.EvidenceLookback = 0
	cfg.Prefetch.ChainLookback = 0
	cfg.Prefetch.ScanBudget = 0
	cfg.Prefetch.MaxScanFiles = 0
	cfg.Prefetch.MaxScanLinesPerFile = 0

	derived := Derive(cfg)

	if derived.Prefetch.Interval != 5*time.Minute {
		t.Fatalf("expected derived interval 5m, got %s", derived.Prefetch.Interval)
	}
	if derived.Prefetch.EvidenceLookback != 20*time.Minute {
		t.Fatalf("expected derived evidence lookback 20m, got %s", derived.Prefetch.EvidenceLookback)
	}
	if derived.Prefetch.ChainLookback != 20*time.Minute {
		t.Fatalf("expected derived chain lookback 20m, got %s", derived.Prefetch.ChainLookback)
	}
	if derived.Prefetch.ScanBudget != 2*time.Second {
		t.Fatalf("expected derived scan budget 2s, got %s", derived.Prefetch.ScanBudget)
	}
	if derived.Prefetch.MaxScanFiles != 3 {
		t.Fatalf("expected derived max scan files 3, got %d", derived.Prefetch.MaxScanFiles)
	}
	if derived.Prefetch.MaxScanLinesPerFile != 80000 {
		t.Fatalf("expected derived max scan lines 80000, got %d", derived.Prefetch.MaxScanLinesPerFile)
	}
	if derived.SocketIndexInterval != 10*time.Second {
		t.Fatalf("expected derived socket index interval 10s, got %s", derived.SocketIndexInterval)
	}
}

func TestDefaultCacheValues(t *testing.T) {
	cfg := Default()
	if cfg.Cache.CountCacheSize != 256 {
		t.Fatalf("count_cache_size default = %d, want 256", cfg.Cache.CountCacheSize)
	}
	if cfg.Cache.ResultCacheSize != 16 {
		t.Fatalf("result_cache_size default = %d, want 16", cfg.Cache.ResultCacheSize)
	}
	if cfg.Cache.SlidingTTL != 60*time.Second {
		t.Fatalf("sliding_ttl default = %v, want 60s", cfg.Cache.SlidingTTL)
	}
	if cfg.Cache.ArchivedTTL != 3600*time.Second {
		t.Fatalf("archived_ttl default = %v, want 3600s", cfg.Cache.ArchivedTTL)
	}
}

func TestCacheValidateNegative(t *testing.T) {
	cfg := Default()
	cfg.Cache.CountCacheSize = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative count_cache_size")
	}
}

func TestLoadCacheAllowsExplicitZeroSizes(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("cache:\n  count_cache_size: 0\n  result_cache_size: 0\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Cache.CountCacheSize != 0 || cfg.Cache.ResultCacheSize != 0 {
		t.Fatalf("expected explicit zero cache sizes to be preserved, got %+v", cfg.Cache)
	}
	if cfg.Cache.SlidingTTL != 60*time.Second || cfg.Cache.ArchivedTTL != 3600*time.Second {
		t.Fatalf("expected omitted cache ttls to use defaults, got %+v", cfg.Cache)
	}
}
