package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsZeroRetentionValues(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("retention:\n  flows_days: 0\n  hourly_days: 0\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Retention.MinuteDays != 30 {
		t.Fatalf("expected default minute retention, got %d", cfg.Retention.MinuteDays)
	}
	if cfg.Retention.HourlyDays != 180 {
		t.Fatalf("expected default hourly retention, got %d", cfg.Retention.HourlyDays)
	}
	if cfg.NginxLogDir != "" {
		t.Fatalf("expected empty nginx_log_dir by default, got %s", cfg.NginxLogDir)
	}
	if cfg.SSLogDir != "" {
		t.Fatalf("expected empty ss_log_dir by default, got %s", cfg.SSLogDir)
	}
	if len(cfg.ProcessLogDirs) != 0 {
		t.Fatalf("expected no default process_log_dirs entries, got %v", cfg.ProcessLogDirs)
	}
	if !cfg.Prefetch.Enabled {
		t.Fatalf("expected prefetch enabled by default")
	}
	if cfg.Prefetch.Interval != time.Minute {
		t.Fatalf("expected default prefetch interval 1m, got %s", cfg.Prefetch.Interval)
	}
	if cfg.Prefetch.EvidenceLookback != 20*time.Minute {
		t.Fatalf("expected default evidence lookback 20m, got %s", cfg.Prefetch.EvidenceLookback)
	}
	if cfg.Prefetch.ChainLookback != 20*time.Minute {
		t.Fatalf("expected default chain lookback 20m, got %s", cfg.Prefetch.ChainLookback)
	}
	if cfg.Prefetch.ScanBudget != 8*time.Second {
		t.Fatalf("expected default scan budget 8s, got %s", cfg.Prefetch.ScanBudget)
	}
	if cfg.Prefetch.MaxScanFiles != 6 {
		t.Fatalf("expected default max scan files 6, got %d", cfg.Prefetch.MaxScanFiles)
	}
	if cfg.Prefetch.MaxScanLinesPerFile != 250000 {
		t.Fatalf("expected default max scan lines 250000, got %d", cfg.Prefetch.MaxScanLinesPerFile)
	}
}

func TestLoadNginxLogDirOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("nginx_log_dir: /data/nginx-logs\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.NginxLogDir != "/data/nginx-logs" {
		t.Fatalf("expected nginx_log_dir override, got %s", cfg.NginxLogDir)
	}
	if cfg.ProcessLogDirs["nginx"] != "/data/nginx-logs" {
		t.Fatalf("expected process_log_dirs.nginx merged from legacy override, got %v", cfg.ProcessLogDirs)
	}
}

func TestLoadSSLogDirOverride(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("ss_log_dir: /data/ss-logs\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.SSLogDir != "/data/ss-logs" {
		t.Fatalf("expected ss_log_dir override, got %s", cfg.SSLogDir)
	}
	if cfg.ProcessLogDirs["ss-server"] != "/data/ss-logs" {
		t.Fatalf("expected process_log_dirs.ss-server merged from legacy override, got %v", cfg.ProcessLogDirs)
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

func TestLoadProcessLogDirsTakesPriorityOverLegacyNginxLogDir(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte("nginx_log_dir: /legacy/nginx\nprocess_log_dirs:\n  nginx: /custom/nginx\n")
	if err := os.WriteFile(configPath, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProcessLogDirs["nginx"] != "/custom/nginx" {
		t.Fatalf("expected process_log_dirs.nginx to override legacy key, got %v", cfg.ProcessLogDirs)
	}
}

func TestDeriveMergesLegacyDirsIntoProcessLogDirs(t *testing.T) {
	cfg := Default()
	cfg.NginxLogDir = "/legacy/nginx"
	cfg.SSLogDir = "/legacy/ss"

	derived := Derive(cfg)

	if derived.ProcessLogDirs["nginx"] != "/legacy/nginx" {
		t.Fatalf("expected derived nginx path, got %v", derived.ProcessLogDirs)
	}
	if derived.ProcessLogDirs["ss-server"] != "/legacy/ss" {
		t.Fatalf("expected derived ss-server path, got %v", derived.ProcessLogDirs)
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

func TestDeriveRestoresPrefetchDefaultsForZeroValues(t *testing.T) {
	cfg := Default()
	cfg.Prefetch.Interval = 0
	cfg.Prefetch.EvidenceLookback = 0
	cfg.Prefetch.ChainLookback = 0
	cfg.Prefetch.ScanBudget = 0
	cfg.Prefetch.MaxScanFiles = 0
	cfg.Prefetch.MaxScanLinesPerFile = 0

	derived := Derive(cfg)

	if derived.Prefetch.Interval != time.Minute {
		t.Fatalf("expected derived interval 1m, got %s", derived.Prefetch.Interval)
	}
	if derived.Prefetch.EvidenceLookback != 20*time.Minute {
		t.Fatalf("expected derived evidence lookback 20m, got %s", derived.Prefetch.EvidenceLookback)
	}
	if derived.Prefetch.ChainLookback != 20*time.Minute {
		t.Fatalf("expected derived chain lookback 20m, got %s", derived.Prefetch.ChainLookback)
	}
	if derived.Prefetch.ScanBudget != 8*time.Second {
		t.Fatalf("expected derived scan budget 8s, got %s", derived.Prefetch.ScanBudget)
	}
	if derived.Prefetch.MaxScanFiles != 6 {
		t.Fatalf("expected derived max scan files 6, got %d", derived.Prefetch.MaxScanFiles)
	}
	if derived.Prefetch.MaxScanLinesPerFile != 250000 {
		t.Fatalf("expected derived max scan lines 250000, got %d", derived.Prefetch.MaxScanLinesPerFile)
	}
}
