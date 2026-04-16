package config

import (
	"os"
	"path/filepath"
	"testing"
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
	if cfg.NginxLogDir != "/var/log/nginx" {
		t.Fatalf("expected default nginx_log_dir, got %s", cfg.NginxLogDir)
	}
	if cfg.SSLogDir != "/var/log" {
		t.Fatalf("expected default ss_log_dir, got %s", cfg.SSLogDir)
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
}
