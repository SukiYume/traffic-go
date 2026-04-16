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
}
