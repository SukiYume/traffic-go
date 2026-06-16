package api

import (
	"path/filepath"
	"testing"
)

func TestProcessLogDescriptorsDedupesEquivalentShadowsocksDirs(t *testing.T) {
	dir := t.TempDir()
	server := &Server{
		processLogDirs: map[string]string{
			"ss-server":   dir,
			"ss-manager":  filepath.Join(dir, "."),
			"obfs-server": filepath.Join(dir, "subdir", ".."),
		},
	}

	descriptors := server.processLogDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("expected one deduped descriptor, got %+v", descriptors)
	}
	if descriptors[0].EvidenceSource != evidenceSourceSS {
		t.Fatalf("expected shadowsocks evidence source, got %+v", descriptors[0])
	}
	if descriptors[0].LookupKey != "obfs-server" {
		t.Fatalf("expected deterministic first sorted lookup key, got %+v", descriptors[0])
	}
}
