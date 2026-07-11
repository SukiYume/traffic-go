//go:build linux

package evidence

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestListLogFilesRejectsFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocked.log")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	if _, err := ListLogFiles(path, nil, 0, 0, 1); err == nil {
		t.Fatal("expected FIFO to be rejected without opening it")
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().IsRegular() {
		t.Fatalf("test FIFO setup failed: info=%v err=%v", info, err)
	}
}
