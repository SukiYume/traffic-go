package buildinfo

import "testing"

func TestCurrentIncludesRequiredFields(t *testing.T) {
	info := Current()
	if info.Version == "" || info.Commit == "" || info.BuildDate == "" || info.GoVersion == "" {
		t.Fatalf("incomplete build info: %+v", info)
	}
}
