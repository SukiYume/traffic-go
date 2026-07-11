package embed

import (
	"embed"
	"fmt"
	"io/fs"
)

// Dist contains the embedded frontend bundle. The placeholder file allows backend
// compilation before the real Vite build has been copied into this directory.
//
//go:embed dist/*
var Dist embed.FS

func StaticFS() fs.FS {
	sub, err := fs.Sub(Dist, "dist")
	if err != nil {
		return Dist
	}
	return sub
}

// Validate makes an incomplete backend-only build fail at startup with an
// actionable error instead of serving a 503 for every WebUI request.
func Validate() error {
	info, err := fs.Stat(StaticFS(), "index.html")
	if err != nil {
		return fmt.Errorf("embedded WebUI is missing index.html; run `make build` (or build and sync web/dist) before starting traffic-go: %w", err)
	}
	if info.IsDir() || info.Size() == 0 {
		return fmt.Errorf("embedded WebUI index.html is not a non-empty file; run `make build`")
	}
	return nil
}
