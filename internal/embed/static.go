package embed

import (
	"embed"
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
