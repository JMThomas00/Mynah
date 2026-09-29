//go:build ignore

// release builds mynah for every OS/CPU a Concord server runs on and packs
// each into dist/mynah_<os>_<arch>.zip -- the layout Concord's installer
// looks for when an admin installs from this repo's GitHub releases.
//
//	go run release.go            # version from the latest git tag
//	VERSION=1.2.0 go run release.go
package main

import (
	"archive/zip"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var targets = [][2]string{
	{"linux", "amd64"}, {"linux", "arm64"},
	{"windows", "amd64"},
	{"darwin", "amd64"}, {"darwin", "arm64"},
}

func main() {
	version := strings.TrimPrefix(os.Getenv("VERSION"), "v")
	if version == "" {
		if out, err := exec.Command("git", "describe", "--tags", "--abbrev=0").Output(); err == nil {
			version = strings.TrimPrefix(strings.TrimSpace(string(out)), "v")
		}
	}
	manifest, err := os.ReadFile("plugin.toml")
	check(err)
	if version != "" {
		manifest = regexp.MustCompile(`(?m)^version = "[^"]*"`).ReplaceAll(manifest, []byte(`version = "`+version+`"`))
	}
	check(os.MkdirAll("dist", 0o755))
	for _, t := range targets {
		goos, goarch := t[0], t[1]
		bin := "mynah"
		if goos == "windows" {
			bin += ".exe"
		}
		out := filepath.Join("dist", "build", goos+"_"+goarch, bin)
		// CGO off: a static binary runs on any Linux, including Concord's
		// Alpine Docker image.
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w", "-o", out, ".")
		cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		check(cmd.Run())

		zipPath := filepath.Join("dist", fmt.Sprintf("mynah_%s_%s.zip", goos, goarch))
		f, err := os.Create(zipPath)
		check(err)
		zw := zip.NewWriter(f)
		add := func(name string, data []byte, mode os.FileMode) {
			h := &zip.FileHeader{Name: "mynah/" + name, Method: zip.Deflate, Modified: time.Now()}
			h.SetMode(mode)
			w, err := zw.CreateHeader(h)
			check(err)
			_, err = w.Write(data)
			check(err)
		}
		binData, err := os.ReadFile(out)
		check(err)
		add(bin, binData, 0o755)
		add("plugin.toml", manifest, 0o644)
		check(zw.Close())
		check(f.Close())
		fmt.Println("packed", zipPath)
	}
	_ = os.RemoveAll(filepath.Join("dist", "build"))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "release:", err)
		os.Exit(1)
	}
}
