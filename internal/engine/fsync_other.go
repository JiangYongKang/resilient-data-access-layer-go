//go:build !windows

package engine

import "os"

func syncDir(f *os.File) error { return f.Sync() }
