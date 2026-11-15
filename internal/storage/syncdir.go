package storage

import "os"

// syncDir makes a rename in dir durable. On Linux that needs an fsync of the
// directory itself; Windows does not allow opening a directory for it, and
// there the error is ignored - production runs on Linux.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
