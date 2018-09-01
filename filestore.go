package blobs

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// Filestore is a filesystem-based implementation of the Storer interface.
type Filestore struct {
	Root string
}

// Upload creates a new File in the Filestore, and returns an `io.WriteCloser`
// to write to it. `sha256` and will be used as the name of the File,
// and `crc32c` will be ignored. This helps fill the Storer interface.
func (s Filestore) Upload(ctx context.Context, sha256 string, crc32c uint32) (io.WriteCloser, error) {
	fullPath := filepath.Join(s.Root, sha256)
	f, err := os.OpenFile(fullPath, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.EEXIST {
			// file already exists
			return nil, nil
		}
		log.Fatal(err)
	}
	return f, nil
}

// Download will locate the File with a name matching `hash`, and
// return an io.ReadCloser to retrieve its contents. This helps fill the Storer interface.
func (s Filestore) Download(ctx context.Context, hash string) (io.ReadCloser, error) {
	fullPath := filepath.Join(s.Root, hash)
	f, err := os.Open(fullPath)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			return nil, ErrHashNotFound
		}
		return nil, err
	}
	return f, nil
}

// Delete will remove the File with a name matching `hash` from
// the directory in `s.Root`. This helps fill the Storer interface.
func (s Filestore) Delete(ctx context.Context, hash string) error {
	fullPath := filepath.Join(s.Root, hash)
	err := os.Remove(fullPath)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
			return nil
		}
		return err
	}
	return nil
}

// BuildURL is not implemented, as Filestore has no way to surface the image without
// actually reading it through Download, so this doesn't apply.
func (s Filestore) BuildURL(ctx context.Context, hash string) (string, error) {
	return "", nil
}
