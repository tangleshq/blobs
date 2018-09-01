package blobs

import (
	"context"
	"io"
)

// Dependencies is a bundle of information that every non-trivial
// function will need access to, but which is not directly relevant
// to the main purpose of the function or request-scoped. Connections,
// clients, and other dependency injection usual suspects are good
// candidates for inclusion in Dependencies.
type Dependencies struct {
	Storer Storer
}

// Storer is the main blob-storage interface. Implementations are
// encouraged to not buffer full blobs in memory, instead using the
// returned io.WriteCloser and io.ReadClosers to write and read them
// in a streaming fashion. An implementation must offer a Download or
// a BuildURL method, or both. BuildURL will be privileged over Download
// when possible, to leverage CDNs and avoid smuggling all the blobs
// through the server unnecessarily.
type Storer interface {
	// Write the blob to the Storer, with the specified SHA256 as the
	// ID. The specified CRC32C hash is there to do integrity checks.
	Upload(ctx context.Context, sha256 string, crc32c uint32) (io.WriteCloser, error)

	// Download the blob from the Storer. Return `nil, nil` if Download
	// isn't supported by an implementation.
	Download(ctx context.Context, sha256 string) (io.ReadCloser, error)

	// Remove the blob specified by `sha256` from the Storer.
	Delete(ctx context.Context, sha256 string) error

	// Build a URL for a publicly-viewable version of the Blob. Return
	// `"", nil` if the implementation doesn't support URL building.
	BuildURL(ctx context.Context, sha256 string) (string, error)
}
