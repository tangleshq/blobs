package blobs // import "tangl.es/code/blobs"

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"gitlab.com/paddycarver/magic-number-checker/checker"
	yall "yall.in"
)

var (
	// ErrHashNotFound is returned when the specified hash can't be found in the Storer
	ErrHashNotFound = errors.New("hash not found")
)

// A Blob represents an opaque binary set of data. It does not contain the data
// in question, but does contain the metadata about the data.
type Blob struct {
	SHA256      string
	Size        int64
	ContentType string
}

// IncomingBlob represents an incoming blob. It holds the asserted
// SHA256 of the blob, and the reader that the blob may be obtained
// from.
type IncomingBlob struct {
	SHA256 string
	Data   io.ReadCloser
	CRC32C uint32
}

// StreamingUpload reads a blob from data and writes it to the Storer in d without
// reading the entire blob into memory first. It returns the blob that was uploaded or an
// error.
//
// The caller is responsible for verifying that the specified SHA256 isn't present in the
// Storer before the upload. If the content does not match the specified SHA256 hash,
// the upload will be aborted and the uploaded data will be deleted.
func StreamingUpload(ctx context.Context, d Dependencies, in IncomingBlob) (Blob, error) {
	logger := yall.FromContext(ctx).WithField("storer", fmt.Sprintf("%T", d.Storer))
	logger = logger.WithField("data_source", fmt.Sprintf("%T", in.Data))
	logger = logger.WithField("sha256_claim", in.SHA256)
	logger = logger.WithField("crc32c_claim", in.CRC32C)

	logger.Info("uploading")

	defer in.Data.Close()

	fileTypeWriter := &checker.MagicNumberChecker{
		SupportedMIMEs: []string{
			"image/gif",
			"image/jpeg",
			"image/jpg",
			"image/png",
			"image/webp",
		},
	}

	hashWriter := sha256.New()
	storerWriter, err := d.Storer.Upload(ctx, in.SHA256, in.CRC32C)
	if err != nil {
		logger.WithError(err).Error("error starting upload to storer")
		return Blob{}, err
	}

	if storerWriter == nil {
		logger.Debug("Storer nil")
		return Blob{}, nil
	}

	multi := io.MultiWriter(fileTypeWriter, hashWriter, storerWriter)

	logger.Debug("starting upload")
	size, err := io.Copy(multi, in.Data)
	if err != nil {
		logger.WithError(err).Error("error during upload")
		return Blob{}, err
	}
	err = fileTypeWriter.Close()
	if err != nil {
		logger.WithError(err).Error("error closing filetype writer")
		return Blob{}, err
	}
	err = storerWriter.Close()
	if err != nil {
		logger.WithError(err).Error("error closing blob storer")
		return Blob{}, err
	}

	logger.WithField("size", size).Debug("upload written")
	sum := hex.EncodeToString(hashWriter.Sum(nil))
	if sum != in.SHA256 {
		logger = logger.WithField("real_hash", string(sum))
		logger.Debug("file did not match hash, deleting")
		err = d.Storer.Delete(ctx, in.SHA256)
		if err != nil {
			logger.WithError(err).Error("error deleting file that did not match hash")
			return Blob{}, err
		}
		logger.Debug("deleted file that did not match hash")
		return Blob{}, errors.New("supplied hash did not match file hash")
	}
	logger.Info("completed upload")
	return Blob{
		SHA256:      sum,
		Size:        size,
		ContentType: fileTypeWriter.MatchedMIME,
	}, nil
}
