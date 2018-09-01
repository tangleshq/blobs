package blobs

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	yall "yall.in"
	"yall.in/colour"
)

type storerFactory interface {
	NewStorer(ctx context.Context) (Storer, error)
}

var storerFactories []storerFactory

func TestUploadDownloadDelete(t *testing.T) {
	type input struct {
		sha256 string
		crc32c uint32
		data   []byte
	}
	type output struct {
		blob Blob
		err  error
	}
	type uploadTest struct {
		in  input
		out output
	}
	table := map[string]uploadTest{
		"dothemath": uploadTest{
			in:  input{data: dothemathgif, sha256: "1c86d1005b0a4114c013c019653026bb955ada19344ca4391a2b6f72febf8668", crc32c: 98891635},
			out: output{blob: Blob{SHA256: "1c86d1005b0a4114c013c019653026bb955ada19344ca4391a2b6f72febf8668", Size: 1377666}},
		},
	}
	for _, storerFactory := range storerFactories {
		storerFactory := storerFactory
		t.Run("Storer="+fmt.Sprintf("%T", storerFactory), func(t *testing.T) {
			for id, testcase := range table {
				id, testcase := id, testcase
				t.Run("ID="+id, func(t *testing.T) {
					t.Parallel()
					ctx := context.Background()
					log := yall.New(colour.New(os.Stdout, yall.Debug))
					ctx = yall.InContext(ctx, log)

					storer, err := storerFactory.NewStorer(ctx)
					if err != nil {
						t.Errorf("Unexpected error setting up storer: %s", err)
						return
					}

					deps := Dependencies{
						Storer: storer,
					}

					buffer := bytes.NewBuffer(testcase.in.data)
					result, err := StreamingUpload(ctx, deps, IncomingBlob{
						SHA256:    testcase.in.sha256,
						Data:      ioutil.NopCloser(buffer),
						Extension: ".test",
						CRC32C:    testcase.in.crc32c,
					})
					if err != testcase.out.err {
						t.Errorf("Expected error to be %q, got %q", testcase.out.err, err)
						return
					}

					if result.Size != testcase.out.blob.Size {
						t.Errorf("Expected size to be %d, got %d", testcase.out.blob.Size, result.Size)
						return
					}
					if result.SHA256 != testcase.out.blob.SHA256 {
						t.Errorf("Expected SHA256 to be %q, got %q", testcase.out.blob.SHA256, result.SHA256)
						return
					}

					ctx = context.Background()
					var buf bytes.Buffer
					downloadReader, err := storer.Download(ctx, testcase.out.blob.SHA256)
					if err != testcase.out.err {
						t.Errorf("Expected error to be %q, got %q", testcase.out.err, err)
						return
					}
					_, err = io.Copy(&buf, downloadReader)
					if err != nil {
						t.Errorf("Unexpected error downloading to check contents")
						return
					}
					b := buf.Bytes()
					if !bytes.Equal(testcase.in.data, b) {
						t.Errorf("Expected download to be %q, got %q", hex.EncodeToString(testcase.in.data), hex.EncodeToString(b))
						return
					}

					ctx = context.Background()
					err = storer.Delete(ctx, testcase.out.blob.SHA256)
					if err != nil {
						t.Errorf("Unexpected error: %s", err)
						return
					}
					buf = bytes.Buffer{}
					downloadReader, err = storer.Download(ctx, testcase.out.blob.SHA256)
					if err != ErrHashNotFound {
						t.Errorf("Expected error to be %q, got %q", ErrHashNotFound, err)
						return
					}
					err = storer.Delete(ctx, testcase.out.blob.SHA256)
					if err != nil {
						t.Errorf("Unexpected error: %s", err)
						return
					}
				})
			}
		})
	}
}

// TODO(paddy): test a file that lies about its hash

// TODO(paddy): test uploading a file with the same hash as an existing file

// TODO(paddy): test uploading a file claiming its hash is the same as an existing file's when it isn't
// we need to be careful that doesn't delete the legitimate file!
