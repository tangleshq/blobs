package blobs

import (
	"bytes"
	"context"
	"io"

	memdb "github.com/hashicorp/go-memdb"
)

var (
	schema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"blob": &memdb.TableSchema{
				Name: "blob",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "ID", Lowercase: true},
					},
				},
			},
		},
	}
)

// Memblob is an in-memory blob implementation, for storing
// in the Memstore.
type Memblob struct {
	ID       string
	Contents []byte
	buf      *bytes.Buffer
}

// NewMemblob returns a ready-to-use Memblob with the specified
// ID and contents.
func NewMemblob(id string, in []byte) *Memblob {
	b := bytes.NewBuffer(in)
	return &Memblob{
		ID:       id,
		Contents: in,
		buf:      b,
	}
}

// Write copies `p` into the contents of `m`, filling the io.Writer
// interface.
func (m *Memblob) Write(p []byte) (int, error) {
	n, err := m.buf.Write(p)
	if err != nil {
		return n, err
	}
	m.Contents = append(m.Contents, p...)
	return n, err
}

// Read inserts data from the contents of `m` into `p`, returning
// the number of bytes read, filling the io.Reader interface.
func (m *Memblob) Read(p []byte) (int, error) {
	return m.buf.Read(p)
}

// Close resets the buffer for `m`, filling the `io.Closer` interface.
func (m *Memblob) Close() error {
	m.buf = bytes.NewBuffer(m.Contents)
	return nil
}

// Memstore is an in-memory implementation of the Storer interface, best
// suited for testing.
type Memstore struct {
	db *memdb.MemDB
}

// Upload creates a new Memblob in the Memstore, and returns an `io.WriteCloser`
// to write to it. `sha256` and will be used as the ID of the Memblob,
// and `crc32c` will be ignored. This helps fill the Storer interface.
func (m *Memstore) Upload(ctx context.Context, sha256 string, crc32c uint32) (io.WriteCloser, error) {
	txn := m.db.Txn(true)
	defer txn.Abort()
	exists, err := txn.First("blob", "id", sha256)
	if err != nil {
		return nil, err
	}
	if exists != nil {
		return nil, nil
	}
	f := NewMemblob(sha256, []byte{})
	err = txn.Insert("blob", f)
	if err != nil {
		return nil, err
	}
	txn.Commit()
	return f, nil
}

// Download will locate the Memblob with an ID matching `hash`, and
// return an io.ReadCloser to retrieve its contents. This helps fill the Storer interface.
func (m *Memstore) Download(ctx context.Context, hash string) (io.ReadCloser, error) {
	txn := m.db.Txn(false)
	res, err := txn.First("blob", "id", hash)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, ErrHashNotFound
	}
	return res.(*Memblob), nil
}

// Delete will remove the Memblob with an ID matching `hash` from
// `m`. This helps fill the Storer interface.
func (m *Memstore) Delete(ctx context.Context, hash string) error {
	txn := m.db.Txn(true)
	defer txn.Abort()
	exists, err := txn.First("blob", "id", hash)
	if err != nil {
		return err
	}
	if exists == nil {
		return nil
	}
	err = txn.Delete("blob", exists)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

// BuildURL is not implemented, as Memstore has no way to surface the image without
// actually reading it through Download, so this doesn't apply.
func (m *Memstore) BuildURL(ctx context.Context, hash string) (string, error) {
	return "", nil
}

// NewMemstore returns a ready-to-use Memstore, which can be used as a Storer.
func NewMemstore() (*Memstore, error) {
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	return &Memstore{
		db: db,
	}, nil
}
