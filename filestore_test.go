package blobs

import (
	"context"
	"os"
	"path/filepath"

	uuid "github.com/hashicorp/go-uuid"
)

func init() {
	storerFactories = append(storerFactories, filestoreFactory{})
}

type filestoreFactory struct{}

func (f filestoreFactory) NewStorer(ctx context.Context) (Storer, error) {
	rand, err := uuid.GenerateUUID()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(os.TempDir(), "filestore-tests", rand)
	err = os.MkdirAll(path, 0744)
	if err != nil {
		return nil, err
	}
	return Filestore{Root: path}, nil
}
