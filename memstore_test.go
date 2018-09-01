package blobs

import "context"

func init() {
	storerFactories = append(storerFactories, memstoreFactory{})
}

type memstoreFactory struct{}

func (m memstoreFactory) NewStorer(ctx context.Context) (Storer, error) {
	return NewMemstore()
}
