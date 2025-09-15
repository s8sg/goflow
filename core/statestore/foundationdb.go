package statestore

import (
	"errors"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type FoundationDBBackend struct {
	db      fdb.Database
	subspace directory.Subspace
	mu      sync.Mutex
}
func NewFoundationDBBackend(clusterFile string) (*FoundationDBBackend, error) {
	// TODO: Implement FoundationDB backend initialization
	return nil, errors.New("FoundationDB backend not implemented yet")
}

func (f *FoundationDBBackend) Configure(flowName, requestId string) {}
func (f *FoundationDBBackend) CopyStore() (StateBackend, error) {
	return nil, errors.New("not implemented")
}
func (f *FoundationDBBackend) Get(key string) (string, error) {
	return "", errors.New("not implemented")
}
func (f *FoundationDBBackend) Incr(key string, value int64) (int64, error) {
	return 0, errors.New("not implemented")
}
func (f *FoundationDBBackend) Init() error { return errors.New("not implemented") }
func (f *FoundationDBBackend) Set(key string, value string) error {
	return errors.New("not implemented")
}
func (f *FoundationDBBackend) Update(key, oldValue, newValue string) error {
	return errors.New("not implemented")
}
func (f *FoundationDBBackend) Cleanup() error { return errors.New("not implemented") }
