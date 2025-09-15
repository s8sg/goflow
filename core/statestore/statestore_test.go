package statestore

import (
	"errors"
	"testing"
)

// --- Mock backend for abstraction test ---
type mockBackend struct {
	setCalled, getCalled, incrCalled, updateCalled, cleanupCalled bool
}

func (m *mockBackend) Configure(flowName, requestId string) {}
func (m *mockBackend) Init() error                          { return nil }
func (m *mockBackend) Set(key, value string) error          { m.setCalled = true; return nil }
func (m *mockBackend) Get(key string) (string, error)       { m.getCalled = true; return "val", nil }
func (m *mockBackend) Incr(key string, value int64) (int64, error) {
	m.incrCalled = true
	return 42, nil
}
func (m *mockBackend) Update(key, oldValue, newValue string) error { m.updateCalled = true; return nil }
func (m *mockBackend) Cleanup() error                              { m.cleanupCalled = true; return nil }
func (m *mockBackend) CopyStore() (StateBackend, error)            { return &mockBackend{}, nil }

func TestStateStore_DelegatesToBackend(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	s.Configure("f", "r")
	s.Init()
	s.Set("k", "v")
	s.Get("k")
	s.Incr("k", 1)
	s.Update("k", "old", "new")
	s.Cleanup()
	s.CopyStore()
	if !mb.setCalled || !mb.getCalled || !mb.incrCalled || !mb.updateCalled || !mb.cleanupCalled {
		t.Error("Not all backend methods were called")
	}
}

// --- RedisBackend unit test (mock redis client) ---
type fakeRedisClient struct {
	setErr, getErr, incrErr, delErr error
	store                           map[string]string
}

func (f *fakeRedisClient) Set(key string, value interface{}, expiration interface{}) *fakeStatusCmd {
	if f.setErr != nil {
		return &fakeStatusCmd{err: f.setErr}
	}
	f.store[key] = value.(string)
	return &fakeStatusCmd{}
}
func (f *fakeRedisClient) Get(key string) *fakeStringCmd {
	if f.getErr != nil {
		return &fakeStringCmd{err: f.getErr}
	}
	v, ok := f.store[key]
	if !ok {
		return &fakeStringCmd{err: errors.New("not found")}
	}
	return &fakeStringCmd{val: v}
}
func (f *fakeRedisClient) IncrBy(key string, value int64) *fakeIntCmd {
	if f.incrErr != nil {
		return &fakeIntCmd{err: f.incrErr}
	}
	return &fakeIntCmd{val: 42}
}
func (f *fakeRedisClient) Del(keys ...string) *fakeIntCmd {
	if f.delErr != nil {
		return &fakeIntCmd{err: f.delErr}
	}
	for _, k := range keys {
		delete(f.store, k)
	}
	return &fakeIntCmd{}
}
func (f *fakeRedisClient) Scan(cursor uint64, match string, count int64) *fakeScanCmd {
	return &fakeScanCmd{}
}
func (f *fakeRedisClient) Ping() *fakeStatusCmd { return &fakeStatusCmd{} }

// --- Fake redis command types ---
type fakeStatusCmd struct{ err error }

func (c *fakeStatusCmd) Err() error { return c.err }

type fakeStringCmd struct {
	val string
	err error
}

func (c *fakeStringCmd) Result() (string, error) { return c.val, c.err }

type fakeIntCmd struct {
	val int64
	err error
}

func (c *fakeIntCmd) Result() (int64, error) { return c.val, c.err }
func (c *fakeIntCmd) Err() error             { return c.err }

type fakeScanCmd struct{}

func (c *fakeScanCmd) Iterator() *fakeScanIterator { return &fakeScanIterator{} }

type fakeScanIterator struct{}

func (it *fakeScanIterator) Next() bool  { return false }
func (it *fakeScanIterator) Val() string { return "" }
func (it *fakeScanIterator) Err() error  { return nil }

// TODO: Add RedisBackend unit tests using fakeRedisClient
// TODO: Add FoundationDBBackend unit tests (when implemented)
