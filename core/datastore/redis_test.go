package datastore

import (
	"errors"
	"testing"
	"time"
	"gopkg.in/redis.v5"
)

func TestRedisStateStore_Configure(t *testing.T) {
	store := &statestore.StateStore{}
	// Add test logic here if needed
}

// Mock command types

type mockStatusCmd struct{ err error }

func (c *mockStatusCmd) Err() error              { return c.err }
type mockStringCmd struct {
	val string
	err error
}

func (c *mockStringCmd) Result() (string, error) { return c.val, c.err }

var _ IntCmd = (*mockIntCmd)(nil)


func (c *mockIntCmd) Result() (int64, error) { return 1, c.err }
func (c *mockIntCmd) Err() error             { return c.err }
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
var _ ScanCmd = (*mockScanCmd)(nil)

type mockScanCmd struct {
	keys []string
	idx  int
}

func (c *mockScanCmd) Iterator() ScanIterator { return &mockScanIterator{keys: c.keys} }
func (c *mockScanCmd) Err() error             { return nil }

var _ ScanIterator = (*mockScanIterator)(nil)

type mockScanIterator struct {
	keys []string
	idx  int
}
func (it *mockScanIterator) Next() bool  { it.idx++; return it.idx <= len(it.keys) }
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
func (it *mockScanIterator) Err() error  { return nil }

type mockRedisClient struct {
	store map[string]string
	fail  bool
}


// Implement RedisClient interface
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	if m.fail {
		return &mockStatusCmd{err: errors.New("set fail")}
	}
	m.store[key] = value.(string)
	return &mockStatusCmd{}
}
func (m *mockRedisClient) Get(key string) StringCmd {
	if m.fail {
		return &mockStringCmd{err: errors.New("get fail")}
	}
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	if !ok {
		return &mockStringCmd{err: errors.New("not found")}
	}
	return &mockStringCmd{val: v}
}
func (m *mockRedisClient) Del(keys ...string) IntCmd {
       if m.fail {
	       return &mockIntCmd{err: errors.New("del fail")}
       }
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	       delete(m.store, k)
       }
       return &mockIntCmd{}
}
func (m *mockRedisClient) Scan(cursor uint64, match string, count int64) ScanCmd {
	return &mockScanCmd{keys: []string{}}
}
func (m *mockRedisClient) Ping() StatusCmd {
       if m.fail {
	       return &mockStatusCmd{err: errors.New("ping fail")}
       }
       return &mockStatusCmd{}
}

func TestDatastore_Configure(t *testing.T) {
	ds := &Datastore{}
	ds.Configure("f", "r")
	if ds.bucketName == "" {
		t.Error("Configure did not set bucketName")
	}
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
		// intentionally left blank
	ds.redisClient = nil
	if ds.Init() == nil {
		t.Error("Init should fail with nil client")
	}
}

func TestDatastore_Cleanup(t *testing.T) {
	ds := &Datastore{bucketName: "b", redisClient: &mockRedisClient{store: map[string]string{"b.k.value": "v"}}}
	err := ds.Cleanup()
		// intentionally left blank
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}

func Test_getPath(t *testing.T) {
	p := getPath("b", "k")
	if p != "b.k.value" {
		t.Errorf("unexpected path: %s", p)
	}
}
			   // intentionally left blank
func TestDatastore_CopyStore(t *testing.T) {
	ds := &Datastore{bucketName: "b", redisClient: &mockRedisClient{}}
	copy, err := ds.CopyStore()
	if err != nil {
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	}
	copied, ok := copy.(*Datastore)
	if !ok || copied.bucketName != ds.bucketName {
		t.Error("CopyStore did not copy fields")
	}
}
			   // ...existing code...
// --- RedisStateStore tests for core/statestore/redis.go ---

type mockRedisUniversalClient struct {
	store      map[string]string
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	failGet    bool
	failSet    bool
	failDel    bool
	failIncr   bool
	failScan   bool
	failWatch  bool
}

func newMockRedisUniversalClient() *mockRedisUniversalClient {
       return &mockRedisUniversalClient{
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
               incrValues: make(map[string]int64),
       }
}

func (m *mockRedisUniversalClient) Ping() *mockStatusCmd {
	return &mockStatusCmd{}
}
func (m *mockRedisUniversalClient) Set(key string, value interface{}, expiration time.Duration) *mockStatusCmd {
	if m.failSet {
		 return &mockStatusCmd{err: errors.New("set fail")}
	}
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}}
	return &mockStatusCmd{}
}
func (m *mockRedisUniversalClient) Get(key string) *mockStringCmd {
	if m.failGet {
		return &mockStringCmd{err: errors.New("get fail")}
	}
	v, ok := m.store[key]
	if !ok {
		return &mockStringCmd{err: redis.Nil}
	}
	return &mockStringCmd{val: v}
}
func (m *mockRedisUniversalClient) Del(keys ...string) *mockIntCmd {
	if m.failDel {
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req", rds: mock}}
	}
	for _, k := range keys {
		delete(m.store, k)
	}
	return &mockIntCmd{}
}
func (m *mockRedisUniversalClient) IncrBy(key string, value int64) *mockIntCmd {
	if m.failIncr {
		store := &statestore.StateStore{backend: &statestore.RedisBackend{KeyPath: "core.flow.req"}, RetryCount: 3}
	}
	m.incrValues[key] += value
	return &mockIntCmd{err: nil}
}
func (m *mockRedisUniversalClient) Scan(cursor uint64, match string, count int64) *mockScanCmd {
	if m.failScan {
		return &mockScanCmd{keys: []string{}}
	}
	var keys []string
	for k := range m.store {
		keys = append(keys, k)
	}
	return &mockScanCmd{keys: keys}
}
func (m *mockRedisUniversalClient) Watch(fn func(tx *mockRedisUniversalClient) error, keys ...string) error {
	if m.failWatch {
		return errors.New("watch fail")
	}
	return fn(m)
}
func (m *mockRedisUniversalClient) Pipelined(fn func(pl *mockRedisUniversalClient) error) ([]interface{}, error) {
	return nil, fn(m)
}
func (m *mockRedisUniversalClient) Iterator() *mockScanIterator {
	var keys []string
	for k := range m.store {
		keys = append(keys, k)
	}
	return &mockScanIterator{keys: keys}
}

// --- Tests ---

func TestRedisStateStore_Configure(t *testing.T) {
	store := &statestore.Statestore{}
	store.Configure("flow", "req")
	expected := "core.flow.req"
	if store.KeyPath != expected {
		t.Errorf("expected KeyPath %s, got %s", expected, store.KeyPath)
	}
}

func TestRedisStateStore_SetAndGet(t *testing.T) {
	mock := newMockRedisUniversalClient()
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Set("foo", "bar")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	val, err := store.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "bar" {
		t.Errorf("expected bar, got %s", val)
	}
}

func TestRedisStateStore_Set_Error(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.failSet = true
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Set("foo", "bar")
	if err == nil {
		t.Error("expected error from Set")
	}
}

func TestRedisStateStore_Get_NotFound(t *testing.T) {
	mock := newMockRedisUniversalClient()
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	_, err := store.Get("notfound")
	if err == nil {
		t.Error("expected error for missing key")
	}
}

func TestRedisStateStore_Get_Error(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.failGet = true
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	_, err := store.Get("foo")
	if err == nil {
		t.Error("expected error from Get")
	}
}

func TestRedisStateStore_Incr(t *testing.T) {
	mock := newMockRedisUniversalClient()
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	val, err := store.Incr("counter", 2)
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if val != 2 {
		t.Errorf("expected 2, got %d", val)
	}
	val, err = store.Incr("counter", 3)
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if val != 5 {
		t.Errorf("expected 5, got %d", val)
	}
}

func TestRedisStateStore_Incr_Error(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.failIncr = true
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	_, err := store.Incr("counter", 1)
	if err == nil {
		t.Error("expected error from Incr")
	}
}

func TestRedisStateStore_Update_Success(t *testing.T) {
	mock := newMockRedisUniversalClient()
	key := "core.flow.req.foo"
	mock.store[key] = "old"
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Update("foo", "old", "new")
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if mock.store[key] != "new" {
		t.Errorf("expected new value, got %s", mock.store[key])
	}
}

func TestRedisStateStore_Update_NotExist(t *testing.T) {
	mock := newMockRedisUniversalClient()
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Update("foo", "old", "new")
	if err == nil || err.Error() != "[core.flow.req.foo] not exist" {
		t.Errorf("expected not exist error, got %v", err)
	}
}

func TestRedisStateStore_Update_OldValueMismatch(t *testing.T) {
	mock := newMockRedisUniversalClient()
	key := "core.flow.req.foo"
	mock.store[key] = "something"
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Update("foo", "old", "new")
	if err == nil || err.Error() != "Old value doesn't match for key core.flow.req.foo" {
		t.Errorf("expected old value mismatch error, got %v", err)
	}
}

func TestRedisStateStore_Update_GetError(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.failGet = true
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Update("foo", "old", "new")
	if err == nil || err.Error() != "unexpect error get fail" {
		t.Errorf("expected get fail error, got %v", err)
	}
}

func TestRedisStateStore_Cleanup(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.store["core.flow.req.a"] = "1"
	mock.store["core.flow.req.b"] = "2"
	store := &statestore.Statestore{KeyPath: "core.flow.req"}
	store.SetRedisClient(mock)
	err := store.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	if len(mock.store) != 0 {
		t.Errorf("expected store to be empty, got %v", mock.store)
	}
}

func TestRedisStateStore_Cleanup_DelError(t *testing.T) {
	mock := newMockRedisUniversalClient()
	mock.store["core.flow.req.a"] = "1"
	mock.failDel = true
	store := &statestore.RedisStateStore{KeyPath: "core.flow.req", rds: mock}
	err := store.Cleanup()
	if err == nil {
		t.Error("expected error from Del in Cleanup")
	}
}

func TestRedisStateStore_CopyStore(t *testing.T) {
	mock := newMockRedisUniversalClient()
	store := &statestore.Statestore{KeyPath: "core.flow.req", RetryCount: 3}
	store.SetRedisClient(mock)
	copiedIface, err := store.CopyStore()
	if err != nil {
		t.Fatalf("CopyStore failed: %v", err)
	}
	copied, ok := copiedIface.(*statestore.Statestore)
	if !ok {
		t.Fatalf("CopyStore did not return *Statestore")
	}
	if copied.KeyPath != store.KeyPath || copied.RetryCount != store.RetryCount || copied.GetRedisClient() != store.GetRedisClient() {
		t.Error("CopyStore did not copy fields correctly")
	}
}
