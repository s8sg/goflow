package statestore

import (
	"reflect"
	"testing"
)

type mockBackend struct {
	configured bool
	inited     bool
	setKey     string
	setValue   string
	getKey     string
	getValue   string
	incrKey    string
	incrValue  int64
	updateKey  string
	updateOld  string
	updateNew  string
	cleaned    bool
	copyCalled bool
}

func (m *mockBackend) Configure(flowName, requestId string) { m.configured = true }
func (m *mockBackend) Init() error                          { m.inited = true; return nil }
func (m *mockBackend) Set(key, value string) error          { m.setKey = key; m.setValue = value; return nil }
func (m *mockBackend) Get(key string) (string, error)       { m.getKey = key; return m.getValue, nil }
func (m *mockBackend) Incr(key string, value int64) (int64, error) {
	m.incrKey = key
	m.incrValue = value
	return value + 1, nil
}
func (m *mockBackend) Update(key, oldValue, newValue string) error {
	m.updateKey = key
	m.updateOld = oldValue
	m.updateNew = newValue
	return nil
}
func (m *mockBackend) Cleanup() error                   { m.cleaned = true; return nil }
func (m *mockBackend) CopyStore() (StateBackend, error) { m.copyCalled = true; return m, nil }

func TestStateStore_Configure(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	s.Configure("flow", "req")
	if !mb.configured {
		t.Error("Configure did not call backend")
	}
}

func TestStateStore_Init(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	err := s.Init()
	if err != nil || !mb.inited {
		t.Error("Init did not call backend or returned error")
	}
}

func TestStateStore_SetGet(t *testing.T) {
	mb := &mockBackend{getValue: "val"}
	s := &StateStore{backend: mb}
	err := s.Set("k", "val")
	if err != nil || mb.setKey != "k" || mb.setValue != "val" {
		t.Error("Set did not call backend or returned error")
	}
	v, err := s.Get("k")
	if err != nil || v != "val" || mb.getKey != "k" {
		t.Error("Get did not call backend or returned error")
	}
}

func TestStateStore_Incr(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	v, err := s.Incr("k", 1)
	if err != nil || v != 2 || mb.incrKey != "k" || mb.incrValue != 1 {
		t.Error("Incr did not call backend or returned error")
	}
}

func TestStateStore_Update(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	err := s.Update("k", "old", "new")
	if err != nil || mb.updateKey != "k" || mb.updateOld != "old" || mb.updateNew != "new" {
		t.Error("Update did not call backend or returned error")
	}
}

func TestStateStore_Cleanup(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	err := s.Cleanup()
	if err != nil || !mb.cleaned {
		t.Error("Cleanup did not call backend or returned error")
	}
}

func TestStateStore_CopyStore(t *testing.T) {
	mb := &mockBackend{}
	s := &StateStore{backend: mb}
	copy, err := s.CopyStore()
	if err != nil {
		t.Error("CopyStore returned error")
	}
	if reflect.TypeOf(copy) != reflect.TypeOf(s) {
		t.Error("CopyStore did not return StateStore type")
	}
	if !mb.copyCalled {
		t.Error("CopyStore did not call backend")
	}
}
