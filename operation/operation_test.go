package operation

import (
	"errors"
	"reflect"
	"testing"
)

func TestGoFlowOperation_addOptions(t *testing.T) {
	op := &GoFlowOperation{Options: make(map[string][]string)}
	op.addOptions("k", "v1")
	op.addOptions("k", "v2")
	if !reflect.DeepEqual(op.Options["k"], []string{"v1", "v2"}) {
		t.Errorf("expected [v1 v2], got %v", op.Options["k"])
	}
}

func TestGoFlowOperation_AddFailureHandler(t *testing.T) {
	op := &GoFlowOperation{}
	h := func(err error) error { return nil }
	op.AddFailureHandler(h)
	if op.FailureHandler == nil {
		t.Error("FailureHandler not set")
	}
}

func TestGoFlowOperation_GetOptions(t *testing.T) {
	op := &GoFlowOperation{Options: map[string][]string{"a": {"b"}}}
	if op.GetOptions()["a"][0] != "b" {
		t.Error("GetOptions failed")
	}
}

func TestGoFlowOperation_GetId(t *testing.T) {
	op := &GoFlowOperation{Id: "id1"}
	if op.GetId() != "id1" {
		t.Error("GetId failed")
	}
}

func TestGoFlowOperation_Encode(t *testing.T) {
	op := &GoFlowOperation{}
	if string(op.Encode()) != "" {
		t.Error("Encode should return empty string")
	}
}

func Test_executeWorkload(t *testing.T) {
	op := &GoFlowOperation{
		Options: map[string][]string{"k": {"v"}},
		Mod: func(data []byte, opts map[string][]string) ([]byte, error) {
			return append(data, []byte(opts["k"][0])...), nil
		},
	}
	res, err := executeWorkload(op, []byte("a"))
	if err != nil || string(res) != "av" {
		t.Errorf("unexpected result: %s, err: %v", res, err)
	}
}

func TestGoFlowOperation_Execute_success(t *testing.T) {
	op := &GoFlowOperation{
		Id:      "id",
		Options: map[string][]string{"k": {"v"}},
		Mod: func(data []byte, opts map[string][]string) ([]byte, error) {
			return append(data, []byte(opts["k"][0])...), nil
		},
	}
	res, err := op.Execute([]byte("a"), nil)
	if err != nil || string(res) != "av" {
		t.Errorf("unexpected result: %s, err: %v", res, err)
	}
}

func TestGoFlowOperation_Execute_failure(t *testing.T) {
	fail := errors.New("fail")
	handlerCalled := false
	op := &GoFlowOperation{
		Id:      "id",
		Options: map[string][]string{},
		Mod: func(data []byte, opts map[string][]string) ([]byte, error) {
			return nil, fail
		},
		FailureHandler: func(err error) error {
			handlerCalled = true
			return nil
		},
	}
	_, err := op.Execute([]byte("a"), nil)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if !handlerCalled {
		t.Error("FailureHandler not called")
	}
}

func TestGoFlowOperation_Execute_failure_final(t *testing.T) {
	fail := errors.New("fail")
	op := &GoFlowOperation{
		Id:      "id",
		Options: map[string][]string{},
		Mod: func(data []byte, opts map[string][]string) ([]byte, error) {
			return nil, fail
		},
		FailureHandler: func(err error) error {
			return err
		},
	}
	_, err := op.Execute([]byte("a"), nil)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestGoFlowOperation_GetProperties(t *testing.T) {
	op := &GoFlowOperation{}
	props := op.GetProperties()
	if props["isFunction"][0] != "false" || props["hasFailureHandler"][0] != "false" {
		t.Error("expected false flags")
	}
	op.Mod = func(data []byte, opts map[string][]string) ([]byte, error) { return nil, nil }
	props = op.GetProperties()
	if props["isFunction"][0] != "true" {
		t.Error("expected isFunction true")
	}
	op.FailureHandler = func(err error) error { return nil }
	props = op.GetProperties()
	if props["hasFailureHandler"][0] != "true" {
		t.Error("expected hasFailureHandler true")
	}
}
