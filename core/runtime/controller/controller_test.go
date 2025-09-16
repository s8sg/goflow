package controller_test

import (
	"github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/runtime/controller"
	"testing"
)

func TestStopFlowHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req-stop", FlowName: "flowStop"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.StopFlowHandler(resp, req, mock)

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "Successfully stopped request req-stop"
	if resp.Body == nil || string(resp.Body) != expected {
		t.Errorf("expected body '%s', got '%s'", expected, string(resp.Body))
	}
}

func TestFlowStateHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req2", FlowName: "flowB"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.FlowStateHandler(resp, req, mock)

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	// FlowStateHandler should return the state from GetState method
	// The mock returns empty state, so we expect empty body
	if resp.Body == nil {
		t.Errorf("expected some response body, got nil")
	}
}

func TestExecuteFlowHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req-exec", FlowName: "flowExec"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.ExecuteFlowHandler(resp, req, mock)

	// Assert
	// ExecuteFlowHandler fails because mock doesn't define a proper flow
	if err == nil {
		t.Error("expected error for invalid dag, got nil")
	}
}

func TestExecuteFlowHandler_WithHeaders(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{
		RequestID: "req-exec-headers",
		FlowName:  "flowExecHeaders",
		Header: map[string][]string{
			"X-Callback-Url":  {"http://callback.url"},
			"X-Hub-Signature": {"signature123"},
		},
		Body:     []byte("test payload"),
		RawQuery: "param=value",
	}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.ExecuteFlowHandler(resp, req, mock)

	// Assert
	// Still expect error due to invalid flow, but test covers header handling
	if err == nil {
		t.Error("expected error for invalid dag, got nil")
	}
}

func TestResumeFlowHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req-resume", FlowName: "flowResume"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.ResumeFlowHandler(resp, req, mock)

	// Assert
	// Resume should fail since request was never started
	if err == nil {
		t.Error("expected error for resuming inactive request, got nil")
	}
}

func TestPartialExecuteFlowHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req-partial", FlowName: "flowPartial"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.PartialExecuteFlowHandler(resp, req, mock)

	// Assert
	// PartialExecute should fail without proper state
	if err == nil {
		t.Error("expected error for partial execution without state, got nil")
	}
}

func TestPauseFlowHandler(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "req-pause", FlowName: "flowPause"}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.PauseFlowHandler(resp, req, mock)

	// Assert
	// Pause should fail since request was never started
	if err == nil {
		t.Error("expected error for pausing inactive request, got nil")
	}
}

// Test ExecuteFlowHandler with empty RequestID to cover that code path
func TestExecuteFlowHandler_EmptyRequestID(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{RequestID: "", FlowName: "flowEmpty"} // Empty RequestID
	mock := &controller.MockExecutor{}

	// Act
	err := controller.ExecuteFlowHandler(resp, req, mock)

	// Assert
	// Still expect error due to invalid flow, but covers empty RequestID path
	if err == nil {
		t.Error("expected error for invalid dag, got nil")
	}
}

// Test PartialExecuteFlowHandler with actual body to cover more paths
func TestPartialExecuteFlowHandler_WithBody(t *testing.T) {
	// Arrange
	resp := &runtime.Response{}
	req := &runtime.Request{
		RequestID: "req-partial-body",
		FlowName:  "flowPartialBody",
		Body:      []byte("some encoded state"),
	}
	mock := &controller.MockExecutor{}

	// Act
	err := controller.PartialExecuteFlowHandler(resp, req, mock)

	// Assert
	// Should still fail without proper state encoding
	if err == nil {
		t.Error("expected error for partial execution without proper state, got nil")
	}
}
