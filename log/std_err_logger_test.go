package log

import (
	"testing"
)

func TestStdErrLogger_Configure(t *testing.T) {
	logger := &StdErrLogger{}
	logger.Configure("flow", "req")
	// No panic = pass
}

func TestStdErrLogger_Init(t *testing.T) {
	logger := &StdErrLogger{}
	if err := logger.Init(); err != nil {
		t.Errorf("Init() error = %v, want nil", err)
	}
}

func TestStdErrLogger_Log(t *testing.T) {
	logger := &StdErrLogger{}
	logger.Log("test log message")
	// No panic = pass
}
