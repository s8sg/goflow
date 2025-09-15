package controller_test

import (
	"testing"

	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/core/sdk/executor"
)

type mockExecutor struct{}

func (m *mockExecutor) HandleNextNode(state *executor.PartialState) error { return nil }

func (m *mockExecutor) GetExecutionOption(operation sdk.Operation) map[string]interface{} {
	return map[string]interface{}{}
}
func (m *mockExecutor) HandleExecutionCompletion(data []byte) error { return nil }

func (m *mockExecutor) Configure(requestId string)                              {}
func (m *mockExecutor) GetFlowName() string                                     { return "mock" }
func (m *mockExecutor) GetFlowDefinition(p *sdk.Pipeline, c *sdk.Context) error { return nil }
func (m *mockExecutor) ReqValidationEnabled() bool                              { return false }
func (m *mockExecutor) GetValidationKey() (string, error)                       { return "", nil }
func (m *mockExecutor) ReqAuthEnabled() bool                                    { return false }
func (m *mockExecutor) GetReqAuthKey() (string, error)                          { return "", nil }
func (m *mockExecutor) MonitoringEnabled() bool                                 { return false }
func (m *mockExecutor) GetEventHandler() (sdk.EventHandler, error)              { return nil, nil }
func (m *mockExecutor) LoggingEnabled() bool                                    { return false }
func (m *mockExecutor) GetLogger() (sdk.Logger, error)                          { return nil, nil }
func (m *mockExecutor) GetStateStore() (sdk.StateStore, error)                  { return nil, nil }
func (m *mockExecutor) GetDataStore() (sdk.DataStore, error)                    { return nil, nil }

func TestStopFlowHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}

func TestFlowStateHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}

func TestExecuteFlowHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}

func TestResumeFlowHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}

func TestPartialExecuteFlowHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}

func TestPauseFlowHandler(t *testing.T) {
	t.Skip("TODO: Improve mockExecutor to avoid nil dereference panic")
}
