package controller

import (
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/core/sdk/executor"
)

type MockExecutor struct {
	executionOptionFunc func(sdk.Operation) map[string]interface{}
	ExecuteFunc         func(executor.ExecutionStateOption) ([]byte, error)
	GetStateFunc        func(requestID string) (string, error)
	PauseFunc           func(requestID string) error
	ResumeFunc          func(requestID string) error
	StopFunc            func(requestID string) error
	ConfigureFunc       func(flowName string, requestId string)
	InitFunc            func() error
	flowName            string
	validationEnabled   bool
	validationKey       string
	authEnabled         bool
	authKey             string
	monitoringEnabled   bool
	eventHandler        sdk.EventHandler
	loggingEnabled      bool
	logger              sdk.Logger
	flowDefinitionFunc  func(*sdk.Pipeline, *sdk.Context) error
}

/*
type Executor interface {
	// Configure configure an executor with request id
	Configure(requestId string)
	// GetFlowName get name of the flow
	GetFlowName() string
	// GetFlowDefinition get definition of the faas-flow
	GetFlowDefinition(*sdk.Pipeline, *sdk.Context) error
	// ReqValidationEnabled check if request validation enabled
	ReqValidationEnabled() bool
	// GetValidationKey get request validation key
	GetValidationKey() (string, error)
	// ReqAuthEnabled check if request auth enabled
	ReqAuthEnabled() bool
	// GetReqAuthKey get the request auth key
	GetReqAuthKey() (string, error)
	// MonitoringEnabled check if request monitoring enabled
	MonitoringEnabled() bool
	// GetEventHandler get the event handler for request monitoring
	GetEventHandler() (sdk.EventHandler, error)
	// LoggingEnabled check if logging is enabled
	LoggingEnabled() bool
	// GetLogger get the logger
	GetLogger() (sdk.Logger, error)
	// GetStateStore get the state store
	GetStateStore() (sdk.StateStore, error)
	// GetDataStore get the data store
	GetDataStore() (sdk.DataStore, error)

	ExecutionRuntime
}


*/

// Add missing interface methods for executor.Executor
func (m *MockExecutor) GetDataStore() (sdk.DataStore, error) { return nil, nil }
func (m *MockExecutor) GetFlowName() string {
	if m.flowName != "" {
		return m.flowName
	}
	return "mock-flow"
}

func (m *MockExecutor) GetFlowDefinition(p *sdk.Pipeline, c *sdk.Context) error {
	if m.flowDefinitionFunc != nil {
		return m.flowDefinitionFunc(p, c)
	}
	return nil
}

func (m *MockExecutor) ReqValidationEnabled() bool {
	return m.validationEnabled
}

func (m *MockExecutor) GetValidationKey() (string, error) {
	if m.validationKey != "" {
		return m.validationKey, nil
	}
	return "mock-validation-key", nil
}

func (m *MockExecutor) ReqAuthEnabled() bool {
	return m.authEnabled
}

func (m *MockExecutor) GetReqAuthKey() (string, error) {
	if m.authKey != "" {
		return m.authKey, nil
	}
	return "mock-auth-key", nil
}

func (m *MockExecutor) MonitoringEnabled() bool {
	return m.monitoringEnabled
}

func (m *MockExecutor) GetEventHandler() (sdk.EventHandler, error) {
	if m.eventHandler != nil {
		return m.eventHandler, nil
	}
	return nil, nil
}

func (m *MockExecutor) LoggingEnabled() bool {
	return m.loggingEnabled
}

func (m *MockExecutor) GetLogger() (sdk.Logger, error) {
	if m.logger != nil {
		return m.logger, nil
	}
	return nil, nil
}

// GetExecutionOption provides execution options for an operation (mock implementation)
func (m *MockExecutor) GetExecutionOption(op sdk.Operation) map[string]interface{} {
	if m.executionOptionFunc != nil {
		return m.executionOptionFunc(op)
	}
	return map[string]interface{}{}
}

// HandleExecutionCompletion mock implementation
func (m *MockExecutor) HandleExecutionCompletion(data []byte) error {
	return nil
}

// Correct signature for Executor interface
func (m *MockExecutor) HandleNextNode(state *executor.PartialState) error {
	return nil
}
func (m *MockExecutor) GetStateStore() (sdk.StateStore, error) {
	return &MockStateStore{}, nil
}

// MockStateStore for testing
type MockStateStore struct{}

func (m *MockStateStore) Configure(flowName, requestId string)        {}
func (m *MockStateStore) Init() error                                 { return nil }
func (m *MockStateStore) Set(key, value string) error                 { return nil }
func (m *MockStateStore) Get(key string) (string, error)              { return "", nil }
func (m *MockStateStore) Incr(key string, value int64) (int64, error) { return value, nil }
func (m *MockStateStore) Update(key, oldValue, newValue string) error { return nil }
func (m *MockStateStore) Cleanup() error                              { return nil }
func (m *MockStateStore) CopyStore() (sdk.StateStore, error)          { return &MockStateStore{}, nil }

func (m *MockExecutor) Execute(opt executor.ExecutionStateOption) ([]byte, error) {
	if m.ExecuteFunc != nil {
		return m.ExecuteFunc(opt)
	}
	return []byte("mocked"), nil
}

func (m *MockExecutor) GetState(requestID string) (string, error) {
	if m.GetStateFunc != nil {
		return m.GetStateFunc(requestID)
	}
	return "mocked-state", nil
}

func (m *MockExecutor) Pause(requestID string) error {
	if m.PauseFunc != nil {
		return m.PauseFunc(requestID)
	}
	return nil
}

func (m *MockExecutor) Resume(requestID string) error {
	if m.ResumeFunc != nil {
		return m.ResumeFunc(requestID)
	}
	return nil
}

func (m *MockExecutor) Stop(requestID string) error {
	if m.StopFunc != nil {
		return m.StopFunc(requestID)
	}
	return nil
}

// Configure Configure with requestId and flowname
func (m *MockExecutor) Configure(flowName string) {

}

// Init initialize the storemanager (called only once in a request span)
func (m *MockExecutor) Init() error {
	return nil
}
