package runtime

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/s8sg/goflow/core/sdk"
	mock_runtime "github.com/s8sg/goflow/runtime/mocks"
	"github.com/stretchr/testify/assert"
)

func TestFlowRuntime_Unit_FlowExecutorBasics(t *testing.T) {
	// Test FlowExecutor basic functionality without full initialization
	fe := &FlowExecutor{
		flowName: "test-flow",
		reqID:    "test-req",
		gateway:  "test-gateway",
	}

	// Test basic getters
	assert.Equal(t, "test-flow", fe.GetFlowName())

	// Test Configure
	fe.Configure("new-req")
	assert.Equal(t, "new-req", fe.reqID)

	// Test GetExecutionOption
	opts := fe.GetExecutionOption(nil)
	assert.Equal(t, "test-gateway", opts["gateway"])
	assert.Equal(t, "new-req", opts["request-id"])
}

func TestFlowRuntime_Unit_StateStoreFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFactory := mock_runtime.NewMockStateStoreFactory(ctrl)
	mockStateStore := &MockStateStore{}

	// Setup expectations
	mockFactory.EXPECT().
		CreateStateStore("localhost:6379", "").
		Return(mockStateStore, nil)

	// Test factory usage
	stateStore, err := mockFactory.CreateStateStore("localhost:6379", "")
	assert.NoError(t, err)
	assert.Equal(t, mockStateStore, stateStore)
}

func TestFlowRuntime_Unit_DataStoreFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFactory := mock_runtime.NewMockDataStoreFactory(ctrl)
	mockDataStore := &MockDataStore{}

	// Setup expectations
	mockFactory.EXPECT().
		CreateDataStore("localhost:6379", "").
		Return(mockDataStore, nil)

	// Test factory usage
	dataStore, err := mockFactory.CreateDataStore("localhost:6379", "")
	assert.NoError(t, err)
	assert.Equal(t, mockDataStore, dataStore)
}

func TestFlowRuntime_Unit_GetNewId(t *testing.T) {
	// Test ID generation function
	id1 := getNewId()
	id2 := getNewId()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
	assert.Greater(t, len(id1), 5) // Should be reasonable length
}

func TestFlowRuntime_Unit_MarshalWorker(t *testing.T) {
	worker := &Worker{
		ID:          "worker-123",
		Flows:       []string{"flow1", "flow2"},
		Concurrency: 5,
	}

	jsonStr := marshalWorker(worker)

	assert.Contains(t, jsonStr, "worker-123")
	assert.Contains(t, jsonStr, "flow1")
	assert.Contains(t, jsonStr, "flow2")
	assert.Contains(t, jsonStr, "5") // Concurrency as string
}

func TestFlowRuntime_Unit_InternalRequestQueueId(t *testing.T) {
	runtime := &FlowRuntime{}

	queueId := runtime.internalRequestQueueId("test-flow")
	expected := "goflow-internal-request:test-flow"

	assert.Equal(t, expected, queueId)
}

func TestFlowRuntime_Unit_MakeRequestFromTask(t *testing.T) {
	task := Task{
		FlowName:    "test-flow",
		RequestID:   "req-123",
		Body:        "test-body",
		Header:      map[string][]string{"X-Test": {"value"}},
		RawQuery:    "key=value",
		Query:       map[string][]string{"key": {"value"}},
		RequestType: "NEW",
	}

	request := makeRequestFromTask(task)

	assert.Equal(t, "test-flow", request.FlowName)
	assert.Equal(t, "req-123", request.RequestID)
	assert.Equal(t, []byte("test-body"), request.Body)
	assert.Equal(t, "key=value", request.RawQuery)
	// AuthSignature is not part of the Task struct in this version
	assert.Equal(t, "value", request.Header["X-Test"][0])
	assert.Equal(t, "value", request.Query["key"][0])
}

// Mock implementations for testing
type MockStateStore struct {
	data map[string]string
}

func (m *MockStateStore) Configure(flowName, requestId string) {
	if m.data == nil {
		m.data = make(map[string]string)
	}
}

func (m *MockStateStore) Init() error { return nil }

func (m *MockStateStore) Set(key, value string) error {
	if m.data == nil {
		m.data = make(map[string]string)
	}
	m.data[key] = value
	return nil
}

func (m *MockStateStore) Get(key string) (string, error) {
	if m.data == nil {
		return "", nil
	}
	return m.data[key], nil
}

func (m *MockStateStore) Incr(key string, value int64) (int64, error) {
	return value, nil
}

func (m *MockStateStore) Update(key, oldValue, newValue string) error {
	if m.data == nil {
		m.data = make(map[string]string)
	}
	m.data[key] = newValue
	return nil
}

func (m *MockStateStore) Cleanup() error { return nil }

func (m *MockStateStore) CopyStore() (sdk.StateStore, error) {
	return &MockStateStore{data: make(map[string]string)}, nil
}

type MockDataStore struct {
	data map[string][]byte
}

func (m *MockDataStore) Configure(flowName, requestId string) {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
}

func (m *MockDataStore) Init() error { return nil }

func (m *MockDataStore) Set(key string, value []byte) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[key] = value
	return nil
}

func (m *MockDataStore) Get(key string) ([]byte, error) {
	if m.data == nil {
		return nil, nil
	}
	return m.data[key], nil
}

func (m *MockDataStore) Del(key string) error {
	if m.data != nil {
		delete(m.data, key)
	}
	return nil
}

func (m *MockDataStore) Cleanup() error { return nil }

func (m *MockDataStore) CopyStore() (sdk.DataStore, error) {
	return &MockDataStore{data: make(map[string][]byte)}, nil
}
