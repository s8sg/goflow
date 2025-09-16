package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNewId_Unique(t *testing.T) {
	// Arrange: nothing to arrange for ID generation

	// Act
	id1 := getNewId()
	id2 := getNewId()

	// Assert
	assert.NotEqual(t, id1, id2)
	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
}

func TestMarshalWorker_JSON(t *testing.T) {
	// Arrange
	w := &Worker{ID: "worker1", Flows: []string{"f1", "f2"}, Concurrency: 2}

	// Act
	jsonStr := marshalWorker(w)

	// Assert
	assert.Contains(t, jsonStr, "worker1")
	assert.Contains(t, jsonStr, "f1")
	assert.Contains(t, jsonStr, "f2")
}

func TestMakeRequestFromTask(t *testing.T) {
	// Arrange
	task := Task{
		FlowName:  "flowA",
		RequestID: "req123",
		Body:      "payload",
		Header:    map[string][]string{"X-Test": {"v"}},
		RawQuery:  "foo=bar",
		Query:     map[string][]string{"foo": {"bar"}},
	}

	// Act
	req := makeRequestFromTask(task)

	// Assert
	assert.Equal(t, "flowA", req.FlowName)
	assert.Equal(t, "req123", req.RequestID)
	assert.Equal(t, []byte("payload"), req.Body)
	assert.Equal(t, "bar", req.Query["foo"][0])
	assert.Equal(t, "foo=bar", req.RawQuery)
	assert.Equal(t, "v", req.Header["X-Test"][0])
}

func TestInternalRequestQueueId(t *testing.T) {
	// Arrange
	fRuntime := &FlowRuntime{}
	flowName := "test-flow"

	// Act
	queueId := fRuntime.internalRequestQueueId(flowName)

	// Assert
	expected := "goflow-internal-request:" + flowName
	assert.Equal(t, expected, queueId)
}

func TestFlowRuntimeBasicMethods(t *testing.T) {
	// Test basic FlowRuntime methods that don't require complex setup
	fRuntime := &FlowRuntime{
		RequestAuthEnabled: true,
		EnableMonitoring:   true,
		DebugEnabled:       true,
	}

	// Test that fields are set correctly
	assert.True(t, fRuntime.RequestAuthEnabled)
	assert.True(t, fRuntime.EnableMonitoring)
	assert.True(t, fRuntime.DebugEnabled)
}
