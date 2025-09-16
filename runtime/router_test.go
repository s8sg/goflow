package runtime

import (
	"net/http"
	"testing"

	v1 "github.com/s8sg/goflow/flow/v1"
	"github.com/stretchr/testify/assert"
)

func TestRouter(t *testing.T) {
	// Create a minimal FlowRuntime for testing
	fRuntime := &FlowRuntime{
		Flows: map[string]FlowDefinitionHandler{
			"test-flow": func(flow *v1.Workflow, context *v1.Context) error {
				return nil
			},
		},
	}

	// Test Router function
	handler := Router(fRuntime)

	// Check that handler is not nil
	assert.NotNil(t, handler)

	// Check that it implements http.Handler
	assert.Implements(t, (*http.Handler)(nil), handler)
}

func TestOpenConnectionV2_InvalidAddress(t *testing.T) {
	// Test OpenConnectionV2 with invalid parameters
	connection, err := OpenConnectionV2("test", "tcp", "invalid:address:format", "", 0, nil)

	// Should return an error for invalid address
	assert.Error(t, err)
	assert.Nil(t, connection)
}
