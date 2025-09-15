package runtime

import (
	"testing"

	"github.com/s8sg/goflow/core/runtime"
	"github.com/stretchr/testify/assert"
)

func TestResponse_SetHeader(t *testing.T) {
	resp := &runtime.Response{Header: make(map[string][]string)}
	resp.SetHeader("X-Test", "value")
	// No panic = pass (add more checks if SetHeader has observable effect)
	value := resp.Header["X-Test"]
	assert.Equal(t, []string{"value"}, value)
}

func TestRequest_GetHeader(t *testing.T) {
	req := &runtime.Request{Header: map[string][]string{"X-Test": {"value"}}}
	if got := req.GetHeader("X-Test"); got != "value" {
		t.Errorf("GetHeader() = %v, want %v", got, "value")
	}
}
