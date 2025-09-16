package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResponse_SetHeader(t *testing.T) {
	// Arrange
	resp := &Response{Header: make(map[string][]string)}

	// Act
	resp.SetHeader("X-Test", "value")
	value := resp.Header["X-Test"]

	// Assert
	assert.Equal(t, []string{"value"}, value)
}

func TestRequest_GetHeader(t *testing.T) {
	// Arrange
	req := &Request{Header: map[string][]string{"X-Test": {"value"}}}

	// Act
	got := req.GetHeader("X-Test")

	// Assert
	if got != "value" {
		t.Errorf("GetHeader() = %v, want %v", got, "value")
	}
}
