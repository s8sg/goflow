package common

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandleError(t *testing.T) {
	// Create a test ResponseWriter
	w := httptest.NewRecorder()

	// Test HandleError function
	HandleError(w, "test error message")

	// Check status code
	assert.Equal(t, 500, w.Code)

	// Check response body contains error message
	body := w.Body.String()
	assert.Contains(t, body, "test error message")
	assert.Contains(t, body, "[ Failed ]")
}

func TestHandleError_EmptyMessage(t *testing.T) {
	// Create a test ResponseWriter
	w := httptest.NewRecorder()

	// Test HandleError with empty message
	HandleError(w, "")

	// Check status code
	assert.Equal(t, 500, w.Code)

	// Check response body contains failed marker
	body := w.Body.String()
	assert.Contains(t, body, "[ Failed ]")
}

func TestHandleError_MessageWithNewlines(t *testing.T) {
	// Create a test ResponseWriter
	w := httptest.NewRecorder()

	// Test HandleError with multiline message
	testMessage := "line1\nline2\nline3"
	HandleError(w, testMessage)

	// Check status code
	assert.Equal(t, 500, w.Code)

	// Check response body
	body := w.Body.String()
	assert.Contains(t, body, testMessage)
	assert.True(t, strings.HasSuffix(strings.TrimSpace(body), testMessage))
}
