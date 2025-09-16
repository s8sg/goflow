package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetExecutionOption(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{gateway: "gw", reqID: "id123"}

	// Act
	opt := fe.GetExecutionOption(nil)

	// Assert
	assert.Equal(t, "gw", opt["gateway"])
	assert.Equal(t, "id123", opt["request-id"])
}

func TestConfigureAndGetFlowName(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{flowName: "flowA"}

	// Act
	fe.Configure("reqX")

	// Assert
	assert.Equal(t, "reqX", fe.reqID)
	assert.Equal(t, "flowA", fe.GetFlowName())
}

func TestReqValidationEnabled(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act
	result := fe.ReqValidationEnabled()

	// Assert
	assert.False(t, result)
}

func TestReqAuthEnabled(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{RequestAuthEnabled: true}

	// Act & Assert
	assert.True(t, fe.ReqAuthEnabled())
	fe.RequestAuthEnabled = false
	assert.False(t, fe.ReqAuthEnabled())
}

func TestGetValidationKey(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act
	key, err := fe.GetValidationKey()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "", key) // Default empty value
}

func TestGetReqAuthKey(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{RequestAuthSharedSecret: "test-auth-key"}

	// Act
	key, err := fe.GetReqAuthKey()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "test-auth-key", key)
}

func TestMonitoringEnabled(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{EnableMonitoring: true}

	// Act & Assert
	assert.True(t, fe.MonitoringEnabled())
	fe.EnableMonitoring = false
	assert.False(t, fe.MonitoringEnabled())
}

func TestLoggingEnabled(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{IsLoggingEnabled: true}

	// Act & Assert
	assert.True(t, fe.LoggingEnabled())
	fe.IsLoggingEnabled = false
	assert.False(t, fe.LoggingEnabled())
}

func TestGetStateStore_Nil(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act
	stateStore, err := fe.GetStateStore()

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, stateStore)
}

func TestGetDataStore_Nil(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act
	dataStore, err := fe.GetDataStore()

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, dataStore)
}

func TestGetEventHandler_Nil_Panics(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act & Assert - This should panic because EventHandler is nil
	assert.Panics(t, func() {
		_, _ = fe.GetEventHandler()
	})
}

func TestGetLogger_Nil(t *testing.T) {
	// Arrange
	fe := &FlowExecutor{}

	// Act
	logger, err := fe.GetLogger()

	// Assert
	assert.NoError(t, err)
	assert.Nil(t, logger)
}
