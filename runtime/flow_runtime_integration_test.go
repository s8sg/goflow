//go:build integration
// +build integration

package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFlowRuntime_Integration_Redis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Start Redis container
	req := testcontainers.ContainerRequest{
		Image:        "redis:7.2",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor: wait.ForLog("Ready to accept connections").
			WithOccurrence(1).
			WithStartupTimeout(30 * time.Second),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(redisContainer); err != nil {
			t.Logf("error terminating container: %s", err)
		}
	}()

	// Get connection string
	redisHost, err := redisContainer.Host(ctx)
	require.NoError(t, err)
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	redisURI := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	// Test StateStore initialization
	t.Run("StateStore_Integration", func(t *testing.T) {
		stateStore, err := initStateStore(redisURI, "")
		assert.NoError(t, err)
		assert.NotNil(t, stateStore)

		// Test basic operations
		stateStore.Configure("test-flow", "test-request")
		err = stateStore.Init()
		assert.NoError(t, err)

		// Test Set/Get
		err = stateStore.Set("test-key", "test-value")
		assert.NoError(t, err)

		value, err := stateStore.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, "test-value", value)

		// Test Incr
		result, err := stateStore.Incr("counter", 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result)

		// Test Update
		err = stateStore.Update("test-key", "test-value", "updated-value")
		assert.NoError(t, err)

		// Test Cleanup
		err = stateStore.Cleanup()
		assert.NoError(t, err)
	})

	// Test DataStore initialization
	t.Run("DataStore_Integration", func(t *testing.T) {
		dataStore, err := initDataStore(redisURI, "")
		assert.NoError(t, err)
		assert.NotNil(t, dataStore)

		// Test basic operations
		dataStore.Configure("test-flow", "test-request")
		err = dataStore.Init()
		assert.NoError(t, err)

		// Test Set/Get
		err = dataStore.Set("test-key", []byte("test-data"))
		assert.NoError(t, err)

		data, err := dataStore.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, []byte("test-data"), data)

		// Test Cleanup
		err = dataStore.Cleanup()
		assert.NoError(t, err)
	})

	// Test FlowRuntime basic initialization
	t.Run("FlowRuntime_Init", func(t *testing.T) {
		runtime := &FlowRuntime{
			RedisURL:      redisURI,
			RedisPassword: "",
			Concurrency:   1,
			DebugEnabled:  true,
		}

		err := runtime.Init()
		assert.NoError(t, err)
	})
}
