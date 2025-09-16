//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/s8sg/goflow/core/datastore"
	"github.com/s8sg/goflow/core/statestore"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestEndToEndFlowWithRedis(t *testing.T) {
	ctx := context.Background()

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.2",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	assert.NoError(t, err)
	defer func() {
		if err := redisC.Terminate(ctx); err != nil {
			t.Logf("Error terminating redis container: %v", err)
		}
	}()

	redisHost, err := redisC.Host(ctx)
	assert.NoError(t, err)
	redisPort, err := redisC.MappedPort(ctx, "6379")
	assert.NoError(t, err)
	redisAddr := redisHost + ":" + redisPort.Port()

	// Initialize DataStore and StateStore
	ds, err := datastore.GetDatastore(redisAddr, "")
	assert.NoError(t, err)
	ss, err := statestore.GetStateStore(redisAddr, "")
	assert.NoError(t, err)

	// Simulate a sample flow: set, get, del
	err = ds.Set("key", []byte("value"))
	assert.NoError(t, err)
	val, err := ds.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), val)
	err = ds.Del("key")
	assert.NoError(t, err)
	_, err = ds.Get("key")
	assert.Error(t, err)

	// StateStore: set, get, incr
	err = ss.Set("counter", "1")
	assert.NoError(t, err)
	v, err := ss.Get("counter")
	assert.NoError(t, err)
	assert.Equal(t, "1", v)
	newVal, err := ss.Incr("counter", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), newVal)
}
