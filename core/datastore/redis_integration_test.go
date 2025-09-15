package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegration_Datastore_SetGetDel(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(10 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer container.Terminate(ctx)

	endpoint, err := container.Endpoint(ctx, "tcp")
	if err != nil {
		t.Fatalf("failed to get endpoint: %v", err)
	}

	ds, err := GetDatastore(endpoint, "")
	if err != nil {
		t.Fatalf("GetDatastore failed: %v", err)
	}
	ds.Configure("testflow", "testreq")
	if err := ds.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if err := ds.Set("foo", []byte("bar")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	val, err := ds.Get("foo")
	if err != nil || string(val) != "bar" {
		t.Fatalf("Get failed: %v, val: %s", err, val)
	}
	if err := ds.Del("foo"); err != nil {
		t.Fatalf("Del failed: %v", err)
	}
}
