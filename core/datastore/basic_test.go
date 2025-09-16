package datastore

import (
	"context"
	"testing"
)

func TestDataStore_Configure(t *testing.T) {
	ds := &DataStore{}
	ds.Configure("flow", "req")
	want := "core-flow-req"
	if ds.bucketName != want {
		t.Errorf("Configure() got = %v, want %v", ds.bucketName, want)
	}
}

func TestDataStore_Init_Error(t *testing.T) {
	ds := &DataStore{}
	err := ds.Init()
	if err == nil {
		t.Errorf("Init() error = nil, want error")
	}
}

func TestDataStore_ErrorCases(t *testing.T) {
	ds := &DataStore{}

	err := ds.Set("key", []byte("value"))
	if err == nil {
		t.Errorf("Set() error = nil, want error")
	}

	_, err = ds.Get("key")
	if err == nil {
		t.Errorf("Get() error = nil, want error")
	}

	err = ds.Del("key")
	if err == nil {
		t.Errorf("Del() error = nil, want error")
	}

	err = ds.Cleanup()
	if err == nil {
		t.Errorf("Cleanup() error = nil, want error")
	}
}

func TestGetPath(t *testing.T) {
	tests := []struct {
		bucket string
		key    string
		want   string
	}{
		{"bucket", "key", "bucket.key.value"},
		{"", "key", ".key.value"},
		{"bucket", "", "bucket..value"},
	}

	for _, tt := range tests {
		got := getPath(tt.bucket, tt.key)
		if got != tt.want {
			t.Errorf("getPath(%v, %v) = %v, want %v", tt.bucket, tt.key, got, tt.want)
		}
	}
}

// Test that interfaces are properly defined
func TestInterfaceDefinition(t *testing.T) {
	// Test that DataStorageClient interface has all required methods
	var client DataStorageClient
	if client != nil {
		ctx := context.Background()
		_ = client.Ping(ctx)
		_, _ = client.Get(ctx, "key")
		_ = client.Set(ctx, "key", "value", 0)
		_, _ = client.Delete(ctx, "key")
		_ = client.Scan(ctx, 0, "pattern", 0)
	}

	// Test that ScanIterator interface has all required methods
	var iter ScanIterator
	if iter != nil {
		ctx := context.Background()
		_ = iter.Next(ctx)
		_ = iter.Val()
		_ = iter.Err()
	}
}
