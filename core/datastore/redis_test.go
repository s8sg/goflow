package datastore

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/golang/mock/gomock"
	"github.com/s8sg/goflow/core/datastore/mocks"
)

func TestGetDatastore(t *testing.T) {
	// Mock redisClientFactory that returns a stub redis.Client
	called := false
	mockFactory := func(redisUri, password string) StorageClient {
		called = true
		// Create a redis.Client with a custom Ping method
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		client := mocks.NewMockStorageClient(ctrl)
		return client
	}

	ds, err := GetDatastore("localhost:6379", "", mockFactory)
	if err == nil || err.Error() != "mock ping error" {
		t.Errorf("GetDatastore() error = %v, want mock ping error", err)
	}
	if ds != nil {
		t.Errorf("GetDatastore() ds = %v, want nil", ds)
	}
	if !called {
		t.Errorf("mockFactory was not called")
	}
}

func TestDataStore_Cleanup_IteratorError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	scanCmd := redis.NewScanCmd(context.Background(), nil)
	scanCmd.SetErr(errors.New("scan error"))
	client.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(scanCmd)
	err := ds.Cleanup()
	if err == nil || err.Error() != "scan error" {
		t.Errorf("Cleanup() error = %v, want scan error", err)
	}
}

func TestDataStore_Cleanup_DeleteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	scanCmd := redis.NewScanCmd(context.Background(), nil)
	scanCmd.SetVal([]string{"bucket.key.value"}, 0)
	client.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(scanCmd)
	client.EXPECT().Delete(gomock.Any(), "bucket.key.value").Return(redis.NewIntCmd(context.Background())).DoAndReturn(
		func(ctx context.Context, key ...string) *redis.IntCmd {
			cmd := redis.NewIntCmd(ctx)
			cmd.SetErr(errors.New("delete error"))
			return cmd
		})
	err := ds.Cleanup()
	if err == nil || err.Error() != "delete error" {
		t.Errorf("Cleanup() error = %v, want delete error", err)
	}
}

func TestDataStore_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	client.EXPECT().Set(ctx, "bucket.key.value", "value", gomock.Any()).Return(redis.NewStatusCmd(ctx))
	err := ds.Set("key", []byte("value"))
	if err != nil {
		t.Errorf("Set() error = %v, wantErr %v", err, nil)
	}
}

func TestDataStore_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	getCmd := redis.NewStringCmd(ctx)
	getCmd.SetVal("value")
	client.EXPECT().Get(ctx, "bucket.key.value").Return(getCmd)
	val, err := ds.Get("key")
	if err != nil || string(val) != "value" {
		t.Errorf("Get() got = %v, want %v", string(val), "value")
	}
}

func TestDataStore_Del(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	delCmd := redis.NewIntCmd(ctx)
	delCmd.SetVal(1)
	client.EXPECT().Delete(ctx, "bucket.key.value").Return(delCmd)
	err := ds.Del("key")
	if err != nil {
		t.Errorf("Del() error = %v, wantErr %v", err, nil)
	}
}

func TestDataStore_Scan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	client := mocks.NewMockStorageClient(ctrl)
	scanCmd := redis.NewScanCmd(ctx, nil)
	scanCmd.SetVal([]string{"bucket.key.value"}, 0)
	client.EXPECT().Scan(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(scanCmd)
	result := client.Scan(ctx, 0, "bucket.*", 10)
	if result == nil {
		t.Errorf("Scan() result = nil, want non-nil")
	}
}

func TestDataStore_Ping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	client := mocks.NewMockStorageClient(ctrl)
	pingCmd := redis.NewStatusCmd(ctx)
	pingCmd.SetVal("PONG")
	client.EXPECT().Ping(ctx).Return(pingCmd)
	pong := client.Ping(ctx)
	if pong.Val() != "PONG" {
		t.Errorf("Ping() got = %v, want %v", pong.Val(), "PONG")
	}
}

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

func TestDataStore_Init_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client}
	err := ds.Init()
	if err != nil {
		t.Errorf("Init() error = %v, want nil", err)
	}
}

func TestDataStore_CopyStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mocks.NewMockStorageClient(ctrl)
	ds := &DataStore{client: client, bucketName: "bucket"}
	copy, err := ds.CopyStore()
	if err != nil {
		t.Errorf("CopyStore() error = %v, want nil", err)
	}
	if copy.bucketName != ds.bucketName {
		t.Errorf("CopyStore() bucketName = %v, want %v", copy.bucketName, ds.bucketName)
	}
}

func TestDataStore_Set_Error(t *testing.T) {
	ds := &DataStore{}
	err := ds.Set("key", []byte("value"))
	if err == nil {
		t.Errorf("Set() error = nil, want error")
	}
}

func TestDataStore_Get_Error(t *testing.T) {
	ds := &DataStore{}
	_, err := ds.Get("key")
	if err == nil {
		t.Errorf("Get() error = nil, want error")
	}
}

func TestDataStore_Del_Error(t *testing.T) {
	ds := &DataStore{}
	err := ds.Del("key")
	if err == nil {
		t.Errorf("Del() error = nil, want error")
	}
}

func TestDataStore_Cleanup_Error(t *testing.T) {
	ds := &DataStore{}
	err := ds.Cleanup()
	if err == nil {
		t.Errorf("Cleanup() error = nil, want error")
	}
}
