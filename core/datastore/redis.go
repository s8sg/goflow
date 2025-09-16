package datastore

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/s8sg/goflow/core/sdk"
)

type ClientFactory func(redisUri, password string) DataStorageClient

type redisStorageClient struct {
	client *redis.Client
}

// redisScanIterator implements ScanIterator for Redis
type redisScanIterator struct {
	iter *redis.ScanIterator
}

func (r *redisScanIterator) Next(ctx context.Context) bool {
	return r.iter.Next(ctx)
}

func (r *redisScanIterator) Val() string {
	return r.iter.Val()
}

func (r *redisScanIterator) Err() error {
	return r.iter.Err()
}

// Ping tests the connection to Redis
func (ds *redisStorageClient) Ping(ctx context.Context) error {
	return ds.client.Ping(ctx).Err()
}

// Scan searches for keys matching a pattern
func (ds *redisStorageClient) Scan(ctx context.Context, cursor uint64, match string, count int64) ScanIterator {
	iter := ds.client.Scan(ctx, cursor, match, count).Iterator()
	return &redisScanIterator{iter: iter}
}

// Set stores a value with the given key
func (ds *redisStorageClient) Set(ctx context.Context, key string, value interface{}, expirationSeconds int) error {
	return ds.client.Set(ctx, key, value, 0).Err()
}

// Get retrieves a value by key
func (ds *redisStorageClient) Get(ctx context.Context, key string) (string, error) {
	return ds.client.Get(ctx, key).Result()
}

// Delete removes one or more keys
func (ds *redisStorageClient) Delete(ctx context.Context, keys ...string) (int64, error) {
	return ds.client.Del(ctx, keys...).Result()
}

var defaultStorageClientFactory ClientFactory = func(redisUri, password string) DataStorageClient {
	return &redisStorageClient{client: redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: password,
	})}
}

func GetDatastore(redisUri string, password string, storageFactory ...ClientFactory) (*DataStore, error) {
	ds := &DataStore{}
	var clientStorageFactory ClientFactory

	if len(storageFactory) > 0 && storageFactory[0] != nil {
		clientStorageFactory = storageFactory[0]
	} else {
		clientStorageFactory = defaultStorageClientFactory
	}
	client := clientStorageFactory(redisUri, password)
	ctx := context.Background()
	err := client.Ping(ctx)
	if err != nil {
		return nil, err
	}
	ds.client = client
	return ds, nil
}

func (ds *DataStore) Configure(flowName string, requestId string) {
	bucketName := fmt.Sprintf("core-%s-%s", flowName, requestId)
	ds.bucketName = bucketName
}

func (ds *DataStore) Init() error {
	if ds.client == nil {
		return fmt.Errorf("storage client not initialized, use GetDatastore()")
	}
	return nil
}

func (ds *DataStore) Set(key string, value []byte) error {
	if ds.client == nil {
		return fmt.Errorf("storage client not initialized, use GetDatastore()")
	}
	fullPath := getPath(ds.bucketName, key)
	ctx := context.Background()
	err := ds.client.Set(ctx, fullPath, string(value), 0)
	if err != nil {
		return fmt.Errorf("error writing: %s, error: %s", fullPath, err.Error())
	}
	return nil
}
func (ds *DataStore) Get(key string) ([]byte, error) {
	if ds.client == nil {
		return nil, fmt.Errorf("storage client not initialized, use GetDatastore()")
	}

	fullPath := getPath(ds.bucketName, key)
	ctx := context.Background()
	value, err := ds.client.Get(ctx, fullPath)
	if err == redis.Nil {
		return nil, fmt.Errorf("error reading: %v, data is nil", fullPath)
	}
	if err != nil {
		return nil, fmt.Errorf("error reading: %s, error: %s", fullPath, err.Error())
	}
	return []byte(value), nil
}
func (ds *DataStore) Del(key string) error {
	if ds.client == nil {
		return fmt.Errorf("redis client not initialized, use GetDatastore()")
	}

	fullPath := getPath(ds.bucketName, key)
	ctx := context.Background()
	_, err := ds.client.Delete(ctx, fullPath)
	if err != nil {
		return fmt.Errorf("error removing: %s, error: %s", fullPath, err.Error())
	}
	return nil
}
func (ds *DataStore) Cleanup() error {
	key := ds.bucketName + ".*"
	if ds.client == nil {
		return fmt.Errorf("redis client not initialized, use GetDatastore()")
	}
	var rerr error
	ctx := context.Background()

	iter := ds.client.Scan(ctx, 0, key, 0)
	for iter.Next(ctx) {
		_, err := ds.client.Delete(ctx, iter.Val())
		if err != nil {
			rerr = err
		}
	}

	if err := iter.Err(); err != nil {
		rerr = err
	}
	return rerr
}

// getPath produces a string as bucketname.value
func getPath(bucket, key string) string {
	fileName := fmt.Sprintf("%s.value", key)
	return fmt.Sprintf("%s.%s", bucket, fileName)
}

func (ds *DataStore) CopyStore() (sdk.DataStore, error) {
	return &DataStore{bucketName: ds.bucketName, client: ds.client}, nil
}
