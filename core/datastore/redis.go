package datastore

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type ClientFactory func(redisUri, password string) StorageClient

type redisStorageClient struct {
	client *redis.Client
}

func (ds *redisStorageClient) Ping(ctx context.Context) *redis.StatusCmd {
	return ds.client.Ping(ctx)
}
func (ds *redisStorageClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return ds.client.Scan(ctx, cursor, match, count)
}
func (ds *redisStorageClient) Set(ctx context.Context, key string, value interface{}, expirationSeconds int) *redis.StatusCmd {
	return ds.client.Set(ctx, key, value, 0)
}
func (ds *redisStorageClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return ds.client.Get(ctx, key)
}
func (ds *redisStorageClient) Delete(ctx context.Context, keys ...string) *redis.IntCmd {
	return ds.client.Del(ctx, keys...)
}

var defaultStorageClientFactory ClientFactory = func(redisUri, password string) StorageClient {
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
	err := client.Ping(ctx).Err()
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
		return fmt.Errorf("Storage Client not initialized, use GetDatastore()")
	}
	return nil
}

func (ds *DataStore) Set(key string, value []byte) error {
	if ds.client == nil {
		return fmt.Errorf("storage client not initialized, use GetDatastore()")
	}
	fullPath := getPath(ds.bucketName, key)
	ctx := context.Background()
	_, err := ds.client.Set(ctx, fullPath, string(value), 0).Result()
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
	value, err := ds.client.Get(ctx, fullPath).Result()
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
	_, err := ds.client.Delete(ctx, fullPath).Result()
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

	iter := ds.client.Scan(ctx, 0, key, 0).Iterator()
	for iter.Next(ctx) {
		err := ds.client.Delete(ctx, iter.Val()).Err()
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

func (ds *DataStore) CopyStore() (*DataStore, error) {
	return &DataStore{bucketName: ds.bucketName, client: ds.client}, nil
}
