package datastore

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/s8sg/goflow/core/sdk"
)

type Datastore struct {
	bucketName  string
	redisClient RedisClient
}

// RedisClient interface for testability
type RedisClient interface {
	Set(key string, value interface{}, expiration time.Duration) StatusCmd
	Get(key string) StringCmd
	Del(keys ...string) IntCmd
	Scan(cursor uint64, match string, count int64) ScanCmd
	Ping() StatusCmd
}

// Wrappers for redis command types
type StatusCmd interface {
	Err() error
	Result() (string, error)
}
type StringCmd interface {
	Result() (string, error)
}
type IntCmd interface {
	Result() (int64, error)
	Err() error
}
type ScanCmd interface {
	Iterator() ScanIterator
	Err() error
}
type ScanIterator interface {
	Next() bool
	Val() string
	Err() error
}

// realRedisClient wraps *redis.Client to implement RedisClient
type realRedisClient struct{ *redis.Client }

func (c *realRedisClient) Set(key string, value interface{}, expiration time.Duration) StatusCmd {
	return c.Client.Set(key, value, expiration)
}
func (c *realRedisClient) Get(key string) StringCmd {
	return c.Client.Get(key)
}
func (c *realRedisClient) Del(keys ...string) IntCmd {
	return c.Client.Del(keys...)
}
func (c *realRedisClient) Scan(cursor uint64, match string, count int64) ScanCmd {
	return &realScanCmd{c.Client.Scan(cursor, match, count)}
}
func (c *realRedisClient) Ping() StatusCmd {
	return c.Client.Ping()
}

type realScanCmd struct{ cmd *redis.ScanCmd }

func (c *realScanCmd) Iterator() ScanIterator { return &realScanIterator{c.cmd.Iterator()} }
func (c *realScanCmd) Err() error             { return c.cmd.Err() }

type realScanIterator struct{ it *redis.ScanIterator }

func (it *realScanIterator) Next() bool  { return it.it.Next() }
func (it *realScanIterator) Val() string { return it.it.Val() }
func (it *realScanIterator) Err() error  { return it.it.Err() }

func GetDatastore(redisUri string, password string) (sdk.DataStore, error) {
	ds := &Datastore{}
	client := redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: password,
	})
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}
	ds.redisClient = &realRedisClient{client}
	return ds, nil
}

func (ds *Datastore) Configure(flowName string, requestId string) {
	bucketName := fmt.Sprintf("core-%s-%s", flowName, requestId)

	ds.bucketName = bucketName
}

func (ds *Datastore) Init() error {
	if ds.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetDatastore()")
	}

	return nil
}

func (ds *Datastore) Set(key string, value []byte) error {
	if ds.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetDatastore()")
	}

	fullPath := getPath(ds.bucketName, key)
	_, err := ds.redisClient.Set(fullPath, string(value), 0).Result()
	if err != nil {
		return fmt.Errorf("error writing: %s, error: %s", fullPath, err.Error())
	}

	return nil
}

func (ds *Datastore) Get(key string) ([]byte, error) {
	if ds.redisClient == nil {
		return nil, fmt.Errorf("redis client not initialized, use GetDatastore()")
	}

	fullPath := getPath(ds.bucketName, key)
	v := ds.redisClient.Get(fullPath)
	if v == nil {
		return nil, fmt.Errorf("error reading: %v, data is nil", fullPath)
	}
	value, err := v.Result()
	if err != nil {
		return nil, fmt.Errorf("error reading: %s, error: %s", fullPath, err.Error())
	}
	return []byte(value), nil
}

func (ds *Datastore) Del(key string) error {
	if ds.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetDatastore()")
	}

	fullPath := getPath(ds.bucketName, key)
	_, err := ds.redisClient.Del(fullPath).Result()
	if err != nil {
		return fmt.Errorf("error removing: %s, error: %s", fullPath, err.Error())
	}
	return nil
}

func (ds *Datastore) Cleanup() error {
	key := ds.bucketName + ".*"
	client := ds.redisClient
	var rerr error

	iter := client.Scan(0, key, 0).Iterator()
	for iter.Next() {
		err := client.Del(iter.Val()).Err()
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

func (ds *Datastore) CopyStore() (sdk.DataStore, error) {
	return &Datastore{bucketName: ds.bucketName, redisClient: ds.redisClient}, nil
}
