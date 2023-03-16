package RedisDataStore

import (
	"fmt"

	"github.com/go-redis/redis"
	"github.com/s8sg/goflow/core/sdk"
)

type RedisDataStore struct {
	bucketName  string
	redisClient redis.UniversalClient
}

func GetRedisDataStore(redisUri string) (sdk.DataStore, error) {
	ds := &RedisDataStore{}
	client := redis.NewClient(&redis.Options{
		Addr: redisUri,
	})
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}

	ds.redisClient = client
	return ds, nil
}

func (this *RedisDataStore) Configure(flowName string, requestId string) {
	bucketName := fmt.Sprintf("core-%s-%s", flowName, requestId)

	this.bucketName = bucketName
}

func (this *RedisDataStore) Init() error {
	if this.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetRedisDataStore()")
	}

	return nil
}

func (this *RedisDataStore) Set(key string, value []byte) error {
	if this.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetRedisDataStore()")
	}

	fullPath := getPath(this.bucketName, key)
	_, err := this.redisClient.Set(fullPath, string(value), 0).Result()
	if err != nil {
		return fmt.Errorf("error writing: %s, error: %s", fullPath, err.Error())
	}

	return nil
}

func (this *RedisDataStore) Get(key string) ([]byte, error) {
	if this.redisClient == nil {
		return nil, fmt.Errorf("redis client not initialized, use GetRedisDataStore()")
	}

	fullPath := getPath(this.bucketName, key)
	value, err := this.redisClient.Get(fullPath).Result()
	if err != nil {
		return nil, fmt.Errorf("error reading: %s, error: %s", fullPath, err.Error())
	}
	return []byte(value), nil
}

func (this *RedisDataStore) Del(key string) error {
	if this.redisClient == nil {
		return fmt.Errorf("redis client not initialized, use GetRedisDataStore()")
	}

	fullPath := getPath(this.bucketName, key)
	_, err := this.redisClient.Del(fullPath).Result()
	if err != nil {
		return fmt.Errorf("error removing: %s, error: %s", fullPath, err.Error())
	}
	return nil
}

func (this *RedisDataStore) Cleanup() error {
	key := this.bucketName + ".*"
	client := this.redisClient
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

func (this *RedisDataStore) CopyStore() (sdk.DataStore, error) {
	return &RedisDataStore{bucketName: this.bucketName, redisClient: this.redisClient}, nil
}
