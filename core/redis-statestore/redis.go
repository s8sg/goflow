package RedisStateStore

import (
	"errors"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/s8sg/goflow/core/sdk"
)

type RedisStateStore struct {
	KeyPath    string
	rds        redis.UniversalClient
	RetryCount int
}

func GetRedisStateStore(redisUri string, password string) (sdk.StateStore, error) {
	stateStore := &RedisStateStore{}

	client := redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: password,
	})

	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}

	stateStore.rds = client
	return stateStore, nil
}

// Configure
func (this *RedisStateStore) Configure(flowName string, requestId string) {
	this.KeyPath = fmt.Sprintf("core.%s.%s", flowName, requestId)
}

// Init (Called only once in a request)
func (this *RedisStateStore) Init() error {
	return nil
}

// Update Compare and Update a valuer
func (this *RedisStateStore) Update(key string, oldValue string, newValue string) error {
	key = this.KeyPath + "." + key
	client := this.rds

	err := client.Watch(func(tx *redis.Tx) error {
		value, err := tx.Get(key).Result()
		if err == redis.Nil {
			err = fmt.Errorf("[%v] not exist", key)
			return err
		} else if err != nil {
			err = fmt.Errorf("unexpect error %v", err)
			return err
		}
		if value != oldValue {
			err = fmt.Errorf("Old value doesn't match for key %s", key)
			return err
		}
		_, err = tx.Pipelined(func(pl redis.Pipeliner) error {
			pl.Set(key, newValue, 0)
			return nil
		})
		return err
	}, key)
	return err
}

// Update Compare and Update a valuer
func (this *RedisStateStore) IncrementBy(key string, value int64) (int64, error) {
	key = this.KeyPath + "." + key
	client := this.rds
	return client.IncrBy(key, value).Result()
}

// Set Sets a value (override existing, or create one)
func (this *RedisStateStore) Set(key string, value string) error {
	key = this.KeyPath + "." + key
	client := this.rds
	err := client.Set(key, value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set key %s, error %v", key, err)
	}
	return nil
}

// Get Gets a value
func (this *RedisStateStore) Get(key string) (string, error) {
	key = this.KeyPath + "." + key
	client := this.rds
	v := client.Get(key)
	if v == nil {
		return "", errors.New(fmt.Sprintf("failed to get key %s, nil", key))
	}
	value, err := v.Result()
	if err == redis.Nil {
		return "", fmt.Errorf("failed to get key %s, nil", key)
	} else if err != nil {
		return "", fmt.Errorf("failed to get key %s, %v", key, err)
	}

	return value, nil
}

// Cleanup (Called only once in a request)
func (this *RedisStateStore) Cleanup() error {
	key := this.KeyPath + ".*"
	client := this.rds
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
func (this *RedisStateStore) CopyStore() (sdk.StateStore, error) {
	return &RedisStateStore{KeyPath: this.KeyPath, RetryCount: this.RetryCount, rds: this.rds}, nil
}
