package statestore

import (
	"errors"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/s8sg/goflow/core/sdk"
)

// StateBackend abstracts the backend logic for statestore
type StateBackend interface {
	Configure(flowName string, requestId string)
	Init() error
	Set(key string, value string) error
	Get(key string) (string, error)
	Incr(key string, value int64) (int64, error)
	Update(key string, oldValue string, newValue string) error
	Cleanup() error
	CopyStore() (StateBackend, error)
}

type StateStore struct {
	backend    StateBackend
	RetryCount int
}

// Backend selection (default: redis)
func NewStateStoreRedis(redisUri, password string) (*StateStore, error) {
	backend, err := NewRedisBackend(redisUri, password)
	if err != nil {
		return nil, err
	}
	return &StateStore{backend: backend}, nil
}

// FoundationDB backend stub (to be implemented)
func NewStateStoreFoundationDB(clusterFile string) (*StateStore, error) {
	backend, err := NewFoundationDBBackend(clusterFile)
	if err != nil {
		return nil, err
	}
	return &StateStore{backend: backend}, nil
}

// StateStore methods delegate to backend
func (s *StateStore) Configure(flowName, requestId string)        { s.backend.Configure(flowName, requestId) }
func (s *StateStore) Init() error                                 { return s.backend.Init() }
func (s *StateStore) Set(key, value string) error                 { return s.backend.Set(key, value) }
func (s *StateStore) Get(key string) (string, error)              { return s.backend.Get(key) }
func (s *StateStore) Incr(key string, value int64) (int64, error) { return s.backend.Incr(key, value) }
func (s *StateStore) Update(key, oldValue, newValue string) error {
	return s.backend.Update(key, oldValue, newValue)
}
func (s *StateStore) Cleanup() error { return s.backend.Cleanup() }
func (s *StateStore) CopyStore() (sdk.StateStore, error) {
	b, err := s.backend.CopyStore()
	return &StateStore{backend: b}, err
}

// Redis backend implements StateBackend
type RedisBackend struct {
	KeyPath string
	rds     redis.UniversalClient
}

func NewRedisBackend(redisUri, password string) (*RedisBackend, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: password,
	})
	if err := client.Ping().Err(); err != nil {
		return nil, err
	}
	return &RedisBackend{rds: client}, nil
}

// RedisBackend implements StateBackend methods
func (r *RedisBackend) Configure(flowName, requestId string) {
	r.KeyPath = fmt.Sprintf("core.%s.%s", flowName, requestId)
}
func (r *RedisBackend) Init() error { return nil }
func (r *RedisBackend) Set(key, value string) error {
	key = r.KeyPath + "." + key
	return r.rds.Set(key, value, 0).Err()
}
func (r *RedisBackend) Get(key string) (string, error) {
	key = r.KeyPath + "." + key
	v := r.rds.Get(key)
	if v == nil {
		return "", fmt.Errorf("failed to get key %s, nil", key)
	}
	value, err := v.Result()
	if err == redis.Nil {
		return "", fmt.Errorf("failed to get key %s, nil", key)
	} else if err != nil {
		return "", fmt.Errorf("failed to get key %s, %v", key, err)
	}
	return value, nil
}
func (r *RedisBackend) Incr(key string, value int64) (int64, error) {
	key = r.KeyPath + "." + key
	return r.rds.IncrBy(key, value).Result()
}
func (r *RedisBackend) Update(key, oldValue, newValue string) error {
	key = r.KeyPath + "." + key
	client := r.rds
	err := client.Watch(func(tx *redis.Tx) error {
		value, err := tx.Get(key).Result()
		if err == redis.Nil {
			return fmt.Errorf("[%v] not exist", key)
		} else if err != nil {
			return fmt.Errorf("unexpect error %v", err)
		}
		if value != oldValue {
			return fmt.Errorf("Old value doesn't match for key %s", key)
		}
		_, err = tx.Pipelined(func(pl redis.Pipeliner) error {
			pl.Set(key, newValue, 0)
			return nil
		})
		return err
	}, key)
	return err
}
func (r *RedisBackend) Cleanup() error {
	key := r.KeyPath + ".*"
	client := r.rds
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
func (r *RedisBackend) CopyStore() (StateBackend, error) {
	return &RedisBackend{KeyPath: r.KeyPath, rds: r.rds}, nil
}
