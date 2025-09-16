//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

package statestore

import (
	"time"

	"github.com/go-redis/redis"
)

// RedisClient interface abstracts the Redis client operations
type RedisClient interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	IncrBy(key string, value int64) *redis.IntCmd
	Watch(fn func(*redis.Tx) error, keys ...string) error
	Del(keys ...string) *redis.IntCmd
	Scan(cursor uint64, match string, count int64) *redis.ScanCmd
	Ping() *redis.StatusCmd
}
