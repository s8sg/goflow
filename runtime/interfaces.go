package runtime

import (
	"context"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis"
	"github.com/s8sg/goflow/core/sdk"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go

// RedisClient abstracts Redis operations for better testability
type RedisClient interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
	Exists(keys ...string) *redis.IntCmd
	Close() error
	Ping() *redis.StatusCmd
}

// QueueConnection abstracts RabbitMQ queue operations for better testability
type QueueConnection interface {
	OpenQueue(name string) (rmq.Queue, error)
	CollectStats(queueList []string) (rmq.Stats, error)
	GetOpenQueues() ([]string, error)
	StopAllConsuming() <-chan struct{}
}

// StateStoreFactory abstracts state store creation
type StateStoreFactory interface {
	CreateStateStore(redisURI, password string) (sdk.StateStore, error)
}

// DataStoreFactory abstracts data store creation
type DataStoreFactory interface {
	CreateDataStore(redisURI, password string) (sdk.DataStore, error)
}

// HTTPServerInterface abstracts HTTP server operations
type HTTPServerInterface interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
	Close() error
}

// Default implementations that wrap the real dependencies

type DefaultStateStoreFactory struct{}

func (f *DefaultStateStoreFactory) CreateStateStore(redisURI, password string) (sdk.StateStore, error) {
	return initStateStore(redisURI, password)
}

type DefaultDataStoreFactory struct{}

func (f *DefaultDataStoreFactory) CreateDataStore(redisURI, password string) (sdk.DataStore, error) {
	return initDataStore(redisURI, password)
}

// RedisClientWrapper wraps go-redis client to implement our interface
type RedisClientWrapper struct {
	client *redis.Client
}

func NewRedisClientWrapper(client *redis.Client) *RedisClientWrapper {
	return &RedisClientWrapper{client: client}
}

func (w *RedisClientWrapper) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return w.client.Set(key, value, expiration)
}

func (w *RedisClientWrapper) Get(key string) *redis.StringCmd {
	return w.client.Get(key)
}

func (w *RedisClientWrapper) Del(keys ...string) *redis.IntCmd {
	return w.client.Del(keys...)
}

func (w *RedisClientWrapper) Exists(keys ...string) *redis.IntCmd {
	return w.client.Exists(keys...)
}

func (w *RedisClientWrapper) Close() error {
	return w.client.Close()
}

func (w *RedisClientWrapper) Ping() *redis.StatusCmd {
	return w.client.Ping()
}

// QueueConnectionWrapper wraps rmq.Connection to implement our interface
type QueueConnectionWrapper struct {
	conn rmq.Connection
}

func NewQueueConnectionWrapper(conn rmq.Connection) *QueueConnectionWrapper {
	return &QueueConnectionWrapper{conn: conn}
}

func (w *QueueConnectionWrapper) OpenQueue(name string) (rmq.Queue, error) {
	return w.conn.OpenQueue(name)
}

func (w *QueueConnectionWrapper) CollectStats(queueList []string) (rmq.Stats, error) {
	return w.conn.CollectStats(queueList)
}

func (w *QueueConnectionWrapper) GetOpenQueues() ([]string, error) {
	return w.conn.GetOpenQueues()
}

func (w *QueueConnectionWrapper) StopAllConsuming() <-chan struct{} {
	return w.conn.StopAllConsuming()
}
