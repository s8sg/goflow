package datastore

// Package datastore provides types and utilities for data storage management within the goflow core.
// This file defines core types and imports required for datastore operations.
import (
	"context"

	"github.com/go-redis/redis/v8"
)

// DataStore represents a datastore with a specific bucket and client.
type DataStore struct {
	bucketName string
	client     StorageClient
}
type StorageClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	Set(ctx context.Context, key string, value interface{}, expirationSeconds int) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Delete(ctx context.Context, keys ...string) *redis.IntCmd
}
