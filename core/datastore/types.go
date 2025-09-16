package datastore

// Package datastore provides types and utilities for data storage management within the goflow core.
// This file defines core types and interfaces for database-agnostic storage operations.
import (
	"context"
)

// DataStore represents a datastore with a specific bucket and client.
type DataStore struct {
	bucketName string
	client     DataStorageClient
}

// ScanResult represents the result of a scan operation
type ScanResult struct {
	Keys   []string
	Cursor uint64
	Err    error
}

// ScanIterator provides an interface for iterating over scan results
type ScanIterator interface {
	Next(ctx context.Context) bool
	Val() string
	Err() error
}

// DataStorageClient provides a database-agnostic interface for storage operations
type DataStorageClient interface {
	// Ping tests the connection to the storage backend
	Ping(ctx context.Context) error

	// Scan searches for keys matching a pattern and returns an iterator
	Scan(ctx context.Context, cursor uint64, match string, count int64) ScanIterator

	// Set stores a value with the given key
	Set(ctx context.Context, key string, value interface{}, expirationSeconds int) error

	// Get retrieves a value by key
	Get(ctx context.Context, key string) (string, error)

	// Delete removes one or more keys
	Delete(ctx context.Context, keys ...string) (int64, error)
}
