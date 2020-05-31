package database

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrEmptyBucket = errors.New("empty bucket")
)

type DB interface {
	Put(bucket []byte, key []byte, value []byte) error
	Get(bucket []byte, key []byte) ([]byte, error)
	Update(bucket []byte, key []byte, newValue []byte) error
	Remove(bucket []byte, key []byte) error
	Clean(bucket []byte) error
	Shutdown() error
}
