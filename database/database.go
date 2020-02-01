package database

import "errors"

var (
	NotFoundError = errors.New("not found")
)

type DB interface {
	Put(bucket []byte, key []byte, value []byte) error
	Get(bucket []byte, key []byte) ([]byte, error)
	Update(bucket []byte, key []byte, newValue []byte) error
	Remove(bucket []byte, key []byte) error
	Shutdown() error
}