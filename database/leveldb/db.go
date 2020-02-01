package leveldb_go

import (
	"github.com/qshuai/bitstate/database"
	"github.com/qshuai/bitstate/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type levelDB struct {
	db *leveldb.DB
}

func (l *levelDB) Put(bucket []byte, key []byte, value []byte) error {
	bucketedKey := spliceKey(bucket, key)
	return l.db.Put(bucketedKey, value, nil)
}

func (l *levelDB) Get(bucket []byte, key []byte) ([]byte, error) {
	bucketedKey := spliceKey(bucket, key)
	value, err := l.db.Get(bucketedKey, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, database.NotFoundError
		}

		return nil, err
	}

	return value, err
}

func (l *levelDB) Update(bucket []byte, key []byte, newValue []byte) error {
	return nil
}

func (l *levelDB) Remove(bucket []byte, key []byte) error {
	bucketedKey := spliceKey(bucket, key)
	return l.db.Delete(bucketedKey, nil)
}

func (l *levelDB) Shutdown() error {
	return l.db.Close()
}

func spliceKey(bucket []byte, key []byte) []byte {
	if bucket == nil {
		return key
	}

	bucketedKey := make([]byte, len(bucket)+len(key))
	copy(bucketedKey, bucket)
	copy(bucketedKey[len(bucket):], key)

	return bucketedKey
}

func New(filePath string, options *opt.Options) (*levelDB, error) {
	err := utils.MkDirAndFile(filePath)
	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(filePath, options)
	if err != nil {
		return nil, err
	}

	return &levelDB{db: db}, nil
}
