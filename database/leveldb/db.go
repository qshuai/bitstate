package leveldb

import (
	"github.com/qshuai/bitstate/database"
	"github.com/qshuai/bitstate/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	db *leveldb.DB
}

func (l *LevelDB) Put(bucket []byte, key []byte, value []byte) error {
	bucketedKey := spliceKey(bucket, key)
	return l.db.Put(bucketedKey, value, nil)
}

func (l *LevelDB) Get(bucket []byte, key []byte) ([]byte, error) {
	bucketedKey := spliceKey(bucket, key)
	value, err := l.db.Get(bucketedKey, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, database.ErrNotFound
		}

		return nil, err
	}

	return value, err
}

func (l *LevelDB) Update(bucket []byte, key []byte, newValue []byte) error {
	return nil
}

func (l *LevelDB) Remove(bucket []byte, key []byte) error {
	bucketedKey := spliceKey(bucket, key)
	return l.db.Delete(bucketedKey, nil)
}

func (l *LevelDB) Clean(bucket []byte) error {
	if len(bucket) <= 0 {
		return database.ErrEmptyBucket
	}

	var err error
	iter := l.db.NewIterator(util.BytesPrefix(bucket), nil)
	for iter.Next() {
		err = l.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return iter.Error()
}

func (l *LevelDB) Shutdown() error {
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

func New(filePath string, options *opt.Options) (*LevelDB, error) {
	err := utils.MkDirAndFile(filePath)
	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(filePath, options)
	if err != nil {
		return nil, err
	}

	return &LevelDB{db: db}, nil
}
