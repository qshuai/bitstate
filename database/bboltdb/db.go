package bboltdb

import (
	"github.com/qshuai/bitstate/database"
	"github.com/qshuai/bitstate/utils"
	"go.etcd.io/bbolt"
)

type bboltDB struct {
	db *bbolt.DB
}

func (b bboltDB) Put(bucket []byte, key []byte, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(key, value)
	})
}

func (b *bboltDB) Get(bucket []byte, key []byte) ([]byte, error) {
	var copyValue []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(bucket).Get(key)
		if value == nil {
			return database.NotFoundError
		}

		copyValue = make([]byte, len(value))
		copy(copyValue, value)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return copyValue, nil
}

func (b *bboltDB) Update(bucket []byte, key []byte, newValue []byte) error {
	return nil
}

func (b *bboltDB) Remove(bucket []byte, key []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Delete(key)
	})

	return err
}

func (b *bboltDB) InitBucket(buckets ...[]byte) error {
	for _, bucket := range buckets {
		err := b.db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucket)
			return err
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (b *bboltDB) Shutdown() error {
	return b.db.Close()
}

func New(filePath string, options *bbolt.Options) (*bboltDB, error) {
	err := utils.MkDirAndFile(filePath)
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(filePath, 0600, options)
	if err != nil {
		return nil, err
	}

	return &bboltDB{db}, nil
}
