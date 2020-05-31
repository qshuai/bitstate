package bboltdb

import (
	"github.com/qshuai/bitstate/database"
	"github.com/qshuai/bitstate/utils"
	"go.etcd.io/bbolt"
)

type BboltDB struct {
	db *bbolt.DB
}

func (b BboltDB) Put(bucket []byte, key []byte, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put(key, value)
	})
}

func (b *BboltDB) Get(bucket []byte, key []byte) ([]byte, error) {
	var copyValue []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(bucket).Get(key)
		if value == nil {
			return database.ErrNotFound
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

func (b *BboltDB) Update(bucket []byte, key []byte, newValue []byte) error {
	return nil
}

func (b *BboltDB) Remove(bucket []byte, key []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Delete(key)
	})

	return err
}

func (b *BboltDB) Clean(bucket []byte) error {
	if len(bucket) <= 0 {
		return database.ErrEmptyBucket
	}

	return b.db.View(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket(bucket)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucket(bucket)
		if err != nil {
			return err
		}

		return nil
	})
}

func (b *BboltDB) InitBucket(buckets ...[]byte) error {
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

func (b *BboltDB) Shutdown() error {
	return b.db.Close()
}

func New(filePath string, options *bbolt.Options) (*BboltDB, error) {
	err := utils.MkDirAndFile(filePath)
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(filePath, 0600, options)
	if err != nil {
		return nil, err
	}

	return &BboltDB{db}, nil
}
