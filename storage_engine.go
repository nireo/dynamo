package dynamo

import "github.com/dgraph-io/badger/v3"

// StorageEngine is a common interface that implements all of the methods that are needed to store data on disk
// or anywhere else etc.
type StorageEngine interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Close() error
}

// BadgerStorage implements the StorageEngine interface using BadgerDB
type BadgerStorage struct {
	db *badger.DB
}

// NewBadgerStorage creates a new BadgerStorage instance
func NewBadgerStore(path string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStorage{db: db}, nil
}

// Get retrieves the value for a given key
func (b *BadgerStorage) Get(key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Put stores a key-value pair
func (b *BadgerStorage) Put(key, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Close closes the BadgerDB instance
func (b *BadgerStorage) Close() error {
	return b.db.Close()
}
