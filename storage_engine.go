package dynamo

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
)

// StorageEngine is a common interface that implements all of the methods that are needed to store data on disk
// or anywhere else etc.
type StorageEngine interface {
	// Get gets the corresponding value of a key from the data store.
	Get(key []byte) ([]byte, error)

	// Put stores a simple key-value pair into the data store.
	Put(key, value []byte) error

	// GetVersioned is an extension of the Get method where the version metadata is read from the
	// value before returned. NOTE: That GetVersioned should only be used to read items that are
	// inserted using PutVersioned
	GetVersioned(key []byte) ([]byte, *VectorClock, error)

	// PutVersioned writes a given key value pair and adds a version at the start of the value that
	// is then written into the data store. NOTE: This value should only be read by using GetVersioned
	// otherwise this will cause problems.
	PutVersioned(key, value []byte, vectorClock *VectorClock) error

	// Close clear out any left over resources etc if those need to be cleared
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

// GetVersioned -- see interface documentation
func (b *BadgerStorage) GetVersioned(key []byte) ([]byte, *VectorClock, error) {
	val, err := b.Get(key)
	if err != nil {
		return nil, nil, err
	}

	vclen := binary.LittleEndian.Uint64(val[:8])
	vcdata := val[8 : 8+vclen]

	vc := NewVectorClock()
	if err := vc.Deserialize(vcdata); err != nil {
		return nil, nil, err
	}
	return val[8+vclen:], vc, nil
}

// PutVersioned stores a key-value pair with a vector clock
func (b *BadgerStorage) PutVersioned(key, value []byte, vc *VectorClock) error {
	vcData := vc.Serialize()
	vcLen := len(vcData)

	newVal := make([]byte, 8+vcLen+len(value))
	binary.LittleEndian.PutUint64(newVal, uint64(vcLen))
	copy(newVal[8:], vcData)
	copy(newVal[8+vcLen:], value)

	return b.Put(key, newVal)
}

// Close closes the BadgerDB instance
func (b *BadgerStorage) Close() error {
	return b.db.Close()
}
