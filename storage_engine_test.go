package dynamo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBadgerStore(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewBadgerStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = store.Put(key, value)
	assert.NoError(t, err)

	retrievedValue, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	nonExistentKey := []byte("non-existent-key")
	_, err = store.Get(nonExistentKey)
	assert.Error(t, err)

	err = store.Put([]byte("empty-value-key"), []byte{})
	assert.NoError(t, err)

	newValue := []byte("new-test-value")
	err = store.Put(key, newValue)
	assert.NoError(t, err)

	retrievedValue, err = store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, newValue, retrievedValue)
}

func TestBadgerStoreVersioned(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewBadgerStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Create a VectorClock
	vc := NewVectorClock()
	vc.Counter["node1"] = 42
	vc.Counter["node2"] = 10

	// Put the versioned value
	err = store.PutVersioned(key, value, vc)
	assert.NoError(t, err)

	// Get the versioned value
	retrievedVal, retrievedVC, err := store.GetVersioned(key)
	assert.NoError(t, err)

	// Check the value
	assert.Equal(t, value, retrievedVal)

	// Check the VectorClock
	assert.Equal(t, vc.Counter, retrievedVC.Counter)
	assert.Equal(t, uint64(42), retrievedVC.Counter["node1"])
	assert.Equal(t, uint64(10), retrievedVC.Counter["node2"])
}

func TestBadgerStoreVersionedMultipleUpdates(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewBadgerStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	key := []byte("test-key")
	value1 := []byte("test-value-1")
	value2 := []byte("test-value-2")

	// First update
	vc1 := NewVectorClock()
	vc1.Counter["node1"] = 1
	err = store.PutVersioned(key, value1, vc1)
	assert.NoError(t, err)

	// Second update
	vc2 := NewVectorClock()
	vc2.Counter["node1"] = 2
	vc2.Counter["node2"] = 1
	err = store.PutVersioned(key, value2, vc2)
	assert.NoError(t, err)

	// Retrieve and check
	retrievedVal, retrievedVC, err := store.GetVersioned(key)
	assert.NoError(t, err)

	assert.Equal(t, value2, retrievedVal)
	assert.Equal(t, vc2.Counter, retrievedVC.Counter)
	assert.Equal(t, uint64(2), retrievedVC.Counter["node1"])
	assert.Equal(t, uint64(1), retrievedVC.Counter["node2"])
}
