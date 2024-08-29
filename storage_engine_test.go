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
