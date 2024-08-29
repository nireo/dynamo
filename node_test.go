package dynamo

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// MockStorageEngine is a mock implementation of StorageEngine for testing
type MockStorageEngine struct {
	data map[string][]byte
}

func NewMockStorageEngine() *MockStorageEngine {
	return &MockStorageEngine{
		data: make(map[string][]byte),
	}
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	if value, ok := m.data[string(key)]; ok {
		return value, nil
	}
	return nil, errors.New("not found")
}

func (m *MockStorageEngine) Put(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockStorageEngine) Close() error {
	return nil
}

func TestNewDynamoNode(t *testing.T) {
	config := Config{
		N: 3,
		R: 2,
		W: 2,
	}
	node, err := NewDynamoNode(config, "127.0.0.1:7946", nil, "127.0.0.1:8946")
	if err != nil {
		t.Fatalf("Failed to create DynamoNode: %v", err)
	}
	defer node.Close()

	if node.conf.N != 3 || node.conf.R != 2 || node.conf.W != 2 {
		t.Errorf("Node config does not match input config")
	}

	if node.serf == nil {
		t.Errorf("Serf instance not created")
	}

	if node.consistent == nil {
		t.Errorf("Consistent hash ring not created")
	}
}

func TestPutAndGet(t *testing.T) {
	config := Config{
		N: 3,
		R: 2,
		W: 2,
	}
	node, err := NewDynamoNode(config, "127.0.0.1:7947", nil, "127.0.0.1:8947")
	if err != nil {
		t.Fatalf("Failed to create DynamoNode: %v", err)
	}
	defer node.Close()

	// Replace the storage engine with our mock
	node.storage = NewMockStorageEngine()

	key := []byte("testkey")
	value := []byte("testvalue")

	err = node.ClientPut(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	retrievedValue, err := node.ClientGet(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Retrieved value does not match put value. Got %s, want %s", retrievedValue, value)
	}
}

func TestMemberJoinAndLeave(t *testing.T) {
	config := Config{
		N: 3,
		R: 2,
		W: 2,
	}
	node1, err := NewDynamoNode(config, "127.0.0.1:7948", nil, "127.0.0.1:8948")
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Close()

	node2, err := NewDynamoNode(config, "127.0.0.1:7949", []string{"127.0.0.1:7948"}, "127.0.0.1:8949")
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Close()

	// Give some time for the cluster to form
	time.Sleep(2 * time.Second)

	members := node1.serf.Members()
	if len(members) != 2 {
		t.Errorf("Expected 2 members in the cluster, got %d", len(members))
	}

	// Simulate node2 leaving
	err = node2.serf.Leave()
	if err != nil {
		t.Fatalf("Failed to leave the cluster: %v", err)
	}

	// Give some time for the leave to propagate
	time.Sleep(2 * time.Second)

	members = node1.serf.Members()
	if len(members) != 1 {
		t.Errorf("Expected 1 member in the cluster after leave, got %d", len(members))
	}
}

func TestForwarding(t *testing.T) {
	config := Config{
		N: 3,
		R: 2,
		W: 2,
	}
	node1, err := NewDynamoNode(config, "127.0.0.1:7950", nil, "127.0.0.1:8950")
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Close()

	node2, err := NewDynamoNode(config, "127.0.0.1:7951", []string{"127.0.0.1:7950"}, "127.0.0.1:8951")
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Close()

	// Replace the storage engines with our mocks
	node1.storage = NewMockStorageEngine()
	node2.storage = NewMockStorageEngine()

	// Give some time for the cluster to form
	time.Sleep(2 * time.Second)

	key := []byte("testkey")
	value := []byte("testvalue")

	// Put the value using node1
	err = node1.ClientPut(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Try to get the value using node2
	retrievedValue, err := node2.ClientGet(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Retrieved value does not match put value. Got %s, want %s", retrievedValue, value)
	}
}
