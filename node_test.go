package dynamo

import (
	"fmt"
	"net"
	"testing"
	"time"
)

type MockStorage struct {
	data map[string][]byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{data: make(map[string][]byte)}
}

func (m *MockStorage) Get(key []byte) ([]byte, error) {
	if val, ok := m.data[string(key)]; ok {
		return val, nil
	}
	return nil, nil
}

func (m *MockStorage) Put(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockStorage) Close() error {
	return nil
}

func getFreeTCPPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestNewDynamoNode(t *testing.T) {
	config := Config{N: 3, R: 2, W: 2}

	bindPort, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	bindAddr := fmt.Sprintf("127.0.0.1:%d", bindPort)

	rpcPort, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	rpcAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort)

	node, err := NewDynamoNode(config, bindAddr, []string{}, rpcAddr)
	if err != nil {
		t.Fatalf("Failed to create DynamoNode: %v", err)
	}
	defer node.Close()

	if node.conf != config {
		t.Errorf("Expected config %v, got %v", config, node.conf)
	}

	if node.serf == nil {
		t.Error("Serf instance is nil")
	}

	if node.consistent == nil {
		t.Error("Consistent hashing instance is nil")
	}

	if node.rpcServer == nil {
		t.Error("RPC server is nil")
	}

	if node.rpcListener == nil {
		t.Error("RPC listener is nil")
	}
}

func TestDynamoNodePutGet(t *testing.T) {
	config := Config{N: 0, R: 1, W: 1}
	bindPort, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	bindAddr := fmt.Sprintf("127.0.0.1:%d", bindPort)

	rpcPort, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	rpcAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort)

	node, err := NewDynamoNode(config, bindAddr, []string{}, rpcAddr)
	if err != nil {
		t.Fatalf("Failed to create DynamoNode: %v", err)
	}
	defer node.Close()

	// Replace the storage with our mock
	node.storage = NewMockStorage()

	key := []byte("testKey")
	value := []byte("testValue")

	// Test Put
	err = node.ClientPut(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Test Get
	retrievedValue, err := node.ClientGet(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(retrievedValue))
	}
}

func TestDynamoNodeForwarding(t *testing.T) {
	config := Config{N: 2, R: 1, W: 1}

	bindPort1, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	bindAddr1 := fmt.Sprintf("127.0.0.1:%d", bindPort1)

	rpcPort1, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	rpcAddr1 := fmt.Sprintf("127.0.0.1:%d", rpcPort1)

	node1, err := NewDynamoNode(config, bindAddr1, []string{}, rpcAddr1)
	if err != nil {
		t.Fatalf("Failed to create DynamoNode1: %v", err)
	}
	defer node1.Close()

	bindPort2, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	bindAddr2 := fmt.Sprintf("127.0.0.1:%d", bindPort2)

	rpcPort2, err := getFreeTCPPort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	rpcAddr2 := fmt.Sprintf("127.0.0.1:%d", rpcPort2)

	node2, err := NewDynamoNode(config, bindAddr2, []string{bindAddr1}, rpcAddr2)
	if err != nil {
		t.Fatalf("Failed to create DynamoNode2: %v", err)
	}
	defer node2.Close()

	// Wait for node2 to join
	time.Sleep(1 * time.Second)

	// Replace the storage with our mock
	node1.storage = NewMockStorage()
	node2.storage = NewMockStorage()

	key := []byte("testKey")
	value := []byte("testValue")

	// Put the value (this may be forwarded to the appropriate node)
	err = node1.ClientPut(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Get the value (this may be forwarded to the appropriate node)
	retrievedValue, err := node2.ClientGet(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(retrievedValue))
	}
}
