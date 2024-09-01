package dynamo

import (
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStorageEngine is a mock implementation of the StorageEngine interface
type MockStorageEngine struct {
	mock.Mock
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockStorageEngine) Put(key, value []byte) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *MockStorageEngine) GetVersioned(key []byte) ([]byte, uint64, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Get(1).(uint64), args.Error(2)
}

func (m *MockStorageEngine) PutVersioned(key, value []byte, version uint64) error {
	args := m.Called(key, value, version)
	return args.Error(0)
}

func (m *MockStorageEngine) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockRPCClient is a mock implementation of the RpcClient interface
type MockRPCClient struct {
	mock.Mock
}

func (m *MockRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	mockArgs := m.Called(serviceMethod, args, reply)
	return mockArgs.Error(0)
}

func (m *MockRPCClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Helper function to create a test DynamoNode
func createTestDynamoNode(t *testing.T) (*DynamoNode, *MockStorageEngine, map[string]*MockRPCClient) {
	mockStorage := new(MockStorageEngine)
	mockClients := make(map[string]*MockRPCClient)

	node := &DynamoNode{
		conf:     Config{N: 3, R: 2, W: 2},
		storage:  mockStorage,
		serfConf: &serf.Config{NodeName: "node1"},
		consistent: consistent.New(nil, consistent.Config{
			PartitionCount:    10,
			ReplicationFactor: 3,
			Load:              1.25,
			Hasher:            hasher{},
		}),
		rpcConnections: make(map[string]RpcClient),
	}

	// Add test nodes to the consistent hash ring and create mock clients
	for _, nodeName := range []string{"node1", "node2", "node3"} {
		member := MemberWrapper{serf.Member{Name: nodeName, Tags: map[string]string{"rpc_addr": "127.0.0.1:800" + nodeName[4:]}}}
		node.consistent.Add(member)

		if nodeName != "node1" { // node1 is the local node, so we don't need a mock client for it
			mockClient := new(MockRPCClient)
			mockClients[nodeName] = mockClient
			node.rpcConnections[member.Tags["rpc_addr"]] = mockClient
		}
	}

	return node, mockStorage, mockClients
}

func TestGet(t *testing.T) {
	node, mockStorage, mockClients := createTestDynamoNode(t)

	t.Run("Successful Get", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")
		version := uint64(time.Now().UnixNano())

		// Set up expectations for all possible nodes
		mockStorage.On("GetVersioned", key).Return(value, version, nil).Maybe()
		for _, client := range mockClients {
			client.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				reply := args.Get(2).(*RPCReply)
				reply.Value = value
				reply.Version = version
			}).Return(nil).Maybe()
		}

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.NoError(t, err)
		assert.Equal(t, value, reply.Value)
		assert.Equal(t, version, reply.Version)

		// Verify that we had the correct number of calls
		totalCalls := len(mockStorage.Calls)
		for _, client := range mockClients {
			totalCalls += len(client.Calls)
		}
		assert.Equal(t, node.conf.R, totalCalls, "Should have exactly R successful calls")

		// Additional check to ensure no mock was called more than once
		assert.LessOrEqual(t, len(mockStorage.Calls), 1, "Storage should be called at most once")
		for nodeName, client := range mockClients {
			assert.LessOrEqual(t, len(client.Calls), 1, "Client %s should be called at most once", nodeName)
		}
	})
}

func TestPut(t *testing.T) {
	node, mockStorage, mockClients := createTestDynamoNode(t)

	t.Run("Successful Put with local write", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("uint64")).Return(nil)

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.NoError(t, err)
		assert.NotZero(t, reply.Version)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Successful Put with remote writes", func(t *testing.T) {
		key := []byte("remote-key")
		value := []byte("remote-value")

		mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("uint64")).Return(errors.New("local write failed"))

		mockClients["node2"].On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(nil)
		mockClients["node3"].On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(nil)

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.NoError(t, err)
		assert.NotZero(t, reply.Version)
		mockStorage.AssertExpectations(t)
		mockClients["node2"].AssertExpectations(t)
		mockClients["node3"].AssertExpectations(t)
	})

	t.Run("Put fails due to not enough replicas", func(t *testing.T) {
		key := []byte("unavailable-key")
		value := []byte("test-value")

		mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("uint64")).Return(errors.New("local write failed"))

		mockClients["node2"].On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))
		mockClients["node3"].On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.Error(t, err)
		assert.Equal(t, ErrNotEnoughReplicas, err)
		mockStorage.AssertExpectations(t)
		mockClients["node2"].AssertExpectations(t)
		mockClients["node3"].AssertExpectations(t)
	})
}

func TestGetLocal(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	version := uint64(time.Now().UnixNano())

	mockStorage.On("GetVersioned", key).Return(value, version, nil)

	args := &RPCArgs{Key: key}
	reply := &RPCReply{}

	err := node.GetLocal(args, reply)

	assert.NoError(t, err)
	assert.Equal(t, value, reply.Value)
	assert.Equal(t, version, reply.Version)
	mockStorage.AssertExpectations(t)
}

func TestPutLocal(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	version := uint64(time.Now().UnixNano())

	mockStorage.On("PutVersioned", key, value, version).Return(nil)

	args := &RPCArgs{Key: key, Value: value, Version: version}
	reply := &RPCReply{}

	err := node.PutLocal(args, reply)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestClientGet(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("client-get-key")
	value := []byte("client-get-value")
	version := uint64(time.Now().UnixNano())

	mockStorage.On("GetVersioned", key).Return(value, version, nil)

	result, err := node.ClientGet(key)

	assert.NoError(t, err)
	assert.Equal(t, value, result)
	mockStorage.AssertExpectations(t)
}

func TestClientPut(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("client-put-key")
	value := []byte("client-put-value")

	mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("uint64")).Return(nil)

	err := node.ClientPut(key, value)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
