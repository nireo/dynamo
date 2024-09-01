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
func createTestDynamoNode(t *testing.T) (*DynamoNode, *MockStorageEngine) {
	t.Helper()
	mockStorage := new(MockStorageEngine)
	node := &DynamoNode{
		conf:     Config{N: 3, R: 2, W: 2},
		storage:  mockStorage,
		serfConf: &serf.Config{NodeName: "test-node"},
		consistent: consistent.New(nil, consistent.Config{
			PartitionCount:    10,
			ReplicationFactor: 3,
			Load:              1.25,
			Hasher:            hasher{},
		}),
		rpcConnections: make(map[string]RpcClient),
	}

	// Add some test nodes to the consistent hash ring
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node1", Tags: map[string]string{"rpc_addr": "localhost:8001"}}})
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node2", Tags: map[string]string{"rpc_addr": "localhost:8002"}}})
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node3", Tags: map[string]string{"rpc_addr": "localhost:8003"}}})

	return node, mockStorage
}

func TestGet(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

	t.Run("Successful Get with local read", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")
		version := uint64(time.Now().UnixNano())

		mockStorage.On("GetVersioned", key).Return(value, version, nil)

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.NoError(t, err)
		assert.Equal(t, value, reply.Value)
		assert.Equal(t, version, reply.Version)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Successful Get with remote reads", func(t *testing.T) {
		key := []byte("remote-key")
		value := []byte("remote-value")
		version := uint64(time.Now().UnixNano())

		mockStorage.On("GetVersioned", key).Return(nil, uint64(0), errors.New("not found"))

		mockClient1 := new(MockRPCClient)
		mockClient2 := new(MockRPCClient)
		node.rpcConnections["localhost:8002"] = mockClient1
		node.rpcConnections["localhost:8003"] = mockClient2

		mockClient1.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			reply := args.Get(2).(*RPCReply)
			reply.Value = value
			reply.Version = version
		}).Return(nil)

		mockClient2.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.NoError(t, err)
		assert.Equal(t, value, reply.Value)
		assert.Equal(t, version, reply.Version)
		mockStorage.AssertExpectations(t)
		mockClient1.AssertExpectations(t)
		mockClient2.AssertExpectations(t)
	})

	t.Run("Get fails due to not enough replicas", func(t *testing.T) {
		key := []byte("unavailable-key")

		mockStorage.On("GetVersioned", key).Return(nil, uint64(0), errors.New("not found"))

		mockClient1 := new(MockRPCClient)
		mockClient2 := new(MockRPCClient)
		node.rpcConnections["localhost:8002"] = mockClient1
		node.rpcConnections["localhost:8003"] = mockClient2

		mockClient1.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))
		mockClient2.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.Error(t, err)
		assert.Equal(t, ErrNotEnoughReplicas, err)
		mockStorage.AssertExpectations(t)
		mockClient1.AssertExpectations(t)
		mockClient2.AssertExpectations(t)
	})
}

func TestPut(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

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

		mockClient1 := new(MockRPCClient)
		mockClient2 := new(MockRPCClient)
		node.rpcConnections["localhost:8002"] = mockClient1
		node.rpcConnections["localhost:8003"] = mockClient2

		mockClient1.On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(nil)
		mockClient2.On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(nil)

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.NoError(t, err)
		assert.NotZero(t, reply.Version)
		mockStorage.AssertExpectations(t)
		mockClient1.AssertExpectations(t)
		mockClient2.AssertExpectations(t)
	})

	t.Run("Put fails due to not enough replicas", func(t *testing.T) {
		key := []byte("unavailable-key")
		value := []byte("test-value")

		mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("uint64")).Return(errors.New("local write failed"))

		mockClient1 := new(MockRPCClient)
		mockClient2 := new(MockRPCClient)
		node.rpcConnections["localhost:8002"] = mockClient1
		node.rpcConnections["localhost:8003"] = mockClient2

		mockClient1.On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))
		mockClient2.On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(errors.New("network error"))

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.Error(t, err)
		assert.Equal(t, ErrNotEnoughReplicas, err)
		mockStorage.AssertExpectations(t)
		mockClient1.AssertExpectations(t)
		mockClient2.AssertExpectations(t)
	})
}

func TestGetLocal(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

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
	node, mockStorage := createTestDynamoNode(t)

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
