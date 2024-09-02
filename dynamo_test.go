package dynamo

import (
	"testing"

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

func (m *MockStorageEngine) GetVersioned(key []byte) ([]byte, *VectorClock, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Get(1).(*VectorClock), args.Error(2)
}

func (m *MockStorageEngine) PutVersioned(key, value []byte, clock *VectorClock) error {
	args := m.Called(key, value, clock)
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
		Storage:  mockStorage,
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
		member := Member{serf.Member{Name: nodeName, Tags: map[string]string{"rpc_addr": "127.0.0.1:800" + nodeName[4:]}}}
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
		clock := NewVectorClock()
		clock.Increment("node1")

		// Set up expectations for all possible nodes
		mockStorage.On("GetVersioned", key).Return(value, clock, nil).Maybe()
		for _, client := range mockClients {
			client.On("Call", "DynamoNode.GetLocal", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				reply := args.Get(2).(*RPCReply)
				reply.Value = value
				reply.Clock = clock
			}).Return(nil).Maybe()
		}

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.NoError(t, err)
		assert.Equal(t, value, reply.Value)
		assert.Equal(t, clock, reply.Clock)

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

	t.Run("Successful Put", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		// Set up expectations for all possible nodes
		mockStorage.On("PutVersioned", key, value, mock.AnythingOfType("*dynamo.VectorClock")).Return(nil).Maybe()
		for _, client := range mockClients {
			client.On("Call", "DynamoNode.PutLocal", mock.Anything, mock.Anything).Return(nil).Maybe()
		}

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.NoError(t, err)
		assert.NotNil(t, reply.Clock)
		assert.Equal(t, uint64(1), reply.Clock.Counter["node1"], "Local node counter should be incremented")

		// Verify that we had the correct number of calls
		totalCalls := len(mockStorage.Calls)
		for _, client := range mockClients {
			totalCalls += len(client.Calls)
		}
		assert.Equal(t, node.conf.W, totalCalls, "Should have exactly W successful calls")

		// Additional check to ensure no mock was called more than once
		assert.LessOrEqual(t, len(mockStorage.Calls), 1, "Storage should be called at most once")
		for nodeName, client := range mockClients {
			assert.LessOrEqual(t, len(client.Calls), 1, "Client %s should be called at most once", nodeName)
		}
	})
}

func TestGetLocal(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	clock := NewVectorClock()
	clock.Increment("node1")

	mockStorage.On("GetVersioned", key).Return(value, clock, nil)

	args := &RPCArgs{Key: key}
	reply := &RPCReply{}

	err := node.GetLocal(args, reply)

	assert.NoError(t, err)
	assert.Equal(t, value, reply.Value)
	assert.Equal(t, clock, reply.Clock)
	mockStorage.AssertExpectations(t)
}

func TestPutLocal(t *testing.T) {
	node, mockStorage, _ := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	clock := NewVectorClock()
	clock.Increment("node1")

	mockStorage.On("PutVersioned", key, value, clock).Return(nil)

	args := &RPCArgs{Key: key, Value: value, Clock: clock}
	reply := &RPCReply{}

	err := node.PutLocal(args, reply)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
