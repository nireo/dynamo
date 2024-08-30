package dynamo

import (
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// Helper function to create a test DynamoNode
func createTestDynamoNode(t *testing.T) (*DynamoNode, *MockStorageEngine) {
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
	}

	// Add some test nodes to the consistent hash ring
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node1", Tags: map[string]string{"rpc_addr": "localhost:8001"}}})
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node2", Tags: map[string]string{"rpc_addr": "localhost:8002"}}})
	node.consistent.Add(MemberWrapper{serf.Member{Name: "node3", Tags: map[string]string{"rpc_addr": "localhost:8003"}}})

	return node, mockStorage
}

func TestGet(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

	t.Run("Successful Get", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")
		version := time.Now().UnixNano()

		mockStorage.EXPECT().GetVersioned(key).Return(value, version, nil)

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.NoError(t, err)
		assert.Equal(t, value, reply.Value)
		assert.Equal(t, version, reply.Version)
	})

	t.Run("Not Enough Replicas", func(t *testing.T) {
		key := []byte("unavailable-key")
		mockStorage.EXPECT().GetVersioned(key).Return(nil, 0, errors.New("not found"))

		args := &RPCArgs{Key: key}
		reply := &RPCReply{}

		err := node.Get(args, reply)

		assert.Error(t, err)
		assert.Equal(t, ErrNotEnoughReplicas, err)
	})
}

func TestPut(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

	// Test successful Put operation
	t.Run("Successful Put", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		mockStorage.EXPECT().PutVersioned(key, value, gomock.Any()).Return(nil)

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.NoError(t, err)
		assert.NotZero(t, reply.Version)
	})

	// Test Put operation with not enough replicas
	t.Run("Not Enough Replicas", func(t *testing.T) {
		key := []byte("unavailable-key")
		value := []byte("test-value")

		mockStorage.EXPECT().PutVersioned(key, value, gomock.Any()).Return(errors.New("something went wrong"))

		args := &RPCArgs{Key: key, Value: value}
		reply := &RPCReply{}

		err := node.Put(args, reply)

		assert.Error(t, err)
		assert.Equal(t, ErrNotEnoughReplicas, err)
	})
}

func TestGetLocal(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	version := time.Now().UnixNano()

	mockStorage.EXPECT().GetVersioned(key).Return(value, version, nil)

	args := &RPCArgs{Key: key}
	reply := &RPCReply{}

	err := node.GetLocal(args, reply)

	assert.NoError(t, err)
	assert.Equal(t, value, reply.Value)
	assert.Equal(t, version, reply.Version)
}

func TestPutLocal(t *testing.T) {
	node, mockStorage := createTestDynamoNode(t)

	key := []byte("local-key")
	value := []byte("local-value")
	version := uint64(time.Now().UnixNano())

	mockStorage.EXPECT().PutVersioned(key, value, version).Return(nil)

	args := &RPCArgs{Key: key, Value: value, Version: version}
	reply := &RPCReply{}

	err := node.PutLocal(args, reply)
	assert.NoError(t, err)
}
