package dynamo

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// mockConn is a mock network connection for testing
type mockConn struct {
	readData  chan []byte
	writeData chan []byte
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	data := <-m.readData
	copy(b, data)
	return len(data), nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData <- b
	return len(b), nil
}

func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestAddPeer(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	node.AddPeer("peer1", "localhost:8001")
	node.AddPeer("peer2", "localhost:8002")
	node.AddPeer("peer3", "localhost:8003")

	if len(node.Peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(node.Peers))
	}

	node.AddPeer("peer4", "localhost:8004")
	if len(node.Peers) != 3 {
		t.Errorf("Expected still 3 peers after adding 4th, got %d", len(node.Peers))
	}
}

func TestHandleGossipEvent(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	data := map[string]DataEntry{
		"key1": {Timestamp: 100, Value: "value1"},
		"key2": {Timestamp: 200, Value: "value2"},
	}

	node.handleGossipEvent(data)

	if len(node.Data) != 2 {
		t.Errorf("Expected 2 data entries, got %d", len(node.Data))
	}

	if node.Data["key1"].Value != "value1" {
		t.Errorf("Expected value1, got %s", node.Data["key1"].Value)
	}

	// Test updating with newer timestamp
	newerData := map[string]DataEntry{
		"key1": {Timestamp: 150, Value: "updatedValue1"},
	}
	node.handleGossipEvent(newerData)

	if node.Data["key1"].Value != "updatedValue1" {
		t.Errorf("Expected updatedValue1, got %s", node.Data["key1"].Value)
	}
}

func TestHandleJoin(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	joinData := map[string]DataEntry{
		"peer1": {Value: "localhost:8001", Timestamp: time.Now().Unix()},
		"peer2": {Value: "localhost:8002", Timestamp: time.Now().Unix()},
	}

	node.handleJoin(joinData)

	if len(node.Peers) != 2 {
		t.Errorf("Expected 2 peers after join, got %d", len(node.Peers))
	}

	if node.Peers["peer1"] != "localhost:8001" {
		t.Errorf("Expected peer1 address to be localhost:8001, got %s", node.Peers["peer1"])
	}
}

func TestHandleHeartbeat(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	node.handleHeartbeat("peer1")

	if _, ok := node.lastHeartbeat["peer1"]; !ok {
		t.Errorf("Expected heartbeat for peer1 to be recorded")
	}
}

func TestSendGossip(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	node.Data["key1"] = DataEntry{Timestamp: 100, Value: "value1"}

	mockConn := &mockConn{
		readData:  make(chan []byte, 1),
		writeData: make(chan []byte, 1),
	}

	go node.sendGossip("mockPeer")

	// Simulate receiving gossip data
	var receivedEvent Event
	data := <-mockConn.writeData
	err := json.Unmarshal(data, &receivedEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal gossip data: %v", err)
	}

	if receivedEvent.Type != EventTypeGossip {
		t.Errorf("Expected gossip event type, got %s", receivedEvent.Type)
	}

	if len(receivedEvent.Data) != 1 {
		t.Errorf("Expected 1 data entry in gossip, got %d", len(receivedEvent.Data))
	}

	if receivedEvent.Data["key1"].Value != "value1" {
		t.Errorf("Expected value1 in gossip data, got %s", receivedEvent.Data["key1"].Value)
	}
}

func TestCheckHeartbeatFailures(t *testing.T) {
	node := NewNode("test", "localhost:8000", 3)
	node.heartbeatInterval = 1 * time.Second

	node.Peers["peer1"] = "localhost:8001"
	node.Peers["peer2"] = "localhost:8002"

	node.lastHeartbeat["peer1"] = time.Now().Add(-4 * time.Second)
	node.lastHeartbeat["peer2"] = time.Now()

	node.checkHeartbeatFailures()

	if _, ok := node.Peers["peer1"]; ok {
		t.Errorf("Expected peer1 to be removed due to heartbeat failure")
	}

	if _, ok := node.Peers["peer2"]; !ok {
		t.Errorf("Expected peer2 to still be present")
	}
}

func TestConcurrentAccess(t *testing.T) {
	node := NewNode("test", "localhost:8000", 10)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node.AddData(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}(i)
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node.AddPeer(fmt.Sprintf("peer%d", i), fmt.Sprintf("localhost:%d", 8001+i))
		}(i)
	}

	wg.Wait()

	if len(node.Data) != 100 {
		t.Errorf("Expected 100 data entries, got %d", len(node.Data))
	}

	if len(node.Peers) != 10 {
		t.Errorf("Expected 10 peers (max peers), got %d", len(node.Peers))
	}
}
