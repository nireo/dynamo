package dynamo

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestVectorClockSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name string
		vc   VectorClock
	}{
		{
			name: "Empty VectorClock",
			vc:   VectorClock{Counter: make(map[string]uint64)},
		},
		{
			name: "Single Entry",
			vc:   VectorClock{Counter: map[string]uint64{"node1": 42}},
		},
		{
			name: "Multiple Entries",
			vc:   VectorClock{Counter: map[string]uint64{"node1": 42, "node2": 17, "node3": 255}},
		},
		{
			name: "Long NodeIDs",
			vc:   VectorClock{Counter: map[string]uint64{"very-long-node-id-1": 1, "another-long-node-id-2": 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized := tt.vc.Serialize()

			deserialized := &VectorClock{}
			err := deserialized.Deserialize(serialized)

			if err != nil {
				t.Errorf("Deserialize() error = %v", err)
				return
			}

			if !reflect.DeepEqual(tt.vc.Counter, deserialized.Counter) {
				t.Errorf("Deserialize() got = %v, want %v", deserialized.Counter, tt.vc.Counter)
			}
		})
	}
}

func TestDeserializeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Incomplete nodeID length",
			data: []byte{1, 2, 3}, // Less than 8 bytes
		},
		{
			name: "Incomplete nodeID",
			data: []byte{5, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3}, // NodeID length is 5, but only 3 bytes provided
		},
		{
			name: "Incomplete count",
			data: []byte{1, 0, 0, 0, 0, 0, 0, 0, 65, 1, 2, 3}, // 'A' for nodeID, incomplete count
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &VectorClock{}
			err := vc.Deserialize(tt.data)

			if err == nil {
				t.Error("Deserialize() expected error, got nil")
			}
		})
	}
}

func TestLargeVectorClock(t *testing.T) {
	// Create a large VectorClock
	largeVC := VectorClock{Counter: make(map[string]uint64)}
	for i := 0; i < 10000; i++ {
		largeVC.Counter[fmt.Sprintf("node-%d", i)] = uint64(i)
	}

	serialized := largeVC.Serialize()

	deserialized := &VectorClock{}
	err := deserialized.Deserialize(serialized)

	if err != nil {
		t.Errorf("Deserialize() error = %v", err)
		return
	}

	if !reflect.DeepEqual(largeVC.Counter, deserialized.Counter) {
		t.Errorf("Large VectorClock: Deserialize() result doesn't match original")
	}
}

func TestVectorClockIncrement(t *testing.T) {
	vc := NewVectorClock()

	vc.Increment("node1")
	if vc.Counter["node1"] != 1 {
		t.Errorf("After first increment, expected 1 for node1, got %d", vc.Counter["node1"])
	}

	vc.Increment("node1")
	if vc.Counter["node1"] != 2 {
		t.Errorf("After second increment, expected 2 for node1, got %d", vc.Counter["node1"])
	}

	vc.Increment("node2")
	if vc.Counter["node2"] != 1 {
		t.Errorf("After increment, expected 1 for node2, got %d", vc.Counter["node2"])
	}
}

func TestVectorClockMerge(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Increment("node1")
	vc1.Increment("node2")

	vc2 := NewVectorClock()
	vc2.Increment("node2")
	vc2.Increment("node3")

	vc1.Merge(vc2)

	expected := map[string]uint64{"node1": 1, "node2": 1, "node3": 1}
	if !reflect.DeepEqual(vc1.Counter, expected) {
		t.Errorf("After merge, expected %v, got %v", expected, vc1.Counter)
	}
}

func TestVectorClockCompare(t *testing.T) {
	tests := []struct {
		name     string
		vc1      VectorClock
		vc2      VectorClock
		expected int
	}{
		{
			name:     "Equal clocks",
			vc1:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 1}},
			vc2:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 1}},
			expected: 0,
		},
		{
			name:     "First clock is greater",
			vc1:      VectorClock{Counter: map[string]uint64{"node1": 2, "node2": 1}},
			vc2:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 1}},
			expected: 1,
		},
		{
			name:     "Second clock is greater",
			vc1:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 1}},
			vc2:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 2}},
			expected: -1,
		},
		{
			name:     "Concurrent updates",
			vc1:      VectorClock{Counter: map[string]uint64{"node1": 2, "node2": 1}},
			vc2:      VectorClock{Counter: map[string]uint64{"node1": 1, "node2": 2}},
			expected: 0,
		},
		{
			name:     "Different nodes",
			vc1:      VectorClock{Counter: map[string]uint64{"node1": 1}},
			vc2:      VectorClock{Counter: map[string]uint64{"node2": 1}},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.vc1.Compare(&tt.vc2)
			if result != tt.expected {
				t.Errorf("Compare() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestVectorClockConcurrency(t *testing.T) {
	vc := NewVectorClock()

	// Simulate concurrent operations
	go func() {
		for i := 0; i < 1000; i++ {
			vc.Increment("node1")
		}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			vc.Increment("node2")
		}
	}()

	time.Sleep(200 * time.Millisecond)

	if vc.Counter["node1"] != 1000 || vc.Counter["node2"] != 1000 {
		t.Errorf("Concurrent increments failed. node1: %d, node2: %d", vc.Counter["node1"], vc.Counter["node2"])
	}
}

func TestVectorClockCopy(t *testing.T) {
	original := NewVectorClock()
	original.Increment("node1")
	original.Increment("node2")

	copy := original.Copy()

	// Modify the copy
	copy.Increment("node1")

	if reflect.DeepEqual(original.Counter, copy.Counter) {
		t.Error("Copy() did not create a deep copy")
	}

	if original.Counter["node1"] != 1 || copy.Counter["node1"] != 2 {
		t.Errorf("Unexpected values after modifying copy. Original: %v, Copy: %v", original.Counter, copy.Counter)
	}
}

func TestVectorClockResolveConflicts(t *testing.T) {
	vc1 := NewVectorClock()
	vc1.Increment("node1")

	vc2 := NewVectorClock()
	vc2.Increment("node2")

	vc3 := NewVectorClock()
	vc3.Increment("node3")

	conflictSet := ConflictSet{
		VersionedValue{Value: []byte("value1"), Clock: vc1},
		VersionedValue{Value: []byte("value2"), Clock: vc2},
		VersionedValue{Value: []byte("value3"), Clock: vc3},
	}

	resolved, automatic := conflictSet.Resolve()

	if automatic {
		t.Error("Expected manual resolution required, got automatic")
	}

	if !reflect.DeepEqual(resolved, conflictSet[0]) {
		t.Errorf("Unexpected resolution. Got %v, want %v", resolved, conflictSet[0])
	}
}
