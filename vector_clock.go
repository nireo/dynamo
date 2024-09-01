package dynamo

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
)

// VectorClock represents the vector clock for a particular value.
type VectorClock struct {
	Counter map[string]uint64
	mu      sync.RWMutex
}

// NewVectorClock creates a new vector clock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		Counter: make(map[string]uint64),
	}
}

// Increment increases the counter for the given node.
func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.Counter[nodeID]++
}

// Merge combines two VectorClocks to be equal.
func (vc *VectorClock) Merge(other *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	for nodeID, count := range other.Counter {
		if vc.Counter[nodeID] < count {
			vc.Counter[nodeID] = count
		}
	}
}

func (vc *VectorClock) Compare(other *VectorClock) int {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	less, greater := false, false
	for nodeID, count := range vc.Counter {
		otherCount := other.Counter[nodeID]
		if count < otherCount {
			less = true
		} else if count > otherCount {
			greater = true
		}
		if less && greater {
			return 0 // Concurrent
		}
	}

	for nodeID, otherCount := range other.Counter {
		if _, exists := vc.Counter[nodeID]; !exists {
			if otherCount > 0 {
				less = true
			}
		}
	}

	if less && !greater {
		return -1 // This is less than other
	} else if !less && greater {
		return 1 // This is greater than other
	}
	return 0 // Equal or concurrent
}

func (vc *VectorClock) Serialize() []byte {
	totalSize := 0
	for nodeID, _ := range vc.Counter {
		totalSize += 8 + len(nodeID) + 8
	}

	res := make([]byte, totalSize)
	offset := 0

	for nodeID, count := range vc.Counter {
		binary.LittleEndian.PutUint64(res[offset:], uint64(len(nodeID)))
		offset += 8

		copy(res[offset:], nodeID)
		offset += len(nodeID)

		binary.LittleEndian.PutUint64(res[offset:], count)
		offset += 8
	}

	return res
}

func (vc *VectorClock) Deserialize(data []byte) error {
	vc.Counter = make(map[string]uint64)
	reader := bytes.NewReader(data)

	for reader.Len() > 0 {
		var nodeIDLen uint64
		if err := binary.Read(reader, binary.LittleEndian, &nodeIDLen); err != nil {
			return err
		}

		nodeID := make([]byte, nodeIDLen)
		if _, err := reader.Read(nodeID); err != nil {
			return err
		}

		var count uint64
		if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
			return err
		}

		vc.Counter[string(nodeID)] = count
	}

	return nil
}

func (vc *VectorClock) Copy() *VectorClock {
	newVC := NewVectorClock()
	for k, v := range vc.Counter {
		newVC.Counter[k] = v
	}
	return newVC
}

type VersionedValue struct {
	Value []byte
	Clock *VectorClock
}

type ConflictSet []VersionedValue

func (cs ConflictSet) Resolve() (VersionedValue, bool) {
	// base cases
	if len(cs) == 0 {
		return VersionedValue{}, false
	}

	if len(cs) == 1 {
		return cs[0], true
	}

	sort.Slice(cs, func(i, j int) bool {
		return cs[i].Clock.Compare(cs[j].Clock) > 0
	})

	if cs[0].Clock.Compare(cs[1].Clock) > 1 {
		return cs[0], true
	}

	// Manual resolution is needed
	return cs[0], false
}
