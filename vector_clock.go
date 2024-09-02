package dynamo

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
)

// A simple implementation of a vector clock. TODO: implement compression

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

// Compare compares this VectorClock with another vector clock
// -1 if this clock is less than the other
// 0 if they are qual or concurrent
// 1 if this clock is greater than the other
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

// Serialize convers the VectorClock to a byte slice.
// Format is: [length of nodeID (8 bytes)][nodeID][count (8 bytes)] for each entry
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

// Deserialize reads a given byte array and creates and fill the VectorClock
// from that.
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

// Copy creates a deep copy of a given VectorClock
func (vc *VectorClock) Copy() *VectorClock {
	newVC := NewVectorClock()
	for k, v := range vc.Counter {
		newVC.Counter[k] = v
	}
	return newVC
}

// Versionedvalue represents a value with its associated VectorClock
type VersionedValue struct {
	Value []byte
	Clock *VectorClock
}

// ConflictSet is a slice of Versionedvalues, representing conflicting versions.
type ConflictSet []VersionedValue

// Resolve attemps to resolve conflicts in the ConflictSet
// It returns the resolved VersionValue and a boolean indicating if resolution was succesful.
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
