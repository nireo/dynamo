package dynamo

import (
	"fmt"
	"math"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	m := NewConsistentHashMap(256, nil) // 100 virtual nodes per physical node

	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	m.Add(nodes...)

	numKeys := 1000000
	distribution := make(map[string]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		node := m.Get(key)
		distribution[node]++
	}

	// Ensure that the distribution is somewhat even.
	expectedDistributionPerNode := numKeys / len(nodes)
	tolerance := 0.13

	for node, count := range distribution {
		deviation := math.Abs(float64(count-expectedDistributionPerNode)) / float64(expectedDistributionPerNode)
		if deviation > tolerance {
			t.Errorf("Uneven distribution for node %s: got %d keys, expected %d (+-%.2f%%)",
				node, count, expectedDistributionPerNode, tolerance*100)
		}
	}
}

func TestConsistentHashGetN(t *testing.T) {
	hash := NewConsistentHashMap(100, nil)
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	hash.Add(nodes...)

	key := "testkey"
	n := 3
	selectedNodes := hash.GetN(key, n)

	if len(selectedNodes) != n {
		t.Errorf("Expected %d nodes, got %d", n, len(selectedNodes))
	}

	uniqueNodes := make(map[string]bool)
	for _, node := range selectedNodes {
		if uniqueNodes[node] {
			t.Errorf("Duplicate node selected: %s", node)
		}
		uniqueNodes[node] = true
	}
}

func TestConsistentHashStability(t *testing.T) {
	hash := NewConsistentHashMap(100, nil)
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	hash.Add(nodes...)

	numKeys := 10000
	initialAssignments := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", i)
		initialAssignments[key] = hash.Get(key)
	}

	hash.Remove("node3")

	reassignments := 0
	for key, initialNode := range initialAssignments {
		newNode := hash.Get(key)
		if newNode != initialNode {
			reassignments++
		}
	}

	expectedReassignments := numKeys / 5
	tolerance := 0.05

	reassignmentRate := float64(reassignments) / float64(numKeys)
	expectedRate := float64(expectedReassignments) / float64(numKeys)

	if math.Abs(reassignmentRate-expectedRate) > tolerance {
		t.Errorf("Unexpected number of reassignments after node removal. Got %.2f%%, expected %.2f%% (+-%.2f%%)",
			reassignmentRate*100, expectedRate*100, tolerance*100)
	}
}
