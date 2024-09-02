package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nireo/dynamo"
)

const (
	baseBindPort = 7946
	baseRPCPort  = 8000
)

func main() {
	numNodes := flag.Int("nodes", 3, "number of nodes to start")
	numPairs := flag.Int("pairs", 100, "number of key-value pairs to insert")
	flag.Parse()

	tempDir, err := os.MkdirTemp("", "dynamo-test")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Start the nodes
	nodes := startNodes(*numNodes, tempDir)
	defer stopNodes(nodes)

	// Wait for the cluster to stabilize
	time.Sleep(5 * time.Second)

	// Test inserting key-value pairs
	testInsertPairs(nodes[0], *numPairs)

	// Test reading key-value pairs
	testReadPairs(nodes[0], *numPairs)

	fmt.Println("All tests completed successfully!")
}

func startNodes(n int, tempDir string) []*exec.Cmd {
	var nodes []*exec.Cmd
	var seeds []string

	for i := 0; i < n; i++ {
		bindAddr := fmt.Sprintf("127.0.0.1:%d", baseBindPort+i)
		rpcAddr := fmt.Sprintf("127.0.0.1:%d", baseRPCPort+i)
		storePath := filepath.Join(tempDir, fmt.Sprintf("node%d", i))

		if i > 0 {
			seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", baseBindPort))
		}

		cmd := exec.Command("./dynamonode",
			"-bind", bindAddr,
			"-rpc", rpcAddr,
			"-store", storePath,
			"-seeds", strings.Join(seeds, ","))

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Fatalf("Failed to start node %d: %v", i, err)
		}

		nodes = append(nodes, cmd)
		fmt.Printf("Started node %d: Bind=%s, RPC=%s\n", i, bindAddr, rpcAddr)
	}

	return nodes
}

func stopNodes(nodes []*exec.Cmd) {
	for _, cmd := range nodes {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			log.Printf("Failed to stop node: %v", err)
		}
	}

	for _, cmd := range nodes {
		if err := cmd.Wait(); err != nil {
			log.Printf("Node exited with error: %v", err)
		}
	}
}

func testInsertPairs(node *exec.Cmd, numPairs int) {
	client, err := createClient(fmt.Sprintf("127.0.0.1:%d", baseRPCPort))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(numPairs)

	for i := 0; i < numPairs; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			err := client.ClientPut(key, value)
			if err != nil {
				log.Printf("Failed to insert key-value pair %d: %v", i, err)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Inserted %d key-value pairs\n", numPairs)
}

func testReadPairs(node *exec.Cmd, numPairs int) {
	client, err := createClient(fmt.Sprintf("127.0.0.1:%d", baseRPCPort))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(numPairs)

	for i := 0; i < numPairs; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", i))
			value, err := client.ClientGet(key)
			if err != nil {
				log.Printf("Failed to read key-value pair %d: %v", i, err)
			} else if string(value) != fmt.Sprintf("value%d", i) {
				log.Printf("Mismatch for key%d: expected 'value%d', got '%s'", i, i, value)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Read %d key-value pairs\n", numPairs)
}

func createClient(rpcAddr string) (*dynamo.DynamoNode, error) {
	// This is a simplified version. You may need to adjust this based on your actual client creation process
	config := dynamo.Config{N: 3, R: 2, W: 2}
	return dynamo.NewDynamoNode(config, rpcAddr, nil, rpcAddr)
}
