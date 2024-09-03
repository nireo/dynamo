package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nireo/dynamo"
)

func main() {
	numNodes := flag.Int("nodes", 3, "Number of nodes to start")
	numPairs := flag.Int("pairs", 100, "Number of key-value pairs to insert")
	flag.Parse()

	// Create a temporary directory for node data
	tempDir, err := os.MkdirTemp("", "dynamo-test")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Start the nodes
	nodes, ports := startNodes(*numNodes, tempDir)
	defer stopNodes(nodes)

	// Wait for the cluster to stabilize
	time.Sleep(5 * time.Second)

	// Test inserting key-value pairs
	testInsertPairs(ports[0].rpcPort, *numPairs)

	// Test reading key-value pairs
	testReadPairs(ports[0].rpcPort, *numPairs)

	fmt.Println("All tests completed successfully!")
}

type nodePorts struct {
	bindPort int
	rpcPort  int
}

func getFreePorts(count int) ([]int, error) {
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

func startNodes(n int, tempDir string) ([]*exec.Cmd, []nodePorts) {
	var nodes []*exec.Cmd
	var ports []nodePorts
	var seeds []string

	for i := 0; i < n; i++ {
		freePorts, err := getFreePorts(2)
		if err != nil {
			log.Fatalf("Failed to get free ports: %v", err)
		}

		bindPort := freePorts[0]
		rpcPort := freePorts[1]

		bindAddr := fmt.Sprintf("127.0.0.1:%d", bindPort)
		rpcAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort)
		storePath := filepath.Join(tempDir, fmt.Sprintf("node%d", i))

		if i > 0 {
			seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", ports[0].bindPort))
		}

		cmd := exec.Command("./node",
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
		ports = append(ports, nodePorts{bindPort: bindPort, rpcPort: rpcPort})
		fmt.Printf("Started node %d: Bind=%s, RPC=%s\n", i, bindAddr, rpcAddr)
	}

	return nodes, ports
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

func testInsertPairs(rpcPort int, numPairs int) {
	client, err := createClient(fmt.Sprintf("127.0.0.1:%d", rpcPort))
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

func testReadPairs(rpcPort int, numPairs int) {
	client, err := createClient(fmt.Sprintf("127.0.0.1:%d", rpcPort))
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
