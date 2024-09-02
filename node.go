package dynamo

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/serf/serf"
)

// ErrNotEnoughReplicas happens when where trying to either will write or read quorums
// and the amount of nodes is not enough to fullfill the configuration provided to the
// node.
var ErrNotEnoughReplicas = errors.New("not enough replicas for parameters")

// Member wraps serf.Member to implement consistent.Member interface
type Member struct {
	serf.Member
}

// String returns the name of the member.
func (m Member) String() string {
	return m.Name
}

// hasher implements the interface that is required by the buraksezer/consistent interface.
// We choose xxhash in this implementation since it's one of the fastest hashing functions
// implemented for Go.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// Config represents the configuration parameters for Amazon DynamoDB's consistency model.
// These settings allow for fine-tuning the trade-offs between consistency, availability,
// and partition tolerance (CAP theorem) in a distributed database system.
//
// DynamoDB uses these parameters to manage its replication strategy and determine
// the behavior of read and write operations across multiple nodes. By adjusting
// these values, users can optimize their DynamoDB setup for either stronger
// consistency guarantees or higher availability, depending on their specific
// application requirements.
//
// The relationship between N, R, and W influences the system's behavior:
// - If R + W > N: The system favors strong consistency over availability.
// - If R + W <= N: The system may return stale data in some scenarios, favoring availability.
//
// Typical DynamoDB deployments often use N=3 as a default replication factor.
type Config struct {
	N int // The number of nodes that store replicas of data
	R int // The number of nodes that must participate in a successful read operation
	W int // The number of nodes that must participate in a successful write operation
}

// RpcClient defines the basic methods needed for RPC communication. This is used instead of
// rpc.Client since this can be mocked easily and we can write tests easier.
type RpcClient interface {
	Call(serviceMethod string, args interface{}, reply interface{}) error
	Close() error
}

// DynamoNode represents a physical node in the dynamo system.
type DynamoNode struct {
	conf           Config
	serf           *serf.Serf
	serfConf       *serf.Config
	events         chan serf.Event
	consistent     *consistent.Consistent
	mu             sync.RWMutex
	rpcServer      *rpc.Server
	rpcListener    net.Listener
	Storage        StorageEngine
	rpcConnections map[string]RpcClient
}

type RPCArgs struct {
	Key   []byte
	Value []byte
	Clock *VectorClock
}

type RPCReply struct {
	Value []byte
	Clock *VectorClock
}

func NewDynamoNode(config Config, bindAddr string, seeds []string, rpcAddr string) (*DynamoNode, error) {
	partitionCount := 271
	if len(seeds) == 0 {
		partitionCount = 10
	}

	log.Printf("setting up node's serf on addr %s and rpc on addr %s", bindAddr, rpcAddr)
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string)
	tags["rpc_addr"] = rpcAddr

	cfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: config.N,
		Load:              1.25,
		Hasher:            hasher{},
	}

	c := consistent.New(nil, cfg)

	node := &DynamoNode{
		conf:       config,
		consistent: c,
		events:     make(chan serf.Event),
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	serfConfig.NodeName = bindAddr
	serfConfig.EventCh = node.events
	serfConfig.Tags = tags

	s, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	node.serf = s
	node.serfConf = serfConfig

	node.rpcServer = rpc.NewServer()
	err = node.rpcServer.Register(node)
	if err != nil {
		return nil, err
	}

	node.rpcListener, err = net.Listen("tcp", rpcAddr)
	if err != nil {
		return nil, err
	}

	_, err = s.Join(seeds, true)
	if err != nil {
		return nil, err
	}

	node.consistent.Add(Member{s.LocalMember()})

	go node.rpcServer.Accept(node.rpcListener)
	go node.eventHandler()

	return node, nil
}

func (n *DynamoNode) eventHandler() {
	for e := range n.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			n.handleMemberJoin(e.(serf.MemberEvent))
		case serf.EventMemberFailed:
			n.handleMemberLeave(e.(serf.MemberEvent))
		}
	}
}

func (n *DynamoNode) handleMemberJoin(event serf.MemberEvent) {
	for _, member := range event.Members {
		log.Printf("member joined: %s", member.Name)
		n.consistent.Add(Member{member})
	}
}

func (n *DynamoNode) handleMemberLeave(event serf.MemberEvent) {
	for _, member := range event.Members {
		log.Printf("member left or failed: %s", member.Name)
		n.consistent.Remove(member.Name)

		// Even though we don't automatically form the rpc connection we should still clean it up here.
		addrToClean, err := getResolvedAddr(member.Tags["rpc_addr"])
		if err != nil {
			log.Printf("error resolving the address for addr %s: %s", member.Tags["rpc_addr"], err)
			continue
		}

		delete(n.rpcConnections, addrToClean)
	}
}

func (n *DynamoNode) Get(args *RPCArgs, reply *RPCReply) error {
	closest, err := n.consistent.GetClosestN(args.Key, n.conf.N)
	if err != nil {
		return err
	}

	successfulReads := 0
	var latestClock *VectorClock
	var latestValue []byte

	for _, node := range closest {
		if successfulReads >= n.conf.R {
			break
		}

		serfNode := node.(Member)
		if serfNode.Name == n.serfConf.NodeName {
			// Should be read from local since it's this node
			val, clock, err := n.Storage.GetVersioned(args.Key)
			if err != nil {
				log.Printf("error reading key from local node: %s", err)
				continue
			}

			successfulReads++
			if latestClock == nil || clock.Compare(latestClock) > 0 {
				latestClock = clock
				latestValue = val
			}
			continue
		}

		client, err := n.findClient(serfNode.Tags["rpc_addr"])
		if err != nil {
			log.Printf("failed to find client for addr %s, got err: %s", serfNode.Tags["rpc_addr"], err)
			continue
		}

		var rpcreply RPCReply
		err = client.Call("DynamoNode.GetLocal", args, &rpcreply)
		if err != nil {
			log.Printf("error reading from member node %s due to error: %s", serfNode.Name, err)
			continue
		}

		successfulReads++
		if latestClock == nil || rpcreply.Clock.Compare(latestClock) > 0 {
			latestValue = rpcreply.Value
			latestClock = rpcreply.Clock
		}
	}

	// Could not fill the read quorum
	if successfulReads < n.conf.R {
		return ErrNotEnoughReplicas
	}

	reply.Clock = latestClock
	reply.Value = latestValue
	return nil
}

// Put stores a key-value pair, implementing a quorum.
// 1. Generate a new version number based on the current timestamp.
// 2. Find the N closest nodes that should store the data (based on consistent hashing)
// 3. Attempt to write to these nodes in parallel.
// 4. If the write quorum is reached, consider the operation succesful.
func (n *DynamoNode) Put(args *RPCArgs, reply *RPCReply) error {
	closest, err := n.consistent.GetClosestN(args.Key, n.conf.N)
	if err != nil {
		return err
	}

	successfulWrites := 0
	newClock := NewVectorClock()
	newClock.Increment(n.serfConf.NodeName)

	if args.Clock != nil {
		newClock.Merge(args.Clock)
	}

	// Attempt to write to nodes until we reach the write quorum or exhaust all nodes.
	for _, node := range closest {
		// Write quorum is reached
		if successfulWrites >= n.conf.W {
			break
		}

		serfNode := node.(Member)
		if serfNode.Name == n.serfConf.NodeName {
			// Write to the local storage since it's this node.
			err := n.Storage.PutVersioned(args.Key, args.Value, newClock)
			if err != nil {
				log.Printf("error putting key-value pair into local storage: %s", err)
				continue
			}
			successfulWrites++
			continue
		}

		// Need to write some other node that is not this node.
		client, err := n.findClient(serfNode.Tags["rpc_addr"])
		if err != nil {
			log.Printf("error dialing node %s due to error: %s", serfNode.Name, err)
			continue
		}

		var rpcreply RPCReply
		err = client.Call("DynamoNode.PutLocal", &RPCArgs{Key: args.Key, Value: args.Value, Clock: newClock}, &rpcreply)
		if err != nil {
			log.Printf("error calling node %s put local: %s", serfNode.Name, err)
			continue
		}

		successfulWrites++
	}

	// A put operation is only successful when the wanted write quorum is reached.
	if successfulWrites < n.conf.W {
		return ErrNotEnoughReplicas
	}

	reply.Clock = newClock
	return nil
}

func (n *DynamoNode) PutLocal(args *RPCArgs, reply *RPCReply) error {
	return n.Storage.PutVersioned(args.Key, args.Value, args.Clock)
}

func (n *DynamoNode) GetLocal(args *RPCArgs, reply *RPCReply) error {
	value, clock, err := n.Storage.GetVersioned(args.Key)
	if err != nil {
		return err
	}

	reply.Value = value
	reply.Clock = clock
	return nil
}

func getResolvedAddr(rpcAddr string) (string, error) {
	// Since string matching is quite bad in terms of addresses resolve the addr and use the ip and port.
	addr, err := net.ResolveTCPAddr("tcp", rpcAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", addr.IP.String(), addr.Port), nil
}

// findClient finds a given client from a list of rpcClients if it doesn't find that it tries
// connection to the correct node.
func (n *DynamoNode) findClient(rpcAddr string) (RpcClient, error) {
	var err error
	rpcAddr, err = getResolvedAddr(rpcAddr)
	if err != nil {
		return nil, err
	}

	client, ok := n.rpcConnections[rpcAddr]
	if ok {
		return client, nil
	}

	client, err = rpc.Dial("tcp", rpcAddr)
	if err != nil {
		return nil, err
	}

	n.rpcConnections[rpcAddr] = client
	return client, nil
}

func (n *DynamoNode) Close() error {
	if err := n.serf.Leave(); err != nil {
		return err
	}

	for _, client := range n.rpcConnections {
		if err := client.Close(); err != nil {
			return err
		}
	}

	return n.rpcListener.Close()
}

func (n *DynamoNode) ClientGet(key []byte) ([]byte, error) {
	args := &RPCArgs{
		Key: key,
	}
	reply := &RPCReply{}

	err := n.Get(args, reply)
	if err != nil {
		return nil, err
	}

	return reply.Value, nil
}

func (n *DynamoNode) ClientPut(key, value []byte) error {
	args := &RPCArgs{Key: key, Value: value}
	reply := &RPCReply{}

	err := n.Put(args, reply)
	if err != nil {
		return err
	}

	return nil
}
