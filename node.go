package dynamo

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/serf/serf"
)

// ErrNotEnoughReplicas happens when where trying to either will write or read quorums
// and the amount of nodes is not enough to fullfill the configuration provided to the
// node.
var ErrNotEnoughReplicas = errors.New("not enough replicas for parameters")

// MemberWrapper wraps serf.Member to implement consistent.Member interface
type MemberWrapper struct {
	serf.Member
}

func (m MemberWrapper) String() string {
	return m.Name
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type Config struct {
	N int
	R int
	W int
}

type DynamoNode struct {
	conf        Config
	serf        *serf.Serf
	serfConf    *serf.Config
	events      chan serf.Event
	consistent  *consistent.Consistent
	data        map[string]string // TODO: store data on disk
	mu          sync.RWMutex
	rpcServer   *rpc.Server
	rpcListener net.Listener
	storage     StorageEngine
}

type RPCArgs struct {
	Key     []byte
	Value   []byte
	Version uint64
}

type RPCReply struct {
	Value   []byte
	Version uint64
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
		data:       make(map[string]string),
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

	node.consistent.Add(MemberWrapper{s.LocalMember()})

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
		n.consistent.Add(MemberWrapper{member})
	}
}

func (n *DynamoNode) handleMemberLeave(event serf.MemberEvent) {
	for _, member := range event.Members {
		log.Printf("member left or failed: %s", member.Name)
		n.consistent.Remove(member.Name)
	}
}

func (n *DynamoNode) Get(args *RPCArgs, reply *RPCReply) error {
	closest, err := n.consistent.GetClosestN(args.Key, n.conf.N)
	if err != nil {
		return err
	}

	successfulReads := 0
	var latestVersion uint64
	var latestValue []byte

	for _, node := range closest {
		if successfulReads >= n.conf.R {
			break
		}

		serfNode := node.(MemberWrapper)
		if serfNode.Name == n.serfConf.NodeName {
			// Should be read from local since it's this node
			val, version, err := n.storage.GetVersioned(args.Key)
			if err != nil {
				log.Printf("error reading key from local node: %s", err)
				continue
			}

			successfulReads++
			if version > latestVersion {
				latestValue = val
				latestVersion = version
			}
			continue
		}

		client, err := rpc.Dial("tcp", serfNode.Tags["rpc_addr"])
		if err != nil {
			continue
		}
		defer client.Close()

		var rpcreply RPCReply

		err = client.Call("DynamoNode.GetLocal", args, &rpcreply)
		if err != nil {
			log.Printf("error reading from member node %s due to error: %s", serfNode.Name, err)
			continue
		}

		successfulReads++
		if rpcreply.Version > latestVersion {
			latestValue = rpcreply.Value
			latestVersion = rpcreply.Version
		}
	}

	// Could not fill the read quorum
	if successfulReads < n.conf.R {
		return ErrNotEnoughReplicas
	}

	reply.Version = latestVersion
	reply.Value = latestValue
	return nil
}

func (n *DynamoNode) Put(args *RPCArgs, reply *RPCReply) error {
	closest, err := n.consistent.GetClosestN(args.Key, n.conf.N)
	if err != nil {
		return err
	}

	successfulWrites := 0
	newVersion := uint64(time.Now().UnixNano())

	for _, node := range closest {
		// Write quorum is reached
		if successfulWrites >= n.conf.W {
			break
		}

		serfNode := node.(MemberWrapper)

		if serfNode.Name == n.serfConf.NodeName {
			err := n.storage.PutVersioned(args.Key, args.Value, newVersion)
			if err != nil {
				log.Printf("error putting key-value pair into local storage: %s", err)
				continue
			}
			successfulWrites++
			continue
		}

		client, err := rpc.Dial("tcp", serfNode.Tags["rpc_addr"])
		if err != nil {
			log.Printf("error dialing node %s due to error: %s", serfNode.Name, err)
			continue
		}

		var rpcreply RPCReply
		err = client.Call("DynamoNode.PutLocal", &RPCArgs{Key: args.Key, Value: args.Value, Version: newVersion}, &rpcreply)
		if err != nil {
			log.Printf("error calling node %s put local: %s", serfNode.Name, err)
			continue
		}

		successfulWrites++
	}

	if successfulWrites < n.conf.W {
		return ErrNotEnoughReplicas
	}

	reply.Version = newVersion
	return nil
}

func (n *DynamoNode) PutLocal(args *RPCArgs, reply *RPCReply) error {
	return n.storage.PutVersioned(args.Key, args.Value, args.Version)
}

func (n *DynamoNode) GetLocal(args *RPCArgs, reply *RPCReply) error {
	value, version, err := n.storage.GetVersioned(args.Key)
	if err != nil {
		return err
	}

	reply.Value = value
	reply.Version = version
	return nil
}

func (n *DynamoNode) forward(method string, args *RPCArgs, reply *RPCReply, toSend MemberWrapper) error {
	client, err := rpc.Dial("tcp", toSend.Tags["rpc_addr"])
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call(method, args, reply)
}

func (n *DynamoNode) Close() error {
	if err := n.serf.Leave(); err != nil {
		return err
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
