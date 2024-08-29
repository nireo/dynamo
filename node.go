package dynamo

import (
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/serf/serf"
)

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
	Key   []byte
	Value []byte
}

type RPCReply struct {
	Value []byte
}

func NewDynamoNode(config Config, bindAddr string, seeds []string, rpcAddr string) (*DynamoNode, error) {
	cfg := consistent.Config{
		PartitionCount:    271,
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
	serfConfig.MemberlistConfig.BindAddr = bindAddr
	serfConfig.NodeName = bindAddr
	serfConfig.EventCh = node.events

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
	owner := n.consistent.LocateKey(args.Key)
	if owner.(MemberWrapper).Name == n.serfConf.NodeName {
		n.mu.RLock()
		defer n.mu.RUnlock()

		val, err := n.storage.Get(args.Key)
		if err != nil {
			return err
		}

		reply.Value = val
		return nil
	}

	return n.forward("DynamoNode.Get", args, reply, owner.(MemberWrapper))
}

func (n *DynamoNode) Put(args *RPCArgs, reply *RPCReply) error {
	owner := n.consistent.LocateKey(args.Key)

	if owner.(MemberWrapper).Name == n.serfConf.NodeName {
		n.mu.Lock()
		defer n.mu.Unlock()

		err := n.storage.Put(args.Key, args.Value)
		return err
	}

	return n.forward("DynamoNode.Put", args, reply, owner.(MemberWrapper))
}

func (n *DynamoNode) forward(method string, args *RPCArgs, reply *RPCReply, toSend MemberWrapper) error {
	client, err := rpc.Dial("tcp", toSend.Addr.String())
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
