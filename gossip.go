package dynamo

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type EventType string

var (
	EventTypeGossip    EventType = "gossip"
	EventTypeJoin      EventType = "join"
	EventTypeLeave     EventType = "leave"
	EventTypeHeartbeat EventType = "hearbeat"
)

type DataEntry struct {
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"value"`
}

type Event struct {
	Type     EventType            `json:"type"`
	Data     map[string]DataEntry `json:"data"`
	SenderID string
}

type Node struct {
	ID                string
	Addr              string
	Data              map[string]DataEntry
	Peers             map[string]string
	mutex             sync.RWMutex
	MaxPeers          int
	listener          net.Listener
	closeChan         chan struct{}
	wg                sync.WaitGroup
	lastHeartbeat     map[string]time.Time
	heartbeatInterval time.Duration
}

func NewNode(id, addr string, maxPeers int) *Node {
	return &Node{
		ID:                id,
		Addr:              addr,
		Data:              make(map[string]DataEntry),
		Peers:             make(map[string]string),
		MaxPeers:          maxPeers,
		closeChan:         make(chan struct{}),
		heartbeatInterval: 7 * time.Second,
		lastHeartbeat:     make(map[string]time.Time),
	}
}

func (n *Node) startHeartbeat() {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(n.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-n.closeChan:
				return
			case <-ticker.C:
			}
		}
	}()
}

func (n *Node) sendHeartbeats() {
	n.mutex.RLock()
	peers := make(map[string]string, len(n.Peers))
	for id, addr := range n.Peers {
		peers[id] = addr
	}
	n.mutex.RUnlock()

	for _, peerAddr := range peers {
		go n.sendHeartbeat(peerAddr)
	}
}

func (n *Node) checkHeartbeatFailures() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	now := time.Now()
	for id, last := range n.lastHeartbeat {
		if now.Sub(last) > 3*n.heartbeatInterval {
			log.Info().Msg("heartbeat has failed three times in a row")
			delete(n.Peers, id)
			delete(n.lastHeartbeat, id)
		}
	}
}

func (n *Node) listen() {
	defer n.wg.Done()
	for {
		select {
		case <-n.closeChan:
			return
		default:
			conn, err := n.listener.Accept()
			if err != nil {
				select {
				case <-n.closeChan:
					return
				default:
					log.Err(err).Msg("could not accept connection")
					continue
				}
			}

			go n.handleNodeConnection(conn)
		}
	}
}

func (n *Node) Close() error {
	close(n.closeChan)

	if n.listener != nil {
		err := n.listener.Close()
		if err != nil {
			return fmt.Errorf("error closing listener: %v", err)
		}
	}

	// wait for everything to close
	n.wg.Wait()
	return nil
}

func (n *Node) handleNodeConnection(conn net.Conn) {
	defer conn.Close()

	connDone := make(chan struct{})
	go func() {
		select {
		case <-n.closeChan:
			conn.Close()
		case <-connDone:
		}
	}()

	var event Event
	if err := json.NewDecoder(conn).Decode(&event); err != nil {
		log.Err(err)
		return
	}

	close(connDone)

	switch event.Type {
	case EventTypeGossip:
		n.handleGossipEvent(event.Data)
	case EventTypeJoin:
		n.handleJoin(event.Data)
	case EventTypeHeartbeat:
		n.handleHeartbeat(event.SenderID)
	}
}

func (n *Node) handleGossipEvent(data map[string]DataEntry) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	for k, v := range data {
		if existing, ok := n.Data[k]; !ok || existing.Timestamp < v.Timestamp {
			n.Data[k] = v
		}
	}
}

func (n *Node) handleJoin(data map[string]DataEntry) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	for id, d := range data {
		if id != n.ID && d.Value != n.Addr {
			n.AddPeer(id, d.Value)
		}
	}
}

func (n *Node) AddPeer(id, addr string) {
	if len(n.Peers) >= n.MaxPeers {
		// for looping the peers is pretty much random (not actually)
		for k := range n.Peers {
			delete(n.Peers, k)
			break
		}
	}

	n.Peers[id] = addr
}

func (n *Node) getRandomPeer() string {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	peers := make([]string, 0, len(n.Peers))
	for _, addr := range n.Peers {
		peers = append(peers, addr)
	}
	return peers[rand.Intn(len(peers))]
}

func (n *Node) AddData(key, value string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	currentTime := time.Now().UnixNano()
	n.Data[key] = DataEntry{Value: value, Timestamp: currentTime}
}

func (n *Node) handleHeartbeat(senderID string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.lastHeartbeat[senderID] = time.Now()
}

func (n *Node) sendHeartbeat(peerAddr string) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		log.Err(err).Str("address", peerAddr).Msg("could not connect to peer for heartbeat")
		return
	}
	defer conn.Close()

	heartbeatEvent := Event{
		Type:     EventTypeHeartbeat,
		Data:     make(map[string]DataEntry),
		SenderID: n.ID,
	}

	err = json.NewEncoder(conn).Encode(heartbeatEvent)
	if err != nil {
		log.Err(err).Msg("could not encode heartbeat message")
	}
}

func (n *Node) sendGossip(peerAddr string) {
	n.mutex.RLock()
	event := Event{
		Type: EventTypeHeartbeat,
		Data: n.Data,
	}
	n.mutex.RUnlock()

	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		log.Err(err).Msg("failed to contact node for gossip")
		return
	}
	defer conn.Close()

	err = json.NewEncoder(conn).Encode(event)
	if err != nil {
		log.Err(err)
	}
}

func (n *Node) gossip() {
	defer n.wg.Done()

	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.closeChan:
			return
		case <-ticker.C:
			if len(n.Peers) > 0 {
				peer := n.getRandomPeer()
				n.sendGossip(peer)
			}
		}
	}
}

func (n *Node) Start() error {
	var err error
	n.listener, err = net.Listen("tcp", n.Addr)
	if err != nil {
		log.Err(err)
		return err
	}

	n.wg.Add(3)
	go n.listen()
	go n.gossip()
	go n.startHeartbeat()

	return nil
}

func (n *Node) Join(bootstrapAddr string) error {
	conn, err := net.Dial("tcp", bootstrapAddr)
	if err != nil {
		log.Err(err).Msg("error dialing bootstrap")
		return err
	}
	defer conn.Close()

	event := Event{
		Type: EventTypeJoin,
		Data: map[string]DataEntry{n.ID: {Value: n.Addr, Timestamp: time.Now().Unix()}},
	}

	return json.NewEncoder(conn).Encode(event)
}
