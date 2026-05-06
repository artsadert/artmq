package server

import (
	"bufio"
	"net"
	"sync"

	"github.com/artsadert/artmq/internal/domain/entities/message"
)

// inflightState tracks where a sent QoS>=1 message is in its handshake.
type inflightState int

const (
	awaitingPubAck  inflightState = iota // QoS 1: sent PUBLISH, expect PUBACK
	awaitingPubRec                       // QoS 2: sent PUBLISH, expect PUBREC
	awaitingPubComp                      // QoS 2: sent PUBREL, expect PUBCOMP
)

type inflightMessage struct {
	origTopic string
	msg       *message.Message
	state     inflightState
}

type Client struct {
	id     string
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex // serializes writes to conn

	// QoS state — guarded by stateMu, never held while writing to conn
	stateMu      sync.Mutex
	nextPacketID uint16
	inflight     map[uint16]*inflightMessage // sent PUBLISH (or PUBREL) awaiting ack
	incomingQoS2 map[uint16]bool             // received PUBLISH qos=2, replied PUBREC, awaiting PUBREL
}

// Subscription pairs a subscriber client with the maximum QoS it accepts.
// Group is empty for regular pub/sub (broadcast) subscriptions; non-empty for
// MQTT 5 shared subscriptions, where members of the same (Group, filter) bucket
// compete for each message.
type Subscription struct {
	Client *Client
	QoS    byte
	Group  string
}

func newClient(conn net.Conn) *Client {
	return &Client{
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writer:       bufio.NewWriter(conn),
		nextPacketID: 1,
		inflight:     make(map[uint16]*inflightMessage),
		incomingQoS2: make(map[uint16]bool),
	}
}

// allocatePacketID returns the next unused packet ID and stores the inflight
// entry under that ID atomically.
func (c *Client) allocatePacketID(entry *inflightMessage) uint16 {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for i := 0; i < 65535; i++ {
		id := c.nextPacketID
		c.nextPacketID++
		if c.nextPacketID == 0 {
			c.nextPacketID = 1
		}
		if _, busy := c.inflight[id]; !busy {
			c.inflight[id] = entry
			return id
		}
	}
	return 0
}

func (c *Client) takeInflight(packetID uint16) *inflightMessage {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	entry, ok := c.inflight[packetID]
	if !ok {
		return nil
	}
	delete(c.inflight, packetID)
	return entry
}

func (c *Client) updateInflight(packetID uint16, state inflightState) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	entry, ok := c.inflight[packetID]
	if !ok {
		return false
	}
	entry.state = state
	return true
}

// drainInflight removes and returns all in-flight messages. Used on disconnect.
func (c *Client) drainInflight() []*inflightMessage {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	out := make([]*inflightMessage, 0, len(c.inflight))
	for _, entry := range c.inflight {
		out = append(out, entry)
	}
	c.inflight = make(map[uint16]*inflightMessage)
	return out
}