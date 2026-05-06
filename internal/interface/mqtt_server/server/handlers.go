package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/artsadert/artmq/internal/application/command"
	"github.com/artsadert/artmq/internal/domain/entities/message"
)

func (b *Broker) handleCONNECT(c *Client, payload []byte) error {
	r := bufio.NewReader(strings.NewReader(string(payload)))
	protoName, err := readUTF8String(r)
	if err != nil || protoName != "MQTT" {
		b.sendCONNACK(c, UnsupportedProtocolVersion)
		return errors.New("invalid protocol name")
	}
	protoLevel, err := r.ReadByte()
	if err != nil || protoLevel != 5 {
		b.sendCONNACK(c, UnsupportedProtocolVersion)
		return errors.New("unsupported protocol version")
	}
	connectFlags, err := r.ReadByte()
	if err != nil {
		b.sendCONNACK(c, MalformedPacket)
		return err
	}

	cleanStart := (connectFlags & 0x02) != 0

	keepalive, err := readUint16(r)
	if err != nil {
		b.sendCONNACK(c, MalformedPacket)
		return err
	}
	_ = keepalive

	// MQTT 5: variable header has a Properties section between Keep Alive and
	// the payload. We don't use any of the properties yet, so skip them.
	propLen, err := decodeVariableByteInteger(r)
	if err != nil {
		b.sendCONNACK(c, MalformedPacket)
		return err
	}
	if propLen > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(propLen)); err != nil {
			b.sendCONNACK(c, MalformedPacket)
			return err
		}
	}

	clientID, err := readUTF8String(r)
	if err != nil {
		b.sendCONNACK(c, ClientIdentifierNotValid)
		return err
	}

	// Empty client ID is allowed only with cleanStart=1; otherwise spec mandates
	// rejection. We assign a random ID in either case to avoid collisions.
	if clientID == "" {
		clientID = generateRandomID()
	}
	c.id = clientID

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.Clients[clientID]; exists {
		old := b.Clients[clientID]
		old.conn.Close()
		delete(b.Clients, clientID)
		b.removeClientSubsLocked(old)
	}

	c.id = clientID
	b.Clients[clientID] = c

	if cleanStart {
		b.removeClientSubsLocked(c)
	}

	b.sendCONNACK(c, Success)
	return nil
}

// removeClientSubsLocked drops every subscription owned by c. Caller holds b.mu.
func (b *Broker) removeClientSubsLocked(c *Client) {
	for topic, subs := range b.Subscriptions {
		newSubs := subs[:0:0]
		for _, sub := range subs {
			if sub.Client != c {
				newSubs = append(newSubs, sub)
			}
		}
		if len(newSubs) == 0 {
			delete(b.Subscriptions, topic)
		} else {
			b.Subscriptions[topic] = newSubs
		}
	}
}

func (b *Broker) sendCONNACK(c *Client, reasonCode byte) error {
	remaining := 2
	buf := []byte{byte(CONNACK << 4), byte(remaining)}
	buf = append(buf, reasonCode, 0x00)
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write(buf)
	return err
}

func (b *Broker) sendSUBACK(c *Client, packetID uint16, reasonCodes []byte) {
	varHeader := make([]byte, 0, 2+1)
	varHeader = append(varHeader, byte(packetID>>8), byte(packetID&0xFF))
	varHeader = append(varHeader, 0x00)

	fullPayload := append(varHeader, reasonCodes...)

	remBytes := encodeVarint(len(fullPayload))

	packet := []byte{byte(SUBACK << 4)}
	packet = append(packet, remBytes...)
	packet = append(packet, fullPayload...)

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write(packet); err != nil {
		log.Printf("Failed to send SUBACK: %v", err)
	}
}

func (b *Broker) handleSUBSCRIBE(c *Client, payload []byte) error {
	if len(payload) < 2 {
		return errors.New("malformed SUBSCRIBE: too short")
	}
	r := bufio.NewReader(strings.NewReader(string(payload)))

	var packetID uint16
	if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
		return err
	}

	propLen, err := decodeVariableByteInteger(r)
	if err != nil {
		return err
	}

	if propLen > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(propLen)); err != nil {
			return err
		}
	}

	var filters []string
	var qosLevels []byte

	for {
		filter, err := readUTF8String(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		options, err := r.ReadByte()
		if err != nil {
			return err
		}
		qos := options & 0x03
		if qos > 2 {
			qos = 2
		}
		filters = append(filters, filter)
		qosLevels = append(qosLevels, qos)
	}

	b.mu.Lock()
	for i, raw := range filters {
		qos := qosLevels[i]
		group, filter, ok := parseSubscriptionFilter(raw)
		if !ok {
			// Malformed $share/... filter — surface as Topic Filter invalid.
			qosLevels[i] = 0x8F
			continue
		}
		subs := b.Subscriptions[filter]
		replaced := false
		for j, existing := range subs {
			if existing.Client == c && existing.Group == group {
				subs[j].QoS = qos
				replaced = true
				break
			}
		}
		if !replaced {
			subs = append(subs, Subscription{Client: c, QoS: qos, Group: group})
		}
		b.Subscriptions[filter] = subs

		select {
		case b.notifyCh <- filter:
		default:
		}
	}
	b.mu.Unlock()

	b.sendSUBACK(c, packetID, qosLevels)
	return nil
}

// parseSubscriptionFilter splits an MQTT 5 shared-subscription filter into
// (group, filter). For a regular topic filter, group is "" and filter is the
// input unchanged. $share/<group>/<filter> → (<group>, <filter>).
// Per spec §4.8.2, ShareName must be non-empty and must not contain '/', '+',
// or '#'; the inner filter must be non-empty.
func parseSubscriptionFilter(raw string) (group, filter string, ok bool) {
	const prefix = "$share/"
	if !strings.HasPrefix(raw, prefix) {
		return "", raw, true
	}
	rest := raw[len(prefix):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", "", false
	}
	group = rest[:slash]
	filter = rest[slash+1:]
	if filter == "" || strings.ContainsAny(group, "+#") {
		return "", "", false
	}
	return group, filter, true
}

// handlePUBLISH processes incoming PUBLISH at QoS 0/1/2.
func (b *Broker) handlePUBLISH(c *Client, flags byte, payload []byte) error {
	retained := (flags & 0x01) != 0
	qos := (flags >> 1) & 0x03
	if qos > 2 {
		return errors.New("malformed PUBLISH: qos>2")
	}
	_ = retained

	r := bufio.NewReader(strings.NewReader(string(payload)))

	topic, err := readUTF8String(r)
	if err != nil {
		return err
	}

	var packetID uint16
	if qos > 0 {
		if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
			return err
		}
	}

	// MQTT 5 properties section
	propLen, err := decodeVariableByteInteger(r)
	if err != nil {
		return err
	}
	if propLen > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(propLen)); err != nil {
			return err
		}
	}

	payloadBytes, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// QoS 2: if we already saw this packet ID and replied PUBREC, do not redeliver
	// to the topic — just resend PUBREC.
	if qos == 2 {
		c.stateMu.Lock()
		_, dup := c.incomingQoS2[packetID]
		if !dup {
			c.incomingQoS2[packetID] = true
		}
		c.stateMu.Unlock()
		if dup {
			b.sendPUBREC(c, packetID)
			return nil
		}
	}

	res := b.msgService.PushMessage(&command.PushMessageCommand{
		TopicName: topic,
		Payload:   payloadBytes,
	})

	if res.Result.Error != "" {
		return fmt.Errorf("failed to push message: %v", res.Result.Error)
	}
	if res.Result.Message != nil {
		res.Result.Message.Qos = qos
	}

	switch qos {
	case 1:
		b.sendPUBACK(c, packetID)
	case 2:
		b.sendPUBREC(c, packetID)
	}

	select {
	case b.notifyCh <- topic:
	default:
	}

	return nil
}

func (b *Broker) handlePUBACK(c *Client, payload []byte) error {
	if len(payload) < 2 {
		return errors.New("malformed PUBACK")
	}
	packetID := uint16(payload[0])<<8 | uint16(payload[1])
	if entry := c.takeInflight(packetID); entry != nil {
		_ = entry
	}
	return nil
}

func (b *Broker) handlePUBREC(c *Client, payload []byte) error {
	if len(payload) < 2 {
		return errors.New("malformed PUBREC")
	}
	packetID := uint16(payload[0])<<8 | uint16(payload[1])
	if !c.updateInflight(packetID, awaitingPubComp) {
		return nil
	}
	b.sendPUBREL(c, packetID)
	return nil
}

func (b *Broker) handlePUBREL(c *Client, payload []byte) error {
	if len(payload) < 2 {
		return errors.New("malformed PUBREL")
	}
	packetID := uint16(payload[0])<<8 | uint16(payload[1])
	c.stateMu.Lock()
	delete(c.incomingQoS2, packetID)
	c.stateMu.Unlock()
	b.sendPUBCOMP(c, packetID)
	return nil
}

func (b *Broker) handlePUBCOMP(c *Client, payload []byte) error {
	if len(payload) < 2 {
		return errors.New("malformed PUBCOMP")
	}
	packetID := uint16(payload[0])<<8 | uint16(payload[1])
	if entry := c.takeInflight(packetID); entry != nil {
		_ = entry
	}
	return nil
}

// sendPublish sends a PUBLISH packet to the subscriber. For qos>=1 the caller is
// responsible for tracking the inflight entry (typically via Client.allocatePacketID).
func (b *Broker) sendPublish(c *Client, topic string, msgPayload []byte, qos byte, packetID uint16, retained bool) {
	flagsByte := byte(PUBLISH << 4)
	if retained {
		flagsByte |= 0x01
	}
	flagsByte |= (qos << 1) & 0x06

	var varHeader []byte
	varHeader = append(varHeader, byte(len(topic)>>8), byte(len(topic)&0xFF))
	varHeader = append(varHeader, []byte(topic)...)

	if qos > 0 {
		varHeader = append(varHeader, byte(packetID>>8), byte(packetID&0xFF))
	}

	// MQTT 5 properties: empty section (length 0).
	varHeader = append(varHeader, 0x00)

	fullPayload := append(varHeader, msgPayload...)
	remBuf := encodeVarint(len(fullPayload))

	packet := append([]byte{flagsByte}, remBuf...)
	packet = append(packet, fullPayload...)

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write(packet); err != nil {
		log.Printf("Failed to send PUBLISH to %s: %v", c.id, err)
	}
}

func (b *Broker) sendAckPacket(c *Client, packetType byte, packetID uint16) {
	// MQTT 5 PUBACK/PUBREC/PUBREL/PUBCOMP: packetID(2) + reasonCode(1) + propLen(1)
	flags := byte(0)
	if packetType == PUBREL {
		flags = 0x02 // PUBREL has reserved bits 0010
	}
	first := (packetType << 4) | flags

	body := []byte{
		byte(packetID >> 8), byte(packetID & 0xFF),
		0x00, // reason code = success
		0x00, // property length = 0
	}
	packet := append([]byte{first, byte(len(body))}, body...)

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write(packet); err != nil {
		log.Printf("Failed to send ack type %d to %s: %v", packetType, c.id, err)
	}
}

func (b *Broker) sendPUBACK(c *Client, packetID uint16) {
	b.sendAckPacket(c, PUBACK, packetID)
}

func (b *Broker) sendPUBREC(c *Client, packetID uint16) {
	b.sendAckPacket(c, PUBREC, packetID)
}

func (b *Broker) sendPUBREL(c *Client, packetID uint16) {
	b.sendAckPacket(c, PUBREL, packetID)
}

func (b *Broker) sendPUBCOMP(c *Client, packetID uint16) {
	b.sendAckPacket(c, PUBCOMP, packetID)
}

func (b *Broker) handlePINGREQ(c *Client) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write([]byte{byte(PINGRESP << 4), 0x00})
	return err
}

// handleDISCONNECT performs a clean disconnect: drops subscriptions and closes conn.
// In-flight cleanup is handled by handleConnectionLoss in the read loop's defer.
func (b *Broker) handleDISCONNECT(c *Client) {
	b.mu.Lock()
	delete(b.Clients, c.id)
	b.removeClientSubsLocked(c)
	b.mu.Unlock()
	c.conn.Close()
}

// handleConnectionLoss runs whenever the connection ends (clean or abrupt).
// It requeues in-flight QoS>=1 messages, escalating to DLQ if MaxAttempts exceeded.
func (b *Broker) handleConnectionLoss(c *Client) {
	if c.id != "" {
		b.mu.Lock()
		if b.Clients[c.id] == c {
			delete(b.Clients, c.id)
		}
		b.removeClientSubsLocked(c)
		b.mu.Unlock()
	}

	for _, entry := range c.drainInflight() {
		b.requeueOrDLQ(entry)
	}
}

func (b *Broker) requeueOrDLQ(entry *inflightMessage) {
	if entry == nil || entry.msg == nil {
		return
	}
	msg := entry.msg
	msg.Attempts++

	maxAttempts := msg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = message.DefaultMaxAttempts
	}

	if msg.Attempts >= maxAttempts {
		if err := b.msgService.DeadLetter(entry.origTopic, msg); err != nil {
			log.Printf("DLQ push failed for topic %s: %v", entry.origTopic, err)
		}
		return
	}

	if err := b.msgService.Requeue(msg); err != nil {
		log.Printf("requeue failed for topic %s: %v", entry.origTopic, err)
		return
	}
	select {
	case b.notifyCh <- entry.origTopic:
	default:
	}
}