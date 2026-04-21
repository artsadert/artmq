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
	_ = keepalive // можно хранить для PING

	clientID, err := readUTF8String(r)
	if err != nil {
		b.sendCONNACK(c, ClientIdentifierNotValid)
		return err
	}

	// Если clientID пустой и CleanStart=0 -> ошибка (для простоты требуем непустой)
	if clientID == "" && cleanStart {
		clientID = generateRandomID()
		// b.sendCONNACK(c, ClientIdentifierNotValid)
	}
	c.id = clientID

	b.mu.Lock()
	defer b.mu.Unlock()
	// Проверяем, не занят ли ID
	if _, exists := b.Clients[clientID]; exists {
		// закрываем старого клиента
		old := b.Clients[clientID]
		old.conn.Close()
		delete(b.Clients, clientID)
		// также удаляем подписки старого клиента (можно пройтись по Subscriptions)
		for topic, subs := range b.Subscriptions {
			newSubs := []*Client{}
			for _, subClient := range subs {
				if subClient != old {
					newSubs = append(newSubs, subClient)
				}
			}
			if len(newSubs) == 0 {
				delete(b.Subscriptions, topic)
			} else {
				b.Subscriptions[topic] = newSubs
			}
		}
	}

	c.id = clientID
	b.Clients[clientID] = c

	if cleanStart {
		// удаляем старые подписки этого клиента (если есть)
		for topic, subs := range b.Subscriptions {
			newSubs := []*Client{}
			for _, subClient := range subs {
				if subClient != c {
					newSubs = append(newSubs, subClient)
				}
			}
			if len(newSubs) == 0 {
				delete(b.Subscriptions, topic)
			} else {
				b.Subscriptions[topic] = newSubs
			}
		}
	}

	// Отправляем CONNACK с успехом
	b.sendCONNACK(c, Success)
	return nil
}

func (b *Broker) sendCONNACK(c *Client, reasonCode byte) error {
	// CONNACK fixed header: тип 2, флаги 0, remaining length 2 (property length 0 + reason code)
	// В MQTT 5.0 есть properties, для простоты property length = 0
	remaining := 2 // reason code (1) + property length (1, значение 0)
	buf := []byte{byte(CONNACK << 4), byte(remaining)}
	// variable header: property length (0), reason code
	buf = append(buf, reasonCode, 0x00) // 0x00 = property length = 0
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write(buf)
	return err
}

func (b *Broker) sendSUBACK(c *Client, packetID uint16, reasonCodes []byte) {
	// Build variable header: packetID (2 bytes) + property length (1 byte = 0)
	varHeader := make([]byte, 0, 2+1)
	varHeader = append(varHeader, byte(packetID>>8), byte(packetID&0xFF))
	varHeader = append(varHeader, 0x00) // property length = 0 (encoded as single byte 0x00)

	// Full payload = varHeader + reasonCodes
	fullPayload := append(varHeader, reasonCodes...)

	// Encode remaining length
	remainingLen := len(fullPayload)
	remBytes := encodeVarint(remainingLen)

	// Build full packet
	packet := []byte{byte(SUBACK << 4)} // 0x90
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

	// 1. Read packet identifier
	var packetID uint16
	if err := binary.Read(r, binary.BigEndian, &packetID); err != nil {
		return err
	}

	// 2. Read property length (variable byte integer)
	propLen, err := decodeVariableByteInteger(r)
	if err != nil {
		return err
	}

	// 3. Skip properties (we ignore them for simplicity)
	if propLen > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(propLen)); err != nil {
			return err
		}
	}

	// 4. Now read topic filters and options
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
		qos := options & 0x03 // lower two bits
		filters = append(filters, filter)
		qosLevels = append(qosLevels, qos)
	}

	// Store subscriptions (same as before)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, filter := range filters {
		if _, exists := b.Subscriptions[filter]; !exists {
			b.Subscriptions[filter] = []*Client{}
		}
		already := false
		for _, cl := range b.Subscriptions[filter] {
			if cl == c {
				already = true
				break
			}
		}
		if !already {
			b.Subscriptions[filter] = append(b.Subscriptions[filter], c)
			select {
			case b.notifyCh <- filter:
			default:
				// не блокируем если канал заполнен
			}
		}
	}

	// Send SUBACK
	b.sendSUBACK(c, packetID, qosLevels)
	return nil
}

// handlePUBLISH обрабатывает PUBLISH (только QoS 0)
func (b *Broker) handlePUBLISH(c *Client, flags byte, payload []byte) error {
	retained := (flags & 0x01) != 0
	qos := (flags >> 1) & 0x03

	if qos != 0 {
		return errors.New("qos not supported")
	}
	if retained {
		// пока игнорируем retained
	}

	r := bufio.NewReader(strings.NewReader(string(payload)))

	topic, err := readUTF8String(r)
	if err != nil {
		return err
	}

	payloadBytes, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	res := b.msgService.PushMessage(&command.PushMessageCommand{
		TopicName: topic,
		Payload:   payloadBytes,
	})

	if res.Result.Error != "" {
		return fmt.Errorf("failed to push message: %v", res.Result.Error)
	}

	select {
	case b.notifyCh <- topic:
	default:
		// не блокируем если канал заполнен
	}

	return nil
}

func (b *Broker) sendPublish(c *Client, topic string, message []byte, qos byte, retained bool) {
	// Формируем PUBLISH: fixed header (type=3, flags: qos<<1, retain)
	flagsByte := byte(PUBLISH << 4)
	if retained {
		flagsByte |= 0x01
	}
	flagsByte |= (qos << 1) & 0x06
	// variable header: topic name (UTF8)
	var varHeader []byte
	varHeader = append(varHeader, byte(len(topic)>>8), byte(len(topic)&0xFF))
	varHeader = append(varHeader, []byte(topic)...)
	// если qos >0, добавляем packetID (2 байта), но у нас qos=0
	// остальное - payload
	fullPayload := append(varHeader, message...)
	remaining := len(fullPayload)
	// encode remaining length
	remBuf := []byte{}
	rem := remaining
	for {
		digit := byte(rem % 128)
		rem /= 128
		if rem > 0 {
			digit |= 0x80
		}
		remBuf = append(remBuf, digit)
		if rem == 0 {
			break
		}
	}
	packet := append([]byte{flagsByte}, remBuf...)
	packet = append(packet, fullPayload...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.Write(packet)
}

// handlePINGREQ
func (b *Broker) handlePINGREQ(c *Client) error {
	// PINGRESP: fixed header тип 13, флаги 0, remaining length 0
	_, err := c.conn.Write([]byte{byte(PINGRESP << 4), 0x00})
	return err
}

// handleDISCONNECT
func (b *Broker) handleDISCONNECT(c *Client) {
	// Закрываем соединение, удаляем клиента и его подписки
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.Clients, c.id)
	for topic, subs := range b.Subscriptions {
		newSubs := []*Client{}
		for _, sub := range subs {
			if sub != c {
				newSubs = append(newSubs, sub)
			}
		}
		if len(newSubs) == 0 {
			delete(b.Subscriptions, topic)
		} else {
			b.Subscriptions[topic] = newSubs
		}
	}
	c.conn.Close()
}
