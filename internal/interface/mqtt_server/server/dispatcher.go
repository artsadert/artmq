package server

import (
	"github.com/artsadert/artmq/internal/application/command"
	"github.com/artsadert/artmq/internal/domain/entities/message"
)

// StartDispatcher запускает event-driven доставку сообщений
func (b *Broker) StartDispatcher() {
	go func() {
		for topic := range b.notifyCh {
			b.dispatchTopic(topic)
		}
	}()
}

func (b *Broker) dispatchTopic(topic string) {
	for {
		b.mu.RLock()
		subs := append([]Subscription(nil), b.Subscriptions[topic]...)
		b.mu.RUnlock()

		if len(subs) == 0 {
			return
		}

		res := b.msgService.PullMessage(&command.PullMessageCommand{
			TopicName: topic,
		})

		if res.Result.Error != "" || res.Result.Message == nil {
			return
		}

		msg := res.Result.Message
		b.deliverToSubscribers(topic, msg, subs)
	}
}

func (b *Broker) deliverToSubscribers(topic string, msg *message.Message, subs []Subscription) {
	for _, sub := range subs {
		effectiveQoS := msg.Qos
		if sub.QoS < effectiveQoS {
			effectiveQoS = sub.QoS
		}

		if effectiveQoS == 0 {
			go b.sendPublish(sub.Client, topic, msg.Payload, 0, 0, false)
			continue
		}

		// QoS>=1: clone so each subscriber has independent attempt tracking,
		// then register inflight before sending so concurrent acks find the entry.
		entry := &inflightMessage{
			origTopic: topic,
			msg:       cloneMessage(msg),
			state:     awaitingPubAck,
		}
		if effectiveQoS == 2 {
			entry.state = awaitingPubRec
		}

		packetID := sub.Client.allocatePacketID(entry)
		if packetID == 0 {
			// no IDs available — fall back to QoS 0 to avoid losing the delivery
			go b.sendPublish(sub.Client, topic, msg.Payload, 0, 0, false)
			continue
		}

		client := sub.Client
		go b.sendPublish(client, topic, msg.Payload, effectiveQoS, packetID, false)
	}
}

func cloneMessage(src *message.Message) *message.Message {
	if src == nil {
		return nil
	}
	cp := *src
	if src.Exp != nil {
		expCopy := *src.Exp
		cp.Exp = &expCopy
	}
	if src.Payload != nil {
		cp.Payload = append([]byte(nil), src.Payload...)
	}
	return &cp
}