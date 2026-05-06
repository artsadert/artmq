package server

import (
	"math/rand/v2"

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
		// Bucket matching subscriptions by routing kind:
		//   - regular (broadcast): every sub gets a copy, deduped per-client
		//     across overlapping filters with max QoS.
		//   - shared (MQTT 5): one sub per (group, filter) bucket gets the
		//     message; members compete.
		type groupKey struct{ group, filter string }
		seen := make(map[*Client]int)
		var regular []Subscription
		groups := make(map[groupKey][]Subscription)

		for filter, filterSubs := range b.Subscriptions {
			if !matchTopic(filter, topic) {
				continue
			}
			for _, s := range filterSubs {
				if s.Group != "" {
					k := groupKey{s.Group, filter}
					groups[k] = append(groups[k], s)
					continue
				}
				if idx, ok := seen[s.Client]; ok {
					if s.QoS > regular[idx].QoS {
						regular[idx].QoS = s.QoS
					}
					continue
				}
				seen[s.Client] = len(regular)
				regular = append(regular, s)
			}
		}
		b.mu.RUnlock()

		if len(regular) == 0 && len(groups) == 0 {
			return
		}

		res := b.msgService.PullMessage(&command.PullMessageCommand{
			TopicName: topic,
		})

		if res.Result.Error != "" || res.Result.Message == nil {
			return
		}

		msg := res.Result.Message
		if len(regular) > 0 {
			b.deliverToSubscribers(topic, msg, regular)
		}
		for _, members := range groups {
			pick := members[rand.IntN(len(members))]
			b.deliverToSubscribers(topic, msg, []Subscription{pick})
		}
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

