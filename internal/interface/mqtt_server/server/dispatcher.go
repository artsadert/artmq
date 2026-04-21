package server

import (
	"github.com/artsadert/artmq/internal/application/command"
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
		subscribers := b.Subscriptions[topic]
		b.mu.RUnlock()

		if len(subscribers) == 0 {
			return
		}

		res := b.msgService.PullMessage(&command.PullMessageCommand{
			TopicName: topic,
		})

		if res.Result.Error != "" || res.Result.Message == nil {
			return
		}

		msg := res.Result.Message

		for _, sub := range subscribers {
			go b.sendPublish(sub, topic, msg.Payload, 0, false)
		}
	}
}
