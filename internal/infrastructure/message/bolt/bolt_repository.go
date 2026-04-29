package bolt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/artsadert/artmq/internal/domain/entities/message"
	"go.etcd.io/bbolt"
)

type BoltRepository struct {
	db *bbolt.DB
}

func NewBoltRepository(path string) (*BoltRepository, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	return &BoltRepository{db: db}, nil
}

func (r *BoltRepository) PushMessage(msg *message.Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.TopicName == "" {
		return fmt.Errorf("topic is required")
	}

	return r.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(msg.TopicName))
		if err != nil {
			return err
		}

		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		key := []byte(msg.Id.String())

		return bucket.Put(key, data)
	})
}

func (r *BoltRepository) PeekMessage(topic string) (*message.Message, error) {
	var best *message.Message

	err := r.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(topic))
		if b == nil {
			return fmt.Errorf("no messages")
		}

		return b.ForEach(func(_, v []byte) error {
			var msg message.Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return nil
			}

			if msg.Exp != nil && time.Now().Unix() > *msg.Exp {
				return nil
			}

			if best == nil || msg.Priority < best.Priority {
				best = &msg
			}

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	if best == nil {
		return nil, fmt.Errorf("no valid messages")
	}

	return best, nil
}

func (r *BoltRepository) PullMessage(topic string) (*message.Message, error) {
	var selected *message.Message
	var expiredMessages []*message.Message

	err := r.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(topic))
		if b == nil {
			return fmt.Errorf("no messages")
		}

		var bestKey []byte
		var expiredKeys [][]byte

		err := b.ForEach(func(k, v []byte) error {
			var msg message.Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return nil
			}

			if msg.Exp != nil && time.Now().Unix() > *msg.Exp {
				kCopy := make([]byte, len(k))
				copy(kCopy, k)
				expiredKeys = append(expiredKeys, kCopy)
				expiredCopy := msg
				expiredMessages = append(expiredMessages, &expiredCopy)
				return nil
			}

			if selected == nil || msg.Priority < selected.Priority {
				selected = &msg
				bestKey = k
			}

			return nil
		})
		if err != nil {
			return err
		}

		for _, k := range expiredKeys {
			_ = b.Delete(k)
		}

		if bestKey == nil {
			return fmt.Errorf("no valid messages")
		}

		return b.Delete(bestKey)
	})

	// Route expired messages to DLQ in a separate transaction so a DLQ write
	// failure does not roll back the pull.
	for _, em := range expiredMessages {
		if dlqErr := r.PushToDLQ(topic, em); dlqErr != nil {
			// best-effort: do not block primary pull on DLQ failure
			_ = dlqErr
		}
	}

	if err != nil {
		return nil, err
	}

	return selected, nil
}

func (r *BoltRepository) IsEmpty(topic string) (bool, error) {
	empty := true

	err := r.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(topic))
		if b == nil {
			return nil
		}

		return b.ForEach(func(_, _ []byte) error {
			empty = false
			return fmt.Errorf("stop")
		})
	})

	if err != nil && err.Error() != "stop" {
		return false, err
	}

	return empty, nil
}

func (r *BoltRepository) PushToDLQ(origTopic string, msg *message.Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	dlqTopic := message.DLQTopic(origTopic)
	dlqMsg := *msg
	dlqMsg.TopicName = dlqTopic
	dlqMsg.Exp = nil // dead-lettered messages do not expire

	return r.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(dlqTopic))
		if err != nil {
			return err
		}

		data, err := json.Marshal(&dlqMsg)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(dlqMsg.Id.String()), data)
	})
}