package server

import (
	"sync"

	"github.com/artsadert/artmq/internal/application/services"
)

// Broker holds all Clients and Subscriptions
type Broker struct {
	Clients       map[string]*Client
	Subscriptions map[string][]Subscription // topicFilter -> subscribers (with per-sub max QoS)
	msgService    *services.MessageService

	mu       sync.RWMutex
	notifyCh chan string
}

func NewBroker(msgService *services.MessageService) *Broker {
	return &Broker{
		Clients:       make(map[string]*Client),
		Subscriptions: make(map[string][]Subscription),
		msgService:    msgService,

		mu:       sync.RWMutex{},
		notifyCh: make(chan string, 100),
	}
}