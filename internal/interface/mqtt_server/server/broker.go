package server

import (
	"sync"

	"github.com/artsadert/artmq/internal/application/services"
)

// Broker holds all Clients and Subscriptions
type Broker struct {
	Clients       map[string]*Client
	Subscriptions map[string][]*Client // topicFilter -> []*Client
	msgService    *services.MessageService
	mu            sync.RWMutex
}

func NewBroker(msgService *services.MessageService) *Broker {
	return &Broker{
		Clients:       make(map[string]*Client),
		Subscriptions: make(map[string][]*Client),
		msgService:    msgService,
		mu:            sync.RWMutex{},
	}
}
