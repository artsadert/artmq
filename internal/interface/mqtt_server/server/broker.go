package server

import "sync"

// Broker holds all Clients and Subscriptions
type Broker struct {
	Clients       map[string]*Client
	Subscriptions map[string][]*Client // topicFilter -> []*Client
	mu            sync.RWMutex
}
