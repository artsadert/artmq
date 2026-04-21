package main

import (
	"github.com/artsadert/artmq/internal/application/services"
	"github.com/artsadert/artmq/internal/infrastructure/message/bolt"
	"github.com/artsadert/artmq/internal/interface/mqtt_server"
)

func main() {
	// repo := ram.NewRamRepository()
	repo, err := bolt.NewBoltRepository("storage/artmq.db")
	if err != nil {
		panic(err)
	}

	service := services.NewMessageService(repo)

	handler := mqtt_server.NewHandler(service, ":1883")

	err = handler.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
