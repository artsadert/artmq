package main

import "github.com/artsadert/artmq/internal/interface/mqtt_server"

func main() {
	handler := mqtt_server.Handler{Port: ":1883"}

	err := handler.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
