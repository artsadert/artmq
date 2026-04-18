package mqtt_server

import (
	"log"
	"net"
	"net/http"

	"github.com/artsadert/artmq/internal/application/services"
	"github.com/artsadert/artmq/internal/interface/mqtt_server/server"
)

type Handler struct {
	msgService *services.MessageService
	Port       string
}

func NewHandler(msgService *services.MessageService, port string) *Handler {
	return &Handler{
		msgService: msgService,
		Port:       port,
	}
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) ListenAndServe() error {
	broker := server.NewBroker(h.msgService)

	listener, err := net.Listen("tcp", h.Port)
	if err != nil {
		return err
	}

	defer listener.Close()
	log.Println("mqtt 5.0 broker started on :1883")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go func() {
			err := broker.HandleConnection(conn)
			if err != nil {
				log.Println(err)
			}
		}()
	}
}
