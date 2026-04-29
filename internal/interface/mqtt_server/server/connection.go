package server

import (
	"fmt"
	"io"
	"log"
	"net"
)

func (b *Broker) HandleConnection(conn net.Conn) error {
	defer conn.Close()
	c := newClient(conn)
	defer b.handleConnectionLoss(c)

	for {
		packetType, flags, _, payload, err := readPacket(c.reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client %s disconnected normally", c.id)
			} else {
				log.Printf("Read error from %s: %v", c.id, err)
			}
			return nil
		}
		switch packetType {
		case CONNECT:
			err = b.handleCONNECT(c, payload)
			if err != nil {
				return err
			}
		case SUBSCRIBE:
			err = b.handleSUBSCRIBE(c, payload)
			if err != nil {
				return err
			}
		case PUBLISH:
			err = b.handlePUBLISH(c, flags, payload)
			if err != nil {
				return err
			}
		case PUBACK:
			err = b.handlePUBACK(c, payload)
			if err != nil {
				return err
			}
		case PUBREC:
			err = b.handlePUBREC(c, payload)
			if err != nil {
				return err
			}
		case PUBREL:
			err = b.handlePUBREL(c, payload)
			if err != nil {
				return err
			}
		case PUBCOMP:
			err = b.handlePUBCOMP(c, payload)
			if err != nil {
				return err
			}
		case PINGREQ:
			err = b.handlePINGREQ(c)
			if err != nil {
				return err
			}
		case DISCONNECT:
			b.handleDISCONNECT(c)
			return nil
		default:
			fmt.Printf("unsupported packet type %d\n", packetType)
			return nil
		}
	}
}