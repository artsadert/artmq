package server

import (
	"bufio"
	"net"
	"sync"
)

type Client struct {
	id     string
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
}
