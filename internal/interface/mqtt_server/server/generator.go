package server

import (
	"crypto/rand"
	"encoding/hex"
)

func generateRandomID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return "go-" + hex.EncodeToString(b)
}
