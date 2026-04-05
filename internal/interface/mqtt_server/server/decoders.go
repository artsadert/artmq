package server

import (
	"bufio"
	"errors"
)

func decodeVariableByteInteger(r *bufio.Reader) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&0x7F) * multiplier
		if b&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, errors.New("malformed variable byte integer")
		}
	}
	return 0, errors.New("invalid variable byte integer")
}
