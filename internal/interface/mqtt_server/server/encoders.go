package server

import (
	"encoding/binary"
	"io"
)

func encodeVariableByteInteger(w io.Writer, value int) error {
	for {
		digit := byte(value % 128)
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		if err := binary.Write(w, binary.BigEndian, digit); err != nil {
			return err
		}
		if value == 0 {
			break
		}
	}
	return nil
}

func encodeVarint(value int) []byte {
	if value == 0 {
		return []byte{0x00}
	}
	var buf []byte
	for {
		digit := byte(value % 128)
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		buf = append(buf, digit)
		if value == 0 {
			break
		}
	}
	return buf
}
