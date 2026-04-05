package server

import (
	"bufio"
	"encoding/binary"
	"io"
)

func readUTF8String(r *bufio.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeUTF8String(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func readPacket(r *bufio.Reader) (packetType byte, flags byte, remaining int, payload []byte, err error) {
	firstByte, err := r.ReadByte()
	if err != nil {
		return 0, 0, 0, nil, err
	}
	packetType = firstByte >> 4
	flags = firstByte & 0x0F

	remaining, err = decodeVariableByteInteger(r)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	payload = make([]byte, remaining)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	return packetType, flags, remaining, payload, nil
}

func readUint16(r *bufio.Reader) (uint16, error) {
	var val uint16
	err := binary.Read(r, binary.BigEndian, &val)
	return val, err
}
