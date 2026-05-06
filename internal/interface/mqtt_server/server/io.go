package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
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

// publishProps holds the subset of MQTT 5 PUBLISH properties this broker uses.
// Fields are pointers so absence is distinguishable from a zero value.
type publishProps struct {
	MessageExpiryInterval *uint32 // property 0x02, seconds
	Priority              *int64  // user property "priority" (decimal string)
}

// parsePublishProperties reads `length` bytes of MQTT 5 PUBLISH properties
// from r. It extracts Message Expiry Interval and any User Property whose key
// is "priority"; it skips other known properties without erroring so that
// well-formed packets with unrelated properties still publish successfully.
func parsePublishProperties(r *bufio.Reader, length int) (publishProps, error) {
	var out publishProps
	remaining := length
	for remaining > 0 {
		id, err := r.ReadByte()
		if err != nil {
			return out, err
		}
		remaining--

		switch id {
		case 0x01: // Payload Format Indicator — 1 byte
			if _, err := r.ReadByte(); err != nil {
				return out, err
			}
			remaining--
		case 0x02: // Message Expiry Interval — 4 bytes
			var v uint32
			if err := binary.Read(r, binary.BigEndian, &v); err != nil {
				return out, err
			}
			remaining -= 4
			out.MessageExpiryInterval = &v
		case 0x03, 0x08: // Content Type / Response Topic — UTF-8 string
			s, err := readUTF8String(r)
			if err != nil {
				return out, err
			}
			remaining -= 2 + len(s)
		case 0x09: // Correlation Data — Binary Data
			var l uint16
			if err := binary.Read(r, binary.BigEndian, &l); err != nil {
				return out, err
			}
			if _, err := io.CopyN(io.Discard, r, int64(l)); err != nil {
				return out, err
			}
			remaining -= 2 + int(l)
		case 0x0B: // Subscription Identifier — Variable Byte Integer
			start := remaining
			before := r.Buffered()
			if _, err := decodeVariableByteInteger(r); err != nil {
				return out, err
			}
			consumed := before - r.Buffered()
			if consumed <= 0 {
				consumed = 1
			}
			remaining = start - consumed
		case 0x23: // Topic Alias — 2 bytes
			if _, err := readUint16(r); err != nil {
				return out, err
			}
			remaining -= 2
		case 0x26: // User Property — UTF-8 string pair
			k, err := readUTF8String(r)
			if err != nil {
				return out, err
			}
			v, err := readUTF8String(r)
			if err != nil {
				return out, err
			}
			remaining -= 4 + len(k) + len(v)
			if k == "priority" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					out.Priority = &n
				}
			}
		default:
			return out, fmt.Errorf("unsupported PUBLISH property id 0x%02x", id)
		}
	}
	if remaining != 0 {
		return out, fmt.Errorf("PUBLISH properties length mismatch")
	}
	return out, nil
}
