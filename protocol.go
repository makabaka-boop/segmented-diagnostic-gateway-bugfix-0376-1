package diaggate

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire protocol
//
// Every frame has an exactly eight-byte little-endian header:
//
//   byte 0:      version (1)
//   byte 1:      frame type (low nibble) | epoch bit (0x80)
//   byte 2:      device ID
//   bytes 3-4:   request ID
//   bytes 5-6:   total payload length, carried meaningfully by the first frame
//   byte 7:      this frame's payload length (0..63)
//
// The first frame records the total payload length.  Continuation frames use
// 63-byte chunks and carry a cyclic uint16 sequence number in RequestID, while
// the first frame's request ID identifies the call.  This layout makes the
// byte stream parser stateless: an abandoned session cannot corrupt the next
// session even when its frames arrive late.

const (
	ProtocolVersion byte = 1

	MaxPayload   = 512
	ChunkSize    = 63
	MaxFrameSize = 8 + ChunkSize

	EpochBit byte = 0x80
	TypeMask byte = 0x0f

	TypeFirst  FrameType = 1
	TypeCont   FrameType = 2
	TypeGrant  FrameType = 3
	TypeCancel FrameType = 4
	TypeError  FrameType = 5
	TypeEnd    FrameType = 6
)

type FrameType byte

func (t FrameType) String() string {
	switch t {
	case TypeFirst:
		return "first"
	case TypeCont:
		return "cont"
	case TypeGrant:
		return "grant"
	case TypeCancel:
		return "cancel"
	case TypeError:
		return "error"
	case TypeEnd:
		return "end"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// Header is the fixed eight-byte frame header.
type Header struct {
	Version   byte
	Type      FrameType
	Epoch     bool
	DeviceID  byte
	RequestID uint16

	// Total is the total payload length for first/end/error frames.  For
	// continuation frames it carries the cyclic sequence number (0..65535).
	Total  uint16
	Length byte
}

type Frame struct {
	Header
	Payload []byte
}

func encodeHeader(h Header) []byte {
	b := make([]byte, 8)
	b[0] = h.Version
	b[1] = byte(byte(h.Type) & TypeMask)
	if h.Epoch {
		b[1] |= EpochBit
	}
	b[2] = h.DeviceID
	binary.LittleEndian.PutUint16(b[3:5], h.RequestID)
	binary.LittleEndian.PutUint16(b[5:7], h.Total)
	b[7] = h.Length
	return b
}

func decodeHeader(b []byte) (Header, error) {
	if len(b) < 8 {
		return Header{}, fmt.Errorf("%w: short header", ErrMalformedFrame)
	}
	h := Header{
		Version:   b[0],
		Type:      FrameType(b[1] & TypeMask),
		Epoch:     b[1]&EpochBit != 0,
		DeviceID:  b[2],
		RequestID: binary.LittleEndian.Uint16(b[3:5]),
		Total:     binary.LittleEndian.Uint16(b[5:7]),
		Length:    b[7],
	}
	if h.Version != ProtocolVersion {
		return h, &ProtocolError{Code: CodeMalformedFrame, Message: fmt.Sprintf("unsupported protocol version %d", h.Version)}
	}
	if h.Type < TypeFirst || h.Type > TypeEnd {
		return h, &ProtocolError{Code: CodeMalformedFrame, Message: fmt.Sprintf("unknown frame type %d", h.Type)}
	}
	return h, nil
}

func (f Frame) marshal() ([]byte, error) {
	if f.Version != 0 && f.Version != ProtocolVersion {
		return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "invalid frame version"}
	}
	if int(f.Length) != len(f.Payload) {
		return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "frame length does not match payload"}
	}
	if f.Length > ChunkSize {
		return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "frame payload exceeds 63 bytes"}
	}
	switch f.Type {
	case TypeFirst, TypeEnd, TypeError:
		if f.Total > MaxPayload {
			return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "total payload exceeds 512 bytes"}
		}
		if f.Type == TypeError && int(f.Total) != len(f.Payload) {
			return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "error frame length mismatch"}
		}
	case TypeCont:
		// Total is overloaded as the continuation sequence number.
	case TypeGrant:
		if f.Length != 1 || len(f.Payload) != 1 {
			return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "grant frame must contain exactly one byte"}
		}
		if f.Payload[0] > 4 {
			return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "grant credit must be 0..4"}
		}
	case TypeCancel:
		if f.Length != 0 || len(f.Payload) != 0 {
			return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "cancel frame must be empty"}
		}
	default:
		return nil, &ProtocolError{Code: CodeMalformedFrame, Message: "unsupported frame type"}
	}
	h := f.Header
	h.Version = ProtocolVersion
	b := encodeHeader(h)
	return append(b, f.Payload...), nil
}

// FramesForPayload splits a payload into a first frame plus continuation
// frames.  Sequence numbers are cyclic over uint16 and begin at one.
func FramesForPayload(deviceID byte, requestID uint16, epoch bool, payload []byte) ([]Frame, error) {
	if len(payload) > MaxPayload {
		return nil, fmt.Errorf("%w: payload is %d bytes", ErrPayloadTooLarge, len(payload))
	}
	frames := make([]Frame, 0, (len(payload)+ChunkSize)/ChunkSize+1)
	frames = append(frames, Frame{
		Header: Header{
			Version:   ProtocolVersion,
			Type:      TypeFirst,
			Epoch:     epoch,
			DeviceID:  deviceID,
			RequestID: requestID,
			Total:     uint16(len(payload)),
			Length:    byte(len(payload)),
		},
		Payload: append([]byte(nil), payload...),
	})
	if len(payload) > ChunkSize {
		frames[0].Length = ChunkSize
		frames[0].Payload = append([]byte(nil), payload[:ChunkSize]...)
	}
	for offset, seq := ChunkSize, uint16(1); offset < len(payload); offset, seq = offset+ChunkSize, seq+1 {
		end := offset + ChunkSize
		if end > len(payload) {
			end = len(payload)
		}
		frames = append(frames, Frame{
			Header: Header{
				Version:   ProtocolVersion,
				Type:      TypeCont,
				Epoch:     epoch,
				DeviceID:  deviceID,
				RequestID: seq,
				Total:     seq,
				Length:    byte(end - offset),
			},
			Payload: append([]byte(nil), payload[offset:end]...),
		})
	}
	return frames, nil
}

// ResponseFrames creates a complete response.  Every response, including an
// empty one, ends with a zero-length End frame so the receiver knows the
// transfer finished and can detect truncation before reading the next session.
func ResponseFrames(deviceID byte, requestID uint16, epoch bool, payload []byte) ([]Frame, error) {
	frames, err := FramesForPayload(deviceID, requestID, epoch, payload)
	if err != nil {
		return nil, err
	}
	frames = append(frames, Frame{
		Header: Header{
			Version:   ProtocolVersion,
			Type:      TypeEnd,
			Epoch:     epoch,
			DeviceID:  deviceID,
			RequestID: requestID,
			Total:     uint16(len(payload)),
		},
	})
	return frames, nil
}

var errAssemblerClosed = errors.New("frame assembler closed")

// assembler validates a first frame followed by cyclic continuation frames.
type assembler struct {
	deviceID  byte
	requestID uint16
	epoch     bool

	total     int
	received  int
	nextSeq   uint16
	firstSeen bool
	expectEnd bool
	sawEnd    bool
	payload   []byte
}

func newAssembler() *assembler { return &assembler{} }

func newResponseAssembler() *assembler {
	a := newAssembler()
	a.expectEnd = true
	return a
}

func (a *assembler) data(f Frame) error {
	if !a.firstSeen {
		if f.Type != TypeFirst {
			return &ProtocolError{Code: CodeOutOfOrder, Message: "expected first frame", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		if int(f.Total) > MaxPayload {
			return &ProtocolError{Code: CodeLengthMismatch, Message: "total length exceeds protocol limit", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		if int(f.Length) > ChunkSize || int(f.Length) != len(f.Payload) {
			return &ProtocolError{Code: CodeMalformedFrame, Message: "invalid first frame length", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		expectedFirst := int(f.Total)
		if expectedFirst > ChunkSize {
			expectedFirst = ChunkSize
		}
		if int(f.Length) != expectedFirst {
			return &ProtocolError{Code: CodeLengthMismatch, Message: "first frame has incorrect chunk length", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		if int(f.Total) == 0 && int(f.Length) != 0 {
			return &ProtocolError{Code: CodeLengthMismatch, Message: "zero-length request has non-empty first frame", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		a.deviceID = f.DeviceID
		a.requestID = f.RequestID
		a.epoch = f.Epoch
		a.total = int(f.Total)
		a.received = len(f.Payload)
		a.nextSeq = 1
		a.payload = append(a.payload, f.Payload...)
		a.firstSeen = true
		return nil
	}

	if f.DeviceID != a.deviceID || f.Epoch != a.epoch {
		return &ProtocolError{Code: CodeOutOfOrder, Message: "frame belongs to another session", DeviceID: f.DeviceID, RequestID: f.RequestID}
	}
	if f.Type == TypeEnd {
		if !a.expectEnd {
			return &ProtocolError{Code: CodeOutOfOrder, Message: "unexpected end frame in request stream", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		if a.received != a.total || int(f.Total) != a.total || int(f.Length) != 0 || len(f.Payload) != 0 {
			return &ProtocolError{Code: CodeLengthMismatch, Message: "end frame does not match assembled length", DeviceID: f.DeviceID, RequestID: f.RequestID}
		}
		a.sawEnd = true
		return errAssemblerClosed
	}
	if f.Type != TypeCont {
		return &ProtocolError{Code: CodeOutOfOrder, Message: "expected continuation or end frame", DeviceID: f.DeviceID, RequestID: f.RequestID}
	}
	if a.sawEnd || (!a.expectEnd && a.received == a.total) {
		return &ProtocolError{Code: CodeOutOfOrder, Message: "frame after completed assembly", DeviceID: f.DeviceID, RequestID: f.RequestID}
	}
	if f.Total != a.nextSeq || f.RequestID != a.nextSeq {
		return &ProtocolError{Code: CodeOutOfOrder, Message: fmt.Sprintf("expected sequence %d, got %d", a.nextSeq, f.RequestID), DeviceID: f.DeviceID, RequestID: f.RequestID}
	}
	remaining := a.total - a.received
	expected := ChunkSize
	if remaining < ChunkSize {
		expected = remaining
	}
	if int(f.Length) != expected || len(f.Payload) != expected {
		return &ProtocolError{Code: CodeLengthMismatch, Message: "continuation frame has incorrect chunk length", DeviceID: f.DeviceID, RequestID: f.RequestID}
	}
	a.payload = append(a.payload, f.Payload...)
	a.received += len(f.Payload)
	a.nextSeq++ // uint16 wraps naturally at 65535 -> 0
	return nil
}

func (a *assembler) complete() bool {
	if !a.firstSeen || a.received != a.total {
		return false
	}
	return !a.expectEnd || a.sawEnd
}
