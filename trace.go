package diaggate

import (
	"encoding/hex"
	"log"
	"sync"
	"time"
)

type FrameDirection string

const (
	DirectionGatewayToDevice FrameDirection = "gateway->device"
	DirectionDeviceToGateway FrameDirection = "device->gateway"
	DirectionClientToGateway FrameDirection = "client->gateway"
	DirectionGatewayToClient FrameDirection = "gateway->client"
)

// FrameEvent is one observable frame or routing decision.  Dropped late
// frames are recorded with DropReason set, allowing tests to prove that they
// were never delivered to a successor request.
type FrameEvent struct {
	Seq        int            `json:"seq"`
	Time       time.Time      `json:"time"`
	Direction  FrameDirection `json:"direction"`
	DeviceID   byte           `json:"device_id"`
	RequestID  uint16         `json:"request_id"`
	Epoch      bool           `json:"epoch"`
	Type       FrameType      `json:"type"`
	Total      uint16         `json:"total,omitempty"`
	Length     byte           `json:"length"`
	PayloadHex string         `json:"payload_hex,omitempty"`
	DropReason string         `json:"drop_reason,omitempty"`
	Note       string         `json:"note,omitempty"`
}

type Tracer interface {
	Record(FrameEvent)
	Events() []FrameEvent
}

type MemoryTracer struct {
	mu     sync.Mutex
	events []FrameEvent
	clock  Clock
}

func NewMemoryTracer(clock Clock) *MemoryTracer {
	if clock == nil {
		clock = RealClock{}
	}
	return &MemoryTracer{clock: clock}
}

func (t *MemoryTracer) Record(e FrameEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e.Time.IsZero() {
		e.Time = t.clock.Now()
	}
	e.Seq = len(t.events) + 1
	t.events = append(t.events, e)
}

func (t *MemoryTracer) Events() []FrameEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]FrameEvent, len(t.events))
	copy(out, t.events)
	return out
}

func (t *MemoryTracer) Reset() {
	t.mu.Lock()
	t.events = nil
	t.mu.Unlock()
}

type NopTracer struct{}

func (NopTracer) Record(FrameEvent)    {}
func (NopTracer) Events() []FrameEvent { return nil }

func traceFrame(tr Tracer, dir FrameDirection, f Frame, note string) {
	if tr == nil {
		return
	}
	tr.Record(FrameEvent{
		Direction:  dir,
		DeviceID:   f.DeviceID,
		RequestID:  f.RequestID,
		Epoch:      f.Epoch,
		Type:       f.Type,
		Total:      f.Total,
		Length:     f.Length,
		PayloadHex: hex.EncodeToString(f.Payload),
		Note:       note,
	})
}

// LoggingTracer prints one structured line per frame event.  It is handy for
// observing a live gateway; tests use MemoryTracer.
type LoggingTracer struct {
	logger *log.Logger
}

func NewLoggingTracer(logger *log.Logger) *LoggingTracer {
	if logger == nil {
		logger = log.New(log.Writer(), "frame ", log.LstdFlags|log.Lmicroseconds)
	}
	return &LoggingTracer{logger: logger}
}

func (t *LoggingTracer) Record(e FrameEvent) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	epoch := 0
	if e.Epoch {
		epoch = 1
	}
	switch {
	case e.DropReason != "":
		t.logger.Printf("DROP dev=%d req=%d epoch=%d type=%s len=%d reason=%q note=%q",
			e.DeviceID, e.RequestID, epoch, e.Type, e.Length, e.DropReason, e.Note)
	case e.PayloadHex != "":
		t.logger.Printf("%s dev=%d req=%d epoch=%d type=%s total=%d len=%d payload=%s note=%q",
			e.Direction, e.DeviceID, e.RequestID, epoch, e.Type, e.Total, e.Length, e.PayloadHex, e.Note)
	default:
		t.logger.Printf("%s dev=%d req=%d epoch=%d type=%s total=%d len=%d note=%q",
			e.Direction, e.DeviceID, e.RequestID, epoch, e.Type, e.Total, e.Length, e.Note)
	}
}

func (t *LoggingTracer) Events() []FrameEvent { return nil }
