package diaggate

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// FrameTransport is the boundary between the gateway and one virtual device.
// It is normally an eight-byte framed net.Conn, but tests can replace it.
type FrameTransport interface {
	Send(Frame) error
	Recv() (Frame, error)
	io.Closer
}

type DeviceDialer func(deviceID byte) (FrameTransport, error)

type GatewayConfig struct {
	DeviceIDs []byte
	Dial      DeviceDialer

	// Timeout bounds one complete request/response exchange.
	Timeout time.Duration

	Clock  Clock
	Tracer Tracer
}

type Gateway struct {
	cfg   GatewayConfig
	slots map[byte]*deviceSlot

	closeMu sync.Mutex
	closed  bool
}

func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	if len(cfg.DeviceIDs) < 2 || len(cfg.DeviceIDs) > 4 {
		return nil, &ProtocolError{Code: CodeUnavailable, Message: "diagnostic gateway requires 2 to 4 virtual devices"}
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = NopTracer{}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.Dial == nil {
		return nil, &ProtocolError{Code: CodeUnavailable, Message: "device dialer is required"}
	}
	g := &Gateway{cfg: cfg, slots: make(map[byte]*deviceSlot, len(cfg.DeviceIDs))}
	for _, id := range cfg.DeviceIDs {
		if _, exists := g.slots[id]; exists {
			return nil, &ProtocolError{Code: CodeUnavailable, Message: "duplicate device ID"}
		}
		g.slots[id] = &deviceSlot{id: id}
	}
	return g, nil
}

func (g *Gateway) DeviceIDs() []byte {
	out := make([]byte, len(g.cfg.DeviceIDs))
	copy(out, g.cfg.DeviceIDs)
	return out
}

func (g *Gateway) Close() error {
	g.closeMu.Lock()
	g.closed = true
	g.closeMu.Unlock()
	return nil
}

type callSession struct {
	key     sessionKey
	grantCh chan Frame
	respCh  chan Frame
	readErr chan error
}

type deviceSlot struct {
	id byte

	// callMu enforces at most one outstanding call on this device.
	callMu sync.Mutex

	// Epoch flips for every call.  Even a reused request ID is therefore
	// distinguishable from a canceled predecessor.
	epochMu sync.Mutex
	epoch   bool
}

// CallResult contains a successful response.
type CallResult struct {
	DeviceID  byte
	RequestID uint16
	Epoch     bool
	Payload   []byte
}

// RoundTrip executes one diagnostic request.  Different device IDs proceed in
// parallel; calls to the same device are serialized.  All timeout, cancel and
// protocol failures are session-local.
func (g *Gateway) RoundTrip(ctx context.Context, deviceID byte, requestID uint16, payload []byte) (*CallResult, error) {
	if len(payload) > MaxPayload {
		return nil, frameError(deviceID, requestID, CodePayloadTooLarge, "payload is %d bytes, maximum is %d", len(payload), MaxPayload)
	}
	s, ok := g.slots[deviceID]
	if !ok {
		return nil, frameError(deviceID, requestID, CodeUnavailable, "unknown device ID")
	}

	// Per-device serialization: only one outstanding request per device.
	s.callMu.Lock()
	defer s.callMu.Unlock()

	g.closeMu.Lock()
	closed := g.closed
	g.closeMu.Unlock()
	if closed {
		return nil, frameError(deviceID, requestID, CodeDeviceClosed, "gateway is closed")
	}

	fc, err := g.cfg.Dial(deviceID)
	if err != nil {
		return nil, frameError(deviceID, requestID, CodeUnavailable, "connect virtual device: %v", err)
	}
	// Every call uses a dedicated transport and closes it on return.  A late
	// predecessor frame therefore cannot even reach the successor; even if a
	// shared transport were used, the flipped epoch bit would make readFrames
	// drop it before delivery.
	defer fc.Close()

	s.epochMu.Lock()
	epoch := s.epoch
	s.epoch = !s.epoch
	s.epochMu.Unlock()

	sess := &callSession{
		key:     sessionKey{Epoch: epoch, RequestID: requestID},
		grantCh: make(chan Frame, 64),
		respCh:  make(chan Frame, 64),
		readErr: make(chan error, 1),
	}
	go g.readFrames(deviceID, fc, sess)

	frames, err := FramesForPayload(deviceID, requestID, epoch, payload)
	if err != nil {
		return nil, frameError(deviceID, requestID, CodePayloadTooLarge, err.Error())
	}

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deadline := g.cfg.Clock.NewTimer(g.cfg.Timeout)
	defer deadline.Stop()

	asm := newResponseAssembler()
	credits := 0
	nextFrame := 0
	var terminalErr *ProtocolError
	transportFailed := false

	terminate := func(code ErrorCode, format string, args ...any) error {
		c := Frame{Header: Header{Version: ProtocolVersion, Type: TypeCancel, Epoch: epoch, DeviceID: deviceID, RequestID: requestID}}
		if err := fc.Send(c); err != nil {
			traceFrame(g.cfg.Tracer, DirectionGatewayToDevice, c, "best-effort cancel failed: "+err.Error())
		} else {
			traceFrame(g.cfg.Tracer, DirectionGatewayToDevice, c, "gateway cancellation")
		}
		return frameError(deviceID, requestID, code, format, args...)
	}

	// The first frame is always admitted; grants govern continuation frames.
	if err := fc.Send(frames[0]); err != nil {
		return nil, frameError(deviceID, requestID, CodeDeviceClosed, "send first frame: %v", err)
	}
	traceFrame(g.cfg.Tracer, DirectionGatewayToDevice, frames[0], "gateway request first frame")
	nextFrame = 1

	for {
		// Send as many continuation frames as current credit permits.
		for nextFrame < len(frames) && credits > 0 {
			f := frames[nextFrame]
			if err := fc.Send(f); err != nil {
				return nil, frameError(deviceID, requestID, CodeDeviceClosed, "send continuation: %v", err)
			}
			traceFrame(g.cfg.Tracer, DirectionGatewayToDevice, f, "gateway request continuation")
			nextFrame++
			credits--
		}

		if nextFrame == len(frames) && asm.complete() {
			return &CallResult{DeviceID: deviceID, RequestID: requestID, Epoch: epoch, Payload: append([]byte(nil), asm.payload...)}, nil
		}
		if terminalErr != nil {
			return nil, terminate(terminalErr.Code, terminalErr.Message)
		}
		if transportFailed {
			return nil, terminate(CodeDeviceClosed, "virtual device transport closed unexpectedly")
		}

		select {
		case <-callCtx.Done():
			return nil, terminate(CodeCanceled, "caller canceled request")

		case <-deadline.Chan():
			return nil, terminate(CodeTimeout, "request did not complete within %s", g.cfg.Timeout)

		case <-sess.readErr:
			transportFailed = true

		case grant := <-sess.grantCh:
			traceFrame(g.cfg.Tracer, DirectionDeviceToGateway, grant, "gateway accepted grant")
			amount := int(grant.Payload[0])
			if amount == 0 {
				terminalErr = frameError(deviceID, requestID, CodeQuotaExhausted, "device granted zero continuation frames")
				continue
			}
			if amount < 1 || amount > 4 {
				terminalErr = frameError(deviceID, requestID, CodeMalformedFrame, "grant credit must be 1..4")
				continue
			}
			credits += amount

		case f := <-sess.respCh:
			traceFrame(g.cfg.Tracer, DirectionDeviceToGateway, f, "gateway response frame")
			if f.Type == TypeError {
				var perr ProtocolError
				if err := json.Unmarshal(f.Payload, &perr); err != nil {
					return nil, terminate(CodeMalformedFrame, "invalid structured device error: %v", err)
				}
				if perr.DeviceID == 0 {
					perr.DeviceID = deviceID
				}
				if perr.RequestID == 0 {
					perr.RequestID = requestID
				}
				return nil, &perr
			}
			if f.Type == TypeCancel {
				return nil, terminate(CodeCanceled, "device canceled session")
			}
			err := asm.data(f)
			if err == errAssemblerClosed {
				terminalErr = frameError(deviceID, requestID, CodeOutOfOrder, "frame after complete response")
				continue
			}
			if err != nil {
				if pe, ok := err.(*ProtocolError); ok {
					terminalErr = pe
				} else {
					terminalErr = frameError(deviceID, requestID, CodeMalformedFrame, err.Error())
				}
			}
		}
	}
}

func (g *Gateway) readFrames(deviceID byte, fc FrameTransport, sess *callSession) {
	for {
		f, err := fc.Recv()
		if err != nil {
			select {
			case sess.readErr <- err:
			default:
			}
			return
		}
		// Session-level frames (first/end/error/cancel/grant) carry the
		// request ID.  Continuation frames overload that field with the cyclic
		// sequence number and correlate to the session through the epoch bit.
		belongs := f.Epoch == sess.key.Epoch && (f.Type == TypeCont || f.RequestID == sess.key.RequestID)
		if !belongs {
			g.dropLate(deviceID, f)
			continue
		}

		switch f.Type {
		case TypeGrant:
			if len(f.Payload) != 1 || f.Payload[0] > 4 {
				g.dropLate(deviceID, f)
				continue
			}
			select {
			case sess.grantCh <- f:
			default:
				g.dropLate(deviceID, f)
			}
		case TypeFirst, TypeCont, TypeEnd, TypeError, TypeCancel:
			select {
			case sess.respCh <- f:
			default:
				g.dropLate(deviceID, f)
			}
		default:
			g.dropLate(deviceID, f)
		}
	}
}

func (g *Gateway) dropLate(deviceID byte, f Frame) {
	g.cfg.Tracer.Record(FrameEvent{
		Direction:  DirectionDeviceToGateway,
		DeviceID:   deviceID,
		RequestID:  f.RequestID,
		Epoch:      f.Epoch,
		Type:       f.Type,
		Total:      f.Total,
		Length:     f.Length,
		PayloadHex: payloadHex(f.Payload),
		DropReason: "late or foreign frame; not delivered to active request",
	})
}

// NewVirtualTopology builds 2..4 in-memory virtual devices and returns
// them together with a gateway connected to the same deterministic
// clock/tracer.
func NewVirtualTopology(ctx context.Context, clock Clock, tracer Tracer, configs ...VirtualDeviceConfig) (*Gateway, []*VirtualDevice, error) {
	return NewVirtualTopologyWith(1*time.Second, ctx, clock, tracer, configs...)
}

// NewVirtualTopologyWith is NewVirtualTopology with an explicit per-request
// timeout.
func NewVirtualTopologyWith(timeout time.Duration, ctx context.Context, clock Clock, tracer Tracer, configs ...VirtualDeviceConfig) (*Gateway, []*VirtualDevice, error) {
	if len(configs) < 2 || len(configs) > 4 {
		return nil, nil, &ProtocolError{Code: CodeUnavailable, Message: "topology requires 2 to 4 devices"}
	}
	devices := make([]*VirtualDevice, len(configs))
	ids := make([]byte, len(configs))
	for i := range configs {
		if configs[i].ID == 0 {
			configs[i].ID = byte(i + 1)
		}
		if configs[i].Clock == nil {
			configs[i].Clock = clock
		}
		if configs[i].Tracer == nil {
			configs[i].Tracer = tracer
		}
		devices[i] = NewVirtualDevice(configs[i])
		ids[i] = devices[i].ID()
		go func(d *VirtualDevice) { _ = d.Run(ctx) }(devices[i])
	}
	g, err := NewGateway(GatewayConfig{
		DeviceIDs: ids,
		Dial: func(id byte) (FrameTransport, error) {
			for _, d := range devices {
				if d.ID() == id {
					return d.Dial()
				}
			}
			return nil, frameError(id, 0, CodeUnavailable, "device not found")
		},
		Clock:   clock,
		Tracer:  tracer,
		Timeout: timeout,
	})
	if err != nil {
		return nil, nil, err
	}
	return g, devices, nil
}
