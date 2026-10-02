package diaggate

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type sessionKey struct {
	Epoch     bool
	RequestID uint16
}

// DeviceRequest is the assembled request presented to virtual device logic.
type DeviceRequest struct {
	DeviceID  byte
	RequestID uint16
	Epoch     bool
	Payload   []byte
}

// GrantPolicy decides how many continuation frames the device will allow for
// the next credit window.  Returning zero is an explicit quota denial and
// terminates only this session.  Blocking here creates a controllable
// flow-control stall.  Credits are automatically clamped to the number of
// continuations still required.
type GrantPolicy func(ctx context.Context, req DeviceRequest, grantedContinuations int, continuationsRemaining int) (uint8, error)

// ResponseFunc implements virtual device behavior.  It may block to inject
// latency; tests can use the supplied deterministic Clock.  A function that
// ignores ctx after cancellation can deliberately emit late frames.
type ResponseFunc func(ctx context.Context, clock Clock, req DeviceRequest) ([]byte, *ProtocolError)

// TransformResponse can reorder, duplicate or alter response frames before
// the virtual device sends them.
type TransformResponse func(frames []Frame) []Frame

type VirtualDeviceConfig struct {
	ID byte

	// Delay is waited before a well-formed request is handled.
	Delay time.Duration

	// GrantPolicy controls continuation-frame credits.  Default grants four.
	GrantPolicy GrantPolicy

	// Respond builds the response.  Default echoes the request payload.
	Respond ResponseFunc

	// TransformResponse is useful for out-of-order/frame corruption tests.
	TransformResponse TransformResponse

	Clock  Clock
	Tracer Tracer
}

type VirtualDevice struct {
	cfg      VirtualDeviceConfig
	listener *streamListener

	mu     sync.Mutex
	closed bool
}

func NewVirtualDevice(cfg VirtualDeviceConfig) *VirtualDevice {
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = NopTracer{}
	}
	if cfg.GrantPolicy == nil {
		cfg.GrantPolicy = DefaultGrantPolicy
	}
	if cfg.Respond == nil {
		cfg.Respond = EchoResponse(cfg.Delay)
	}
	return &VirtualDevice{cfg: cfg, listener: newStreamListener(4)}
}

func DefaultGrantPolicy(context.Context, DeviceRequest, int, int) (uint8, error) {
	return 4, nil
}

func OneThenBlockPolicy(block <-chan struct{}) GrantPolicy {
	first := true
	return func(ctx context.Context, _ DeviceRequest, _ int, remaining int) (uint8, error) {
		if first {
			first = false
			return 1, nil
		}
		select {
		case <-block:
			return min64u8(4, remaining), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func min64u8(a uint8, b int) uint8 {
	if b < 0 {
		return 0
	}
	if int(a) > b {
		return uint8(b)
	}
	return a
}

// EchoResponse waits the configured delay and returns the request payload.
func EchoResponse(delay time.Duration) ResponseFunc {
	return func(ctx context.Context, clock Clock, req DeviceRequest) ([]byte, *ProtocolError) {
		if delay > 0 {
			t := clock.NewTimer(delay)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "request canceled before response")
			case <-t.Chan():
			}
		}
		return append([]byte(nil), req.Payload...), nil
	}
}

func (d *VirtualDevice) ID() byte { return d.cfg.ID }

func (d *VirtualDevice) Dial() (FrameTransport, error) {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return nil, ErrDeviceClosed
	}
	conn, err := d.listener.Dial()
	if err != nil {
		return nil, err
	}
	return NewFrameConn(conn), nil
}

func (d *VirtualDevice) Run(ctx context.Context) error {
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return err
		}
		// serveConn blocks in Recv until the gateway closes its end of the
		// pipe; it is deliberately fire-and-forget (no WaitGroup), so Close
		// cannot deadlock and no Add/Wait race exists.
		go d.serveConn(ctx, NewFrameConn(conn))
	}
}

func (d *VirtualDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		d.closed = true
		return d.listener.Close()
	}
	return nil
}

type deviceSession struct {
	key        sessionKey
	req        DeviceRequest
	asm        *assembler
	frameCount int
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	complete   bool
	terminal   bool
}

func (d *VirtualDevice) serveConn(parent context.Context, fc FrameTransport) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Sessions keyed by their identifying session key (epoch + request ID)
	// support cancel/error lookup.  Continuation frames cannot carry the
	// request ID (that field holds the cyclic sequence), so the currently
	// assembling session is additionally indexed by epoch alone.
	sessions := make(map[sessionKey]*deviceSession)
	byEpoch := make(map[bool]*deviceSession)
	var sessionsMu sync.Mutex

	for {
		frame, err := fc.Recv()
		if err != nil {
			return
		}
		traceFrame(d.cfg.Tracer, DirectionGatewayToDevice, frame, "virtual device received")
		key := sessionKey{Epoch: frame.Epoch, RequestID: frame.RequestID}

		if frame.Type == TypeCancel {
			sessionsMu.Lock()
			s := sessions[key]
			if s != nil {
				delete(byEpoch, frame.Epoch)
			}
			sessionsMu.Unlock()
			if s != nil {
				s.cancel()
				s.mu.Lock()
				s.terminal = true
				s.mu.Unlock()
				d.drop(frame, "cancel accepted")
			} else {
				d.drop(frame, "cancel for unknown or expired session")
			}
			continue
		}

		if frame.Type == TypeFirst {
			s := &deviceSession{
				key: key,
				req: DeviceRequest{
					DeviceID:  frame.DeviceID,
					RequestID: frame.RequestID,
					Epoch:     frame.Epoch,
				},
				asm: newAssembler(),
			}
			s.ctx, s.cancel = context.WithCancel(ctx)
			if err := s.asm.data(frame); err != nil {
				d.sendProtocolError(fc, frame, err)
				continue
			}
			s.req.Payload = append([]byte(nil), s.asm.payload...)
			s.frameCount = framesForPayloadCount(int(frame.Total))
			s.complete = s.asm.complete()
			sessionsMu.Lock()
			sessions[key] = s
			byEpoch[frame.Epoch] = s
			sessionsMu.Unlock()

			go d.grantLoop(fc, s)
			if s.complete {
				go d.handleComplete(fc, s)
			}
			continue
		}

		if frame.Type != TypeCont {
			d.drop(frame, "virtual device received unexpected frame type")
			continue
		}

		sessionsMu.Lock()
		s := byEpoch[frame.Epoch]
		sessionsMu.Unlock()
		if s == nil {
			d.drop(frame, "late continuation for unknown session")
			continue
		}

		s.mu.Lock()
		if s.terminal {
			s.mu.Unlock()
			d.drop(frame, "frame after canceled/terminal session")
			continue
		}
		err = s.asm.data(frame)
		if err == errAssemblerClosed {
			err = frameError(frame.DeviceID, s.req.RequestID, CodeOutOfOrder, "frame after completed assembly")
		}
		if err != nil {
			s.terminal = true
			s.cancel()
			s.mu.Unlock()
			d.sendProtocolError(fc, frame, err)
			continue
		}
		completedNow := s.asm.complete()
		if completedNow {
			s.complete = true
			s.req.Payload = append([]byte(nil), s.asm.payload...)
		}
		s.mu.Unlock()

		if completedNow {
			sessionsMu.Lock()
			delete(byEpoch, frame.Epoch)
			sessionsMu.Unlock()
			go d.handleComplete(fc, s)
		}
	}
}

func (d *VirtualDevice) grantLoop(fc FrameTransport, s *deviceSession) {
	continuations := s.frameCount - 1
	grantedTotal := 0
	for continuations > 0 {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		credits, err := d.cfg.GrantPolicy(s.ctx, s.req, grantedTotal, continuations)
		if err != nil {
			if s.ctx.Err() == nil {
				d.sendError(fc, s.req, frameError(s.req.DeviceID, s.req.RequestID, CodeDeviceError, "grant policy failed: %v", err))
			}
			return
		}
		if credits > 4 {
			credits = 4
		}
		grant := Frame{
			Header: Header{
				Version:   ProtocolVersion,
				Type:      TypeGrant,
				Epoch:     s.req.Epoch,
				DeviceID:  s.req.DeviceID,
				RequestID: s.req.RequestID,
				Total:     uint16(grantedTotal + int(credits)),
				Length:    1,
			},
			Payload: []byte{credits},
		}
		if err := fc.Send(grant); err != nil {
			return
		}
		traceFrame(d.cfg.Tracer, DirectionDeviceToGateway, grant, "virtual device granted credits")
		if credits == 0 {
			s.mu.Lock()
			s.terminal = true
			s.mu.Unlock()
			s.cancel()
			return
		}
		amount := int(credits)
		if amount > continuations {
			amount = continuations
		}
		grantedTotal += amount
		continuations -= amount
	}
}

func (d *VirtualDevice) handleComplete(fc FrameTransport, s *deviceSession) {
	payload, perr := d.cfg.Respond(s.ctx, d.cfg.Clock, s.req)
	if perr != nil {
		d.sendError(fc, s.req, perr)
		return
	}
	if len(payload) > MaxPayload {
		d.sendError(fc, s.req, frameError(s.req.DeviceID, s.req.RequestID, CodeLengthMismatch, "virtual response exceeds 512 bytes"))
		return
	}
	frames, err := ResponseFrames(s.req.DeviceID, s.req.RequestID, s.req.Epoch, payload)
	if err != nil {
		d.sendError(fc, s.req, frameError(s.req.DeviceID, s.req.RequestID, CodeMalformedFrame, err.Error()))
		return
	}
	if d.cfg.TransformResponse != nil {
		frames = d.cfg.TransformResponse(frames)
	}
	for _, f := range frames {
		if err := fc.Send(f); err != nil {
			return
		}
		traceFrame(d.cfg.Tracer, DirectionDeviceToGateway, f, "virtual device response")
	}
}

func (d *VirtualDevice) sendProtocolError(fc FrameTransport, f Frame, err error) {
	perr, ok := err.(*ProtocolError)
	if !ok {
		perr = frameError(f.DeviceID, f.RequestID, CodeMalformedFrame, err.Error())
	}
	d.sendError(fc, DeviceRequest{DeviceID: f.DeviceID, RequestID: f.RequestID, Epoch: f.Epoch}, perr)
}

func (d *VirtualDevice) sendError(fc FrameTransport, req DeviceRequest, perr *ProtocolError) {
	body, err := json.Marshal(perr)
	if err != nil {
		body = []byte(`{"code":"device_error"}`)
	}
	if len(body) > MaxPayload {
		body = body[:MaxPayload]
	}
	f := Frame{
		Header: Header{
			Version:   ProtocolVersion,
			Type:      TypeError,
			Epoch:     req.Epoch,
			DeviceID:  req.DeviceID,
			RequestID: req.RequestID,
			Total:     uint16(len(body)),
			Length:    byte(len(body)),
		},
		Payload: body,
	}
	if err := fc.Send(f); err == nil {
		traceFrame(d.cfg.Tracer, DirectionDeviceToGateway, f, "virtual device error")
	}
}

func (d *VirtualDevice) drop(f Frame, reason string) {
	d.cfg.Tracer.Record(FrameEvent{
		Direction:  DirectionGatewayToDevice,
		DeviceID:   f.DeviceID,
		RequestID:  f.RequestID,
		Epoch:      f.Epoch,
		Type:       f.Type,
		Total:      f.Total,
		Length:     f.Length,
		PayloadHex: payloadHex(f.Payload),
		DropReason: reason,
	})
}

func payloadHex(b []byte) string {
	const hexChars = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexChars[v>>4]
		out[i*2+1] = hexChars[v&0x0f]
	}
	return string(out)
}

func framesForPayloadCount(n int) int {
	if n <= ChunkSize {
		return 1
	}
	return 1 + (n-ChunkSize+ChunkSize-1)/ChunkSize
}
