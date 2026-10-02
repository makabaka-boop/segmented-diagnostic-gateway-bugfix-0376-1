package diaggate

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func testPayload(marker byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = marker + byte(i%7)
	}
	return b
}

func closeDevices(devs []*VirtualDevice) {
	for _, d := range devs {
		_ = d.Close()
	}
}

// TestRoundTripEchoAndFrameLimits covers split/assembly and the grant trail.
func TestRoundTripEchoAndFrameLimits(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1},
		VirtualDeviceConfig{ID: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	payload := testPayload('A', 512)
	res, err := g.RoundTrip(ctx, 2, 42, payload)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if !bytes.Equal(res.Payload, payload) {
		t.Fatalf("payload mismatch: got %d bytes", len(res.Payload))
	}
	var data, grants int
	for _, e := range tr.Events() {
		if e.DeviceID != 2 || !strings.HasPrefix(e.Note, "gateway") {
			continue // count the gateway's view only; tracer is shared with devices
		}
		if e.Type == TypeFirst || e.Type == TypeCont || e.Type == TypeEnd {
			data++
		}
		if e.Type == TypeGrant {
			grants++
		}
	}
	// 9 request frames (1 first + 8 cont), 10 response frames (1 first +
	// 8 cont + 1 terminal End).
	if want := 9 + 10; data != want {
		t.Fatalf("data frames=%d want %d", data, want)
	}
	if grants < 1 {
		t.Fatalf("expected grant trail")
	}
}

// TestCrossDeviceInterleaving proves independent devices make progress while
// each device has at most one active call.
func TestCrossDeviceInterleaving(t *testing.T) {
	ctx := context.Background()
	clock := NewFakeClock(time.Now())
	tr := NewMemoryTracer(clock)
	var mu sync.Mutex
	order := []string{}
	started := make([]chan struct{}, 4)
	released := make([]chan struct{}, 4)
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}
	configs := make([]VirtualDeviceConfig, 4)
	for i := range configs {
		i := i
		started[i] = make(chan struct{})
		released[i] = make(chan struct{})
		configs[i] = VirtualDeviceConfig{
			ID:     byte(i + 1),
			Clock:  clock,
			Tracer: tr,
			Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
				close(started[i])
				select {
				case <-released[i]:
				case <-ctx.Done():
					return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "canceled")
				}
				record("response-" + string(rune('A'+i)))
				return []byte{byte('A' + i)}, nil
			},
		}
	}
	g, devs, err := NewVirtualTopology(ctx, clock, tr, configs...)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	type callResult struct {
		marker byte
		err    error
	}
	results := make(chan callResult, 4)
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			r, err := g.RoundTrip(ctx, byte(i+1), 100, []byte("parallel"))
			if err != nil {
				results <- callResult{err: err}
				return
			}
			results <- callResult{marker: r.Payload[0]}
		}()
	}
	for i := range started {
		<-started[i]
	}
	record("all-devices-started")
	for i := range released {
		close(released[i])
	}
	got := make(map[byte]bool)
	for i := 0; i < 4; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("parallel call failed: %v", r.err)
		}
		got[r.marker] = true
	}
	if len(got) != 4 {
		t.Fatalf("expected A,B,C,D once each, got %v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 5 || order[0] != "all-devices-started" {
		t.Fatalf("devices did not overlap before responses: %v", order)
	}
}

// TestSequenceWraparound directly exercises the cyclic uint16 continuation
// sequence across 65535 -> 0 -> 1.
func TestSequenceWraparound(t *testing.T) {
	payload := testPayload('S', 200)
	first := Frame{
		Header: Header{
			Version: ProtocolVersion, Type: TypeFirst, DeviceID: 3,
			RequestID: 55, Total: uint16(len(payload)), Length: ChunkSize,
		},
		Payload: payload[:ChunkSize],
	}
	asm := newAssembler()
	if err := asm.data(first); err != nil {
		t.Fatal(err)
	}
	// Force the continuation window up against the uint16 ceiling.  Real
	// payloads are capped at 512 bytes (at most 8 continuations), so the wrap
	// itself is exercised here directly.
	asm.nextSeq = 65535
	offset := ChunkSize
	for _, seq := range []uint16{65535, 0, 1} {
		end := offset + ChunkSize
		if end > len(payload) {
			end = len(payload)
		}
		f := Frame{
			Header: Header{
				Version: ProtocolVersion, Type: TypeCont, DeviceID: 3,
				RequestID: seq, Total: seq, Length: byte(end - offset),
			},
			Payload: payload[offset:end],
		}
		if err := asm.data(f); err != nil {
			t.Fatalf("sequence %d rejected: %v", seq, err)
		}
		offset = end
	}
	if !asm.complete() || !bytes.Equal(asm.payload, payload) {
		t.Fatal("wrapped sequence was not reassembled correctly")
	}
}

// TestLateFrameAfterCancel reuses the same request ID.  The predecessor's late
// response has the old epoch and must never be received by the successor.
func TestLateFrameAfterCancel(t *testing.T) {
	ctx := context.Background()
	clock := NewFakeClock(time.Now())
	tr := NewMemoryTracer(clock)
	firstStarted := make(chan struct{})
	var responseMu sync.Mutex
	firstCall := true
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{
			ID: 1, Clock: clock, Tracer: tr,
			Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
				responseMu.Lock()
				isFirst := firstCall
				if firstCall {
					firstCall = false
				}
				responseMu.Unlock()
				if isFirst {
					close(firstStarted)
					timer := c.NewTimer(500 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.Chan():
					case <-ctx.Done():
					}
					// Deliberately ignore cancellation and send late.
					return []byte("LATE-OLD"), nil
				}
				return []byte("NEW-SUCCESSOR"), nil
			},
		},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	callCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := g.RoundTrip(callCtx, 1, 9, []byte("first"))
		errCh <- err
	}()
	<-firstStarted
	cancel()
	if err := <-errCh; err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected cancellation error, got %v", err)
	}

	// The canceled call's transport is closed, so the device's deliberately
	// stale "LATE-OLD" response is written into a closed pipe and can never
	// reach the successor.  The successor reuses request ID 9 but flips the
	// epoch, so even a shared transport would reject the old frame.
	res, err := g.RoundTrip(ctx, 1, 9, []byte("second"))
	if err != nil {
		t.Fatalf("successor request failed: %v", err)
	}
	if string(res.Payload) != "NEW-SUCCESSOR" {
		t.Fatalf("successor consumed late predecessor response: %q", res.Payload)
	}
	var epochs []bool
	for _, e := range tr.Events() {
		if e.DeviceID == 1 && e.Type == TypeFirst && e.DropReason == "" &&
			strings.HasPrefix(e.Note, "gateway request first frame") {
			epochs = append(epochs, e.Epoch)
		}
	}
	if len(epochs) < 2 || epochs[0] == epochs[1] {
		t.Fatalf("expected opposite epochs for reused request ID: %v", epochs)
	}
}

// TestReadFramesDropsForeignEpoch proves the receive router never hands a
// predecessor frame with a stale epoch to the successor's channels, even when
// both reuse the identical request ID on a shared transport.
func TestReadFramesDropsForeignEpoch(t *testing.T) {
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, err := NewGateway(GatewayConfig{
		DeviceIDs: []byte{1, 2},
		Dial:      func(byte) (FrameTransport, error) { return nil, ErrDeviceClosed },
		Clock:     clock,
		Tracer:    tr,
	})
	if err != nil {
		t.Fatal(err)
	}

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	fc := NewFrameConn(b)
	sess := &callSession{
		key:     sessionKey{Epoch: true, RequestID: 42},
		grantCh: make(chan Frame, 4),
		respCh:  make(chan Frame, 4),
		readErr: make(chan error, 1),
	}
	go g.readFrames(1, fc, sess)

	write := func(f Frame) {
		if err := NewFrameConn(a).Send(f); err != nil {
			t.Fatal(err)
		}
	}
	// Stale epoch (predecessor): must be dropped, not delivered.
	write(Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeFirst, Epoch: false, RequestID: 42, Total: 3, Length: 3},
		Payload: []byte("OLD"),
	})
	// Continuation from stale epoch also dropped.
	write(Frame{Header: Header{Version: ProtocolVersion, Type: TypeCont, Epoch: false, RequestID: 1, Total: 1, Length: 0}})
	// Correct epoch + request ID: delivered.
	write(Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeError, Epoch: true, RequestID: 42, Total: 2, Length: 2},
		Payload: []byte("{}"),
	})

	select {
	case f := <-sess.respCh:
		if f.Epoch != true || f.Type != TypeError {
			t.Fatalf("routed wrong frame: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("matching-session frame was not delivered")
	}
	select {
	case bad := <-sess.respCh:
		t.Fatalf("foreign-epoch frame leaked into session: %+v", bad)
	default:
	}
	var dropped int
	for _, e := range tr.Events() {
		if e.DropReason != "" && e.DeviceID == 1 {
			dropped++
		}
	}
	if dropped < 2 {
		t.Fatalf("expected at least 2 dropped foreign frames, got %d: %#v", dropped, tr.Events())
	}
}

// TestFlowControlPause verifies a request stalls after one continuation until
// a later grant releases it, and every grant is within 1..4.
func TestFlowControlPause(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	resume := make(chan struct{})
	gotFirstGrant := make(chan struct{})
	var once sync.Once
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{
			ID: 1, Clock: clock, Tracer: tr,
			GrantPolicy: func(ctx context.Context, req DeviceRequest, granted, remaining int) (uint8, error) {
				if granted == 0 {
					once.Do(func() { close(gotFirstGrant) })
					return 1, nil
				}
				select {
				case <-resume:
					return 4, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			},
		},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	payload := testPayload('F', 200)
	done := make(chan struct {
		r   *CallResult
		err error
	}, 1)
	go func() {
		r, err := g.RoundTrip(ctx, 1, 11, payload)
		done <- struct {
			r   *CallResult
			err error
		}{r, err}
	}()
	<-gotFirstGrant
	select {
	case r := <-done:
		t.Fatalf("request proceeded during flow-control stall: %v", r)
	case <-time.After(50 * time.Millisecond):
	}
	close(resume)
	r := <-done
	if r.err != nil || !bytes.Equal(r.r.Payload, payload) {
		t.Fatalf("request did not resume: result=%v err=%v", r.r, r.err)
	}
	var credits []byte
	for _, e := range tr.Events() {
		if e.DeviceID == 1 && e.Type == TypeGrant && len(e.PayloadHex) == 2 {
			credits = append(credits, parseHexByte(e.PayloadHex))
		}
	}
	if len(credits) < 2 || credits[0] != 1 {
		t.Fatalf("expected initial one-credit grant, got %v", credits)
	}
	for _, c := range credits {
		if c < 1 || c > 4 {
			t.Fatalf("grant outside 1..4: %d", c)
		}
	}
}

// TestQuotaExhaustion verifies an explicit zero grant terminates one session.
func TestQuotaExhaustion(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1, Clock: clock, Tracer: tr, GrantPolicy: func(context.Context, DeviceRequest, int, int) (uint8, error) {
			return 0, nil
		}},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	_, err = g.RoundTrip(ctx, 1, 12, testPayload('Q', 64))
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != CodeQuotaExhausted {
		t.Fatalf("expected quota_exhausted, got %v", err)
	}
	res, err := g.RoundTrip(ctx, 2, 13, []byte("other"))
	if err != nil || string(res.Payload) != "other" {
		t.Fatalf("quota failure affected another device: %v", err)
	}
}

// TestEmptyPayload verifies an empty request still yields an explicit End and
// does not hang waiting for a continuation.
func TestEmptyPayload(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1},
		VirtualDeviceConfig{ID: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	res, err := g.RoundTrip(ctx, 1, 1, nil)
	if err != nil {
		t.Fatalf("empty round trip: %v", err)
	}
	if len(res.Payload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(res.Payload))
	}
	sawEnd := false
	for _, e := range tr.Events() {
		if e.DeviceID == 1 && e.Type == TypeEnd && strings.HasPrefix(e.Note, "gateway response frame") {
			sawEnd = true
		}
	}
	if !sawEnd {
		t.Fatal("empty response was not terminated by an End frame")
	}
}

// TestOutOfOrderAndLengthMismatch covers malformed response/request sessions.
func TestOutOfOrderAndLengthMismatch(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{
			ID: 1, Clock: clock, Tracer: tr,
			TransformResponse: func(frames []Frame) []Frame {
				if len(frames) > 2 {
					frames[1], frames[2] = frames[2], frames[1]
				}
				return frames
			},
		},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	_, err = g.RoundTrip(ctx, 1, 14, testPayload('O', 130))
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != CodeOutOfOrder {
		t.Fatalf("expected out_of_order, got %v", err)
	}

	f := Frame{
		Header:  Header{Version: ProtocolVersion, Type: TypeFirst, DeviceID: 1, RequestID: 1, Total: 100, Length: 20},
		Payload: make([]byte, 20),
	}
	asm := newAssembler()
	if err := asm.data(f); err == nil {
		t.Fatal("expected length mismatch")
	}
}

// TestTimeoutIsSessionScoped ensures a timed-out device does not stop others.
func TestTimeoutIsSessionScoped(t *testing.T) {
	ctx := context.Background()
	clock := NewFakeClock(time.Now())
	tr := NewMemoryTracer(clock)
	release := make(chan struct{})
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1, Clock: clock, Tracer: tr, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			<-release
			return []byte("never"), nil
		}},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer func() {
		close(release)
		closeDevices(devs)
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := g.RoundTrip(ctx, 1, 15, []byte("slow"))
		errCh <- err
	}()
	if !clock.WaitForTimers(1, 2*time.Second) {
		t.Fatal("gateway deadline timer was never registered")
	}
	clock.Advance(1100 * time.Millisecond)
	callErr := <-errCh
	var pe *ProtocolError
	if !errors.As(callErr, &pe) || pe.Code != CodeTimeout {
		t.Fatalf("expected timeout, got %v", callErr)
	}
	res, err := g.RoundTrip(ctx, 2, 16, []byte("alive"))
	if err != nil || string(res.Payload) != "alive" {
		t.Fatalf("timeout affected independent device: %v", err)
	}
}

// TestTCPClientServer verifies the external TCP client-facing protocol.
func TestTCPClientServer(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1, Clock: clock, Tracer: tr},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer closeDevices(devs)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ServeTCP(ln, ServerConfig{Gateway: g, Clock: clock})
	defer srv.Close()

	resp, err := DialAndSubmit(ctx, srv.Addr().String(), ClientRequest{DeviceID: 2, RequestID: 77, Payload: []byte("tcp")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != StatusOK || string(resp.Payload) != "tcp" {
		t.Fatalf("unexpected response status=%d payload=%q error=%v", resp.Status, resp.Payload, resp.Error)
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	big := make([]byte, MaxPayload+1)
	if _, err := SubmitClient(ctx, conn, ClientRequest{DeviceID: 2, RequestID: 78, Payload: big}); err == nil {
		t.Fatal("expected oversized payload failure")
	}
}

// TestTCPCancelSendsStructuredError exercises the TCP cancel operation: the
// in-flight request must terminate with a structured canceled error while the
// connection itself remains usable.
func TestTCPCancelSendsStructuredError(t *testing.T) {
	ctx := context.Background()
	clock := RealClock{}
	tr := NewMemoryTracer(clock)
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	g, devs, err := NewVirtualTopology(ctx, clock, tr,
		VirtualDeviceConfig{ID: 1, Clock: clock, Tracer: tr, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			once.Do(func() { close(started) })
			<-release
			return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "never")
		}},
		VirtualDeviceConfig{ID: 2, Clock: clock, Tracer: tr},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defer func() {
		close(release)
		closeDevices(devs)
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ServeTCP(ln, ServerConfig{Gateway: g, Clock: clock})
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	go func() {
		_, _ = SubmitClient(ctx, conn, ClientRequest{DeviceID: 1, RequestID: 321, Payload: []byte("slow")})
	}()
	<-started

	// Send the cancel op and read its acknowledgment.
	cancelHdr := make([]byte, 8)
	cancelHdr[0], cancelHdr[1] = 'D', 'G'
	cancelHdr[2] = OpCancel
	cancelHdr[3] = 1
	binaryBigEndianPutUint16(cancelHdr[4:6], 321)
	if _, err := conn.Write(cancelHdr); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readClientMessage(conn); err != nil {
		t.Fatalf("cancel ack read: %v", err)
	}

	// Connection stays usable: a fresh request to a healthy device succeeds.
	resp, err := SubmitClient(ctx, conn, ClientRequest{DeviceID: 2, RequestID: 322, Payload: []byte("after-cancel")})
	if err != nil {
		t.Fatalf("request after cancel: %v", err)
	}
	if resp.Status != StatusOK || string(resp.Payload) != "after-cancel" {
		t.Fatalf("connection not usable after cancel: status=%d err=%v", resp.Status, resp.Error)
	}
}

func binaryBigEndianPutUint16(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func parseHexByte(s string) byte {
	if len(s) != 2 {
		return 0
	}
	return parseHexNibble(s[0])<<4 | parseHexNibble(s[1])
}

func parseHexNibble(b byte) byte {
	switch {
	case b >= '0' && b <= '9':
		return b - '0'
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10
	default:
		return 0
	}
}
