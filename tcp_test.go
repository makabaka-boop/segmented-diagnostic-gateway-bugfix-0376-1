package diaggate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// tcpTestServer builds a two-device topology behind a TCP server and returns
// a multiplexed client connected to it.
func tcpTestServer(t *testing.T, tr Tracer, configs ...VirtualDeviceConfig) (*Client, *Server, func()) {
	t.Helper()
	ctx := context.Background()
	clock := RealClock{}
	g, devs, err := NewVirtualTopologyWith(5*time.Second, ctx, clock, tr, configs...)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ServeTCP(ln, ServerConfig{Gateway: g, Clock: clock, Tracer: tr})
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn)
	cleanup := func() {
		_ = client.Close()
		_ = srv.Close()
		_ = g.Close()
		closeDevices(devs)
	}
	return client, srv, cleanup
}

type submitOutcome struct {
	resp ClientResponse
	err  error
}

func submitAsync(ctx context.Context, c *Client, req ClientRequest) chan submitOutcome {
	ch := make(chan submitOutcome, 1)
	go func() {
		resp, err := c.Submit(ctx, req)
		ch <- submitOutcome{resp, err}
	}()
	return ch
}

func waitOutcome(t *testing.T, ch chan submitOutcome, what string) submitOutcome {
	t.Helper()
	select {
	case o := <-ch:
		if o.err != nil {
			t.Fatalf("%s: %v", what, o.err)
		}
		return o
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: no response within 2s", what)
		return submitOutcome{}
	}
}

// TestTCPInterleavedSameRequestID is the scenario's core case: one TCP
// connection interleaves the same request ID to two devices.  Neither request
// may be rejected as a duplicate, and each out-of-order response must be
// attributable to its own request.
func TestTCPInterleavedSameRequestID(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTracer(RealClock{})
	release1 := make(chan struct{})
	started1 := make(chan struct{})
	var once sync.Once
	client, _, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			once.Do(func() { close(started1) })
			select {
			case <-release1:
			case <-ctx.Done():
				return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "canceled")
			}
			return []byte("dev1:" + string(req.Payload)), nil
		}},
		VirtualDeviceConfig{ID: 2, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			return []byte("dev2:" + string(req.Payload)), nil
		}},
	)
	defer cleanup()

	slow := submitAsync(ctx, client, ClientRequest{DeviceID: 1, RequestID: 7, Payload: []byte("task")})
	<-started1 // device 1 now holds request 7 mid-flight
	fast := submitAsync(ctx, client, ClientRequest{DeviceID: 2, RequestID: 7, Payload: []byte("task")})

	// Device 2 answers first although its request was submitted second: the
	// reused request ID must not be misjudged as a duplicate, and the
	// response must be matched by (device, request), not arrival order.
	o2 := waitOutcome(t, fast, "device 2 request with reused ID")
	if o2.resp.Status != StatusOK || string(o2.resp.Payload) != "dev2:task" {
		t.Fatalf("device 2 response: status=%d payload=%q err=%v", o2.resp.Status, o2.resp.Payload, o2.resp.Error)
	}
	if o2.resp.DeviceID != 2 || o2.resp.RequestID != 7 {
		t.Fatalf("device 2 response not attributable: device=%d request=%d", o2.resp.DeviceID, o2.resp.RequestID)
	}

	// Device 1 is still blocked: its response must not have arrived early.
	select {
	case o := <-slow:
		t.Fatalf("device 1 responded while blocked: %+v", o.resp)
	case <-time.After(50 * time.Millisecond):
	}
	close(release1)
	o1 := waitOutcome(t, slow, "device 1 request")
	if o1.resp.Status != StatusOK || string(o1.resp.Payload) != "dev1:task" {
		t.Fatalf("device 1 response: status=%d payload=%q err=%v", o1.resp.Status, o1.resp.Payload, o1.resp.Error)
	}
	if o1.resp.DeviceID != 1 || o1.resp.RequestID != 7 {
		t.Fatalf("device 1 response not attributable: device=%d request=%d", o1.resp.DeviceID, o1.resp.RequestID)
	}

	// The trace joins client requests to device frames by (device, request).
	clientReqs := map[byte]int{}
	deviceFirsts := map[byte]int{}
	for _, e := range tr.Events() {
		if e.RequestID != 7 {
			continue
		}
		if e.Direction == DirectionClientToGateway && e.Note == "client request" {
			clientReqs[e.DeviceID]++
		}
		if e.Direction == DirectionGatewayToDevice && e.Type == TypeFirst &&
			strings.HasPrefix(e.Note, "gateway request first frame") {
			deviceFirsts[e.DeviceID]++
		}
	}
	if clientReqs[1] != 1 || clientReqs[2] != 1 || deviceFirsts[1] != 1 || deviceFirsts[2] != 1 {
		t.Fatalf("trace does not attribute both requests: client=%v device=%v", clientReqs, deviceFirsts)
	}
}

// TestTCPCancelScopedToDevice cancels one of two in-flight requests that
// share a request ID on one connection; the other device's session must be
// unaffected.
func TestTCPCancelScopedToDevice(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTracer(RealClock{})
	release1 := make(chan struct{})
	started1, started2 := make(chan struct{}), make(chan struct{})
	var once1, once2 sync.Once
	client, _, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			once1.Do(func() { close(started1) })
			select {
			case <-release1:
				return []byte("dev1-done"), nil
			case <-ctx.Done():
				return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "canceled")
			}
		}},
		VirtualDeviceConfig{ID: 2, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			once2.Do(func() { close(started2) })
			<-ctx.Done()
			return nil, frameError(req.DeviceID, req.RequestID, CodeCanceled, "canceled")
		}},
	)
	defer cleanup()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release1) })

	victim := submitAsync(ctx, client, ClientRequest{DeviceID: 2, RequestID: 9, Payload: []byte("victim")})
	bystander := submitAsync(ctx, client, ClientRequest{DeviceID: 1, RequestID: 9, Payload: []byte("bystander")})
	<-started1
	<-started2

	if err := client.Cancel(2, 9); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	o2 := waitOutcome(t, victim, "canceled request on device 2")
	if o2.resp.Status != StatusCanceled {
		t.Fatalf("expected canceled status, got %+v", o2.resp)
	}
	if o2.resp.DeviceID != 2 || o2.resp.RequestID != 9 {
		t.Fatalf("cancel attributed to wrong request: device=%d request=%d", o2.resp.DeviceID, o2.resp.RequestID)
	}
	if o2.resp.Error == nil || o2.resp.Error.Code != CodeCanceled ||
		o2.resp.Error.DeviceID != 2 || o2.resp.Error.RequestID != 9 {
		t.Fatalf("structured cancel error not attributable: %+v", o2.resp.Error)
	}

	// The same request ID on device 1 must still be running.
	select {
	case o := <-bystander:
		t.Fatalf("cancel of (2,9) affected device 1 session: %+v", o.resp)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release1) })
	o1 := waitOutcome(t, bystander, "device 1 request after scoped cancel")
	if o1.resp.Status != StatusOK || string(o1.resp.Payload) != "dev1-done" {
		t.Fatalf("device 1 session did not complete normally: %+v", o1.resp)
	}
}

// TestTCPRequestIDReuseAfterCompletion reuses the same (device, request ID)
// pair back to back on one connection.
func TestTCPRequestIDReuseAfterCompletion(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTracer(RealClock{})
	var mu sync.Mutex
	runs := 0
	client, _, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			mu.Lock()
			runs++
			n := runs
			mu.Unlock()
			return []byte(fmt.Sprintf("run-%d:%s", n, req.Payload)), nil
		}},
		VirtualDeviceConfig{ID: 2},
	)
	defer cleanup()

	for i := 1; i <= 3; i++ {
		resp, err := client.Submit(ctx, ClientRequest{DeviceID: 1, RequestID: 5, Payload: []byte("again")})
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		want := fmt.Sprintf("run-%d:again", i)
		if resp.Status != StatusOK || string(resp.Payload) != want {
			t.Fatalf("run %d: status=%d payload=%q err=%v", i, resp.Status, resp.Payload, resp.Error)
		}
		if resp.DeviceID != 1 || resp.RequestID != 5 {
			t.Fatalf("run %d: response not attributable: %+v", i, resp)
		}
	}
}

// TestTCPReuseAfterCancelIgnoresLateFrames cancels a request whose device
// deliberately answers late, then reuses the same (device, request ID) pair
// on the same connection.  The successor must get its own response; the late
// frames of the canceled call must not reach it.
func TestTCPReuseAfterCancelIgnoresLateFrames(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTracer(RealClock{})
	lateRelease := make(chan struct{})
	firstStarted := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	client, _, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				close(firstStarted)
				<-lateRelease // deliberately ignore cancellation and answer late
				return []byte("LATE-OLD"), nil
			}
			return []byte("FRESH"), nil
		}},
		VirtualDeviceConfig{ID: 2},
	)
	defer cleanup()
	var lateOnce sync.Once
	defer lateOnce.Do(func() { close(lateRelease) })

	first := submitAsync(ctx, client, ClientRequest{DeviceID: 1, RequestID: 6, Payload: []byte("first")})
	<-firstStarted
	if err := client.Cancel(1, 6); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	o1 := waitOutcome(t, first, "canceled first request")
	if o1.resp.Status != StatusCanceled || o1.resp.DeviceID != 1 || o1.resp.RequestID != 6 {
		t.Fatalf("first request did not resolve as an attributed cancel: %+v", o1.resp)
	}

	// The same pair is reusable immediately after the cancel resolves.
	resp2, err := client.Submit(ctx, ClientRequest{DeviceID: 1, RequestID: 6, Payload: []byte("second")})
	if err != nil {
		t.Fatalf("successor submit: %v", err)
	}
	if resp2.Status != StatusOK || string(resp2.Payload) != "FRESH" {
		t.Fatalf("successor consumed wrong response: status=%d payload=%q", resp2.Status, resp2.Payload)
	}

	// Let the canceled device call emit its late response; the connection and
	// the next request (same ID, other device) must be unaffected.
	lateOnce.Do(func() { close(lateRelease) })
	time.Sleep(20 * time.Millisecond)
	resp3, err := client.Submit(ctx, ClientRequest{DeviceID: 2, RequestID: 6, Payload: []byte("third")})
	if err != nil {
		t.Fatalf("request after late frames: %v", err)
	}
	if resp3.Status != StatusOK || string(resp3.Payload) != "third" {
		t.Fatalf("late frames corrupted the connection: status=%d payload=%q", resp3.Status, resp3.Payload)
	}
}

// TestTCPStructuredErrorAttribution interleaves a failing and a succeeding
// request with the same ID; each side of the result must carry its own
// (device, request) identity.
func TestTCPStructuredErrorAttribution(t *testing.T) {
	ctx := context.Background()
	tr := NewMemoryTracer(RealClock{})
	client, _, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, GrantPolicy: func(context.Context, DeviceRequest, int, int) (uint8, error) {
			return 0, nil // quota_exhausted for any multi-frame request
		}},
		VirtualDeviceConfig{ID: 2},
	)
	defer cleanup()

	failing := submitAsync(ctx, client, ClientRequest{DeviceID: 1, RequestID: 8, Payload: testPayload('E', 200)})
	succeeding := submitAsync(ctx, client, ClientRequest{DeviceID: 2, RequestID: 8, Payload: []byte("fine")})

	o1 := waitOutcome(t, failing, "failing request")
	if o1.resp.Status != StatusError || o1.resp.DeviceID != 1 || o1.resp.RequestID != 8 {
		t.Fatalf("error response not attributable: %+v", o1.resp)
	}
	if o1.resp.Error == nil || o1.resp.Error.Code != CodeQuotaExhausted ||
		o1.resp.Error.DeviceID != 1 || o1.resp.Error.RequestID != 8 {
		t.Fatalf("structured error not attributable: %+v", o1.resp.Error)
	}

	o2 := waitOutcome(t, succeeding, "succeeding request")
	if o2.resp.Status != StatusOK || string(o2.resp.Payload) != "fine" ||
		o2.resp.DeviceID != 2 || o2.resp.RequestID != 8 {
		t.Fatalf("success response not attributable: %+v", o2.resp)
	}
}

// TestTCPDuplicateSameDeviceRejected pins the remaining duplicate rule on the
// raw protocol: the same (device, request ID) pair may be active only once
// per connection, and the rejection is an attributed structured error.
func TestTCPDuplicateSameDeviceRejected(t *testing.T) {
	tr := NewMemoryTracer(RealClock{})
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	_, srv, cleanup := tcpTestServer(t, tr,
		VirtualDeviceConfig{ID: 1, Respond: func(ctx context.Context, c Clock, req DeviceRequest) ([]byte, *ProtocolError) {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-ctx.Done():
			}
			return []byte("done"), nil
		}},
		VirtualDeviceConfig{ID: 2},
	)
	defer cleanup()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	// Raw connection: the multiplexing Client rejects duplicates locally, so
	// exercise the server-side rule directly.
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeRaw := func(op, device byte, requestID uint16, payload []byte) {
		t.Helper()
		hdr := []byte{'D', 'G', op, device, byte(requestID >> 8), byte(requestID), byte(len(payload) >> 8), byte(len(payload))}
		if _, err := conn.Write(append(hdr, payload...)); err != nil {
			t.Fatal(err)
		}
	}
	writeRaw(OpRequest, 1, 4, []byte("x"))
	<-started
	writeRaw(OpRequest, 1, 4, []byte("x"))

	// The rejection arrives first, attributed to (1, 4).
	h, body, err := readClientMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if h.Op != StatusError || h.DeviceID != 1 || h.RequestID != 4 {
		t.Fatalf("duplicate rejection not attributable: op=%d device=%d request=%d", h.Op, h.DeviceID, h.RequestID)
	}
	var envelope struct {
		Error *ProtocolError `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error == nil ||
		envelope.Error.Code != CodeUnavailable || envelope.Error.DeviceID != 1 || envelope.Error.RequestID != 4 {
		t.Fatalf("duplicate rejection is not an attributable structured error: %v %+v", err, envelope.Error)
	}

	// The original request is undisturbed and completes once released.
	releaseOnce.Do(func() { close(release) })
	h, body, err = readClientMessage(conn)
	if err != nil {
		t.Fatal(err)
	}
	if h.Op != StatusOK || h.DeviceID != 1 || h.RequestID != 4 || string(body) != "done" {
		t.Fatalf("original request disturbed by duplicate rejection: op=%d body=%q", h.Op, body)
	}
}
