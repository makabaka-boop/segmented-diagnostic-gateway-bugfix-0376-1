package diaggate

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Client-side wire format is intentionally simple and independent of the
// eight-byte virtual-device frame protocol:
//
//   magic          2 bytes: "DG"
//   operation      1 byte:  1=request, 2=cancel (status on responses)
//   device ID      1 byte
//   request ID     2 bytes big-endian
//   payload length 2 bytes big-endian (0..512)
//   payload        N bytes
//
// Responses use the same header layout: the operation byte carries the
// status and the device ID / request ID fields echo the request the
// response belongs to, so interleaved and out-of-order responses on a
// shared connection stay attributable.
//
// In-flight requests are keyed by (device ID, request ID): the same
// request ID may be active once per device, and a cancel only affects the
// exact pair it names on that TCP connection.  Every request produces
// exactly one response; canceling an in-flight request makes its own
// terminal response the canceled one, while canceling an unknown pair is
// acknowledged idempotently.

const (
	clientMagic0 = 'D'
	clientMagic1 = 'G'

	OpRequest byte = 1
	OpCancel  byte = 2

	StatusOK       byte = 1
	StatusError    byte = 2
	StatusCanceled byte = 3
	StatusProtocol byte = 4
)

type ClientRequest struct {
	DeviceID  byte
	RequestID uint16
	Payload   []byte
}

type ClientHeader struct {
	Op        byte
	DeviceID  byte
	RequestID uint16
	Length    uint16
}

type ClientResponse struct {
	Status    byte           `json:"-"`
	DeviceID  byte           `json:"-"`
	RequestID uint16         `json:"-"`
	Payload   []byte         `json:"-"`
	Error     *ProtocolError `json:"error,omitempty"`
}

// clientRequestKey identifies one in-flight request on a client connection.
// Request IDs are scoped per device, so the same ID may be active once per
// device on the same connection.
type clientRequestKey struct {
	DeviceID  byte
	RequestID uint16
}

// pendingRequest is one registered in-flight request.  The pointer itself is
// the map value so cleanup can verify it removes its own entry and not a
// successor that reused the same key.
type pendingRequest struct {
	cancel context.CancelFunc
}

type ServerConfig struct {
	Gateway *Gateway
	Clock   Clock

	// Tracer, when set, additionally records client-facing events
	// (client->gateway requests/cancels, gateway->client responses) so a
	// client result can be joined with the device frame trail by
	// (device ID, request ID).
	Tracer Tracer
}

type Server struct {
	cfg ServerConfig
	ln  net.Listener

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func ServeTCP(ln net.Listener, cfg ServerConfig) *Server {
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = NopTracer{}
	}
	s := &Server{cfg: cfg, ln: ln}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *Server) Addr() net.Addr { return s.ln.Addr() }

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	err := s.ln.Close()
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer c.Close()
			s.serveConn(context.Background(), c)
		}(conn)
	}
}

func (s *Server) serveConn(parent context.Context, conn net.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		pendingMu sync.Mutex
		pending   = make(map[clientRequestKey]*pendingRequest)
		writeMu   sync.Mutex
		wg        sync.WaitGroup
	)
	cancelAll := func() {
		pendingMu.Lock()
		for _, p := range pending {
			p.cancel()
		}
		pending = make(map[clientRequestKey]*pendingRequest)
		pendingMu.Unlock()
	}
	// On connection teardown cancel in-flight requests first, then wait for
	// their goroutines to finish (their writes will fail harmlessly).
	defer wg.Wait()
	defer cancelAll()

	send := func(resp ClientResponse) error {
		s.cfg.Tracer.Record(FrameEvent{
			Direction:  DirectionGatewayToClient,
			DeviceID:   resp.DeviceID,
			RequestID:  resp.RequestID,
			Total:      uint16(len(resp.Payload)),
			PayloadHex: payloadHex(resp.Payload),
			Note:       clientResponseNote(resp),
		})
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeClientResponse(conn, resp)
	}

	for {
		hdr, payload, err := readLimitedClientMessage(conn, MaxPayload)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = send(ClientResponse{Status: StatusProtocol, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Error: &ProtocolError{Code: CodeMalformedFrame, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Message: err.Error()}})
			return
		}
		key := clientRequestKey{DeviceID: hdr.DeviceID, RequestID: hdr.RequestID}

		if hdr.Op == OpCancel {
			s.cfg.Tracer.Record(FrameEvent{Direction: DirectionClientToGateway, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Note: "client cancel"})
			pendingMu.Lock()
			p := pending[key]
			pendingMu.Unlock()
			if p != nil {
				// The request's own terminal response acknowledges the
				// cancel, so every request produces exactly one response.
				p.cancel()
			} else {
				// Canceling a pair that is not active on this connection is
				// a harmless no-op, acknowledged idempotently.
				_ = send(ClientResponse{Status: StatusCanceled, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Error: &ProtocolError{Code: CodeCanceled, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Message: "no active request for this device and request ID"}})
			}
			continue
		}
		if hdr.Op != OpRequest {
			_ = send(ClientResponse{Status: StatusProtocol, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Error: frameError(hdr.DeviceID, hdr.RequestID, CodeMalformedFrame, "unknown operation %d", hdr.Op)})
			return
		}

		reqCtx, reqCancel := context.WithCancel(ctx)
		entry := &pendingRequest{cancel: reqCancel}
		pendingMu.Lock()
		if _, exists := pending[key]; exists {
			pendingMu.Unlock()
			reqCancel()
			_ = send(ClientResponse{Status: StatusError, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Error: frameError(hdr.DeviceID, hdr.RequestID, CodeUnavailable, "request ID %d is already active for device %d on this connection", hdr.RequestID, hdr.DeviceID)})
			continue
		}
		pending[key] = entry
		pendingMu.Unlock()
		s.cfg.Tracer.Record(FrameEvent{Direction: DirectionClientToGateway, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Total: uint16(len(payload)), PayloadHex: payloadHex(payload), Note: "client request"})
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer reqCancel()
			defer func() {
				pendingMu.Lock()
				if pending[key] == entry {
					delete(pending, key)
				}
				pendingMu.Unlock()
			}()

			result, callErr := s.cfg.Gateway.RoundTrip(reqCtx, hdr.DeviceID, hdr.RequestID, payload)

			// Free the (device, request) pair before responding so the
			// client may reuse it as soon as it reads this response.
			pendingMu.Lock()
			if pending[key] == entry {
				delete(pending, key)
			}
			pendingMu.Unlock()

			if callErr != nil {
				resp := ClientResponse{Status: StatusError, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID}
				var pe *ProtocolError
				if errors.As(callErr, &pe) {
					if pe.Code == CodeCanceled {
						resp.Status = StatusCanceled
					}
					resp.Error = pe
				} else if errors.Is(callErr, context.Canceled) {
					resp.Status = StatusCanceled
					resp.Error = frameError(hdr.DeviceID, hdr.RequestID, CodeCanceled, callErr.Error())
				} else {
					resp.Error = frameError(hdr.DeviceID, hdr.RequestID, CodeDeviceError, callErr.Error())
				}
				_ = send(resp)
				return
			}
			_ = send(ClientResponse{Status: StatusOK, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Payload: result.Payload})
		}()
	}
}

func clientResponseNote(r ClientResponse) string {
	if r.Error != nil {
		return fmt.Sprintf("client response status=%d code=%s", r.Status, r.Error.Code)
	}
	return fmt.Sprintf("client response status=%d", r.Status)
}

func readLimitedClientMessage(r io.Reader, maxPayload uint16) (ClientHeader, []byte, error) {
	h, payload, err := readClientMessage(r)
	if err == nil && h.Length > maxPayload {
		return h, nil, ErrPayloadTooLarge
	}
	return h, payload, err
}

func readClientMessage(r io.Reader) (ClientHeader, []byte, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return ClientHeader{}, nil, err
	}
	if b[0] != clientMagic0 || b[1] != clientMagic1 {
		return ClientHeader{}, nil, errors.New("invalid client magic")
	}
	h := ClientHeader{
		Op:        b[2],
		DeviceID:  b[3],
		RequestID: binary.BigEndian.Uint16(b[4:6]),
		Length:    binary.BigEndian.Uint16(b[6:8]),
	}
	if h.Length > MaxPayload {
		return h, nil, ErrPayloadTooLarge
	}
	payload := make([]byte, h.Length)
	if h.Length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return h, nil, err
		}
	}
	return h, payload, nil
}

func writeClientResponse(w io.Writer, r ClientResponse) error {
	var body []byte
	status := r.Status
	if r.Error != nil {
		body, _ = json.Marshal(struct {
			Error *ProtocolError `json:"error"`
		}{Error: r.Error})
	} else {
		body = r.Payload
		status = StatusOK
	}
	if len(body) > 1<<16-1 {
		body = body[:1<<16-1]
	}
	hdr := make([]byte, 8)
	hdr[0] = clientMagic0
	hdr[1] = clientMagic1
	hdr[2] = status
	hdr[3] = r.DeviceID
	binary.BigEndian.PutUint16(hdr[4:6], r.RequestID)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(body)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(body) > 0 {
		_, err := w.Write(body)
		return err
	}
	return nil
}

// SubmitClient sends one request over an existing TCP connection and reads
// exactly one response.  It is only safe while no other request is in flight
// on the same connection; use Client for concurrent or interleaved use.
func SubmitClient(ctx context.Context, conn net.Conn, req ClientRequest) (ClientResponse, error) {
	type result struct {
		resp ClientResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := submitClient(conn, req)
		ch <- result{resp, err}
	}()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return ClientResponse{}, ctx.Err()
	case r := <-ch:
		return r.resp, r.err
	}
}

func submitClient(conn net.Conn, req ClientRequest) (ClientResponse, error) {
	if len(req.Payload) > MaxPayload {
		return ClientResponse{}, ErrPayloadTooLarge
	}
	hdr := make([]byte, 8)
	hdr[0] = clientMagic0
	hdr[1] = clientMagic1
	hdr[2] = OpRequest
	hdr[3] = req.DeviceID
	binary.BigEndian.PutUint16(hdr[4:6], req.RequestID)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(req.Payload)))
	if _, err := conn.Write(hdr); err != nil {
		return ClientResponse{}, err
	}
	if len(req.Payload) > 0 {
		if _, err := conn.Write(req.Payload); err != nil {
			return ClientResponse{}, err
		}
	}
	h, body, err := readClientMessage(conn)
	if err != nil {
		return ClientResponse{}, err
	}
	resp := ClientResponse{Status: h.Op, DeviceID: h.DeviceID, RequestID: h.RequestID}
	if h.Op == StatusOK {
		resp.Payload = body
		return resp, nil
	}
	var envelope struct {
		Error *ProtocolError `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error != nil {
		resp.Error = envelope.Error
	}
	return resp, nil
}

// DialAndSubmit is a convenience used by tests and example clients.
func DialAndSubmit(ctx context.Context, address string, req ClientRequest) (ClientResponse, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return ClientResponse{}, err
	}
	defer conn.Close()
	return SubmitClient(ctx, conn, req)
}

// Client multiplexes concurrent diagnostic requests over a single TCP
// connection.  Requests for different devices may reuse the same request ID
// and may complete out of order; a single read loop matches every response
// to its caller by the (device ID, request ID) pair echoed in the response
// header.  Writes are serialized, so concurrent Submit/Cancel calls cannot
// interleave bytes on the wire.
type Client struct {
	conn    net.Conn
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[clientRequestKey]chan clientResult
	readErr error
}

type clientResult struct {
	resp ClientResponse
	err  error
}

// NewClient wraps conn and starts its response demultiplexer.
func NewClient(conn net.Conn) *Client {
	c := &Client{conn: conn, pending: make(map[clientRequestKey]chan clientResult)}
	go c.readLoop()
	return c
}

// Submit sends one request and waits for its response.  At most one request
// per (device ID, request ID) pair may be outstanding on a Client.
func (c *Client) Submit(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	identity := ClientResponse{DeviceID: req.DeviceID, RequestID: req.RequestID}
	if len(req.Payload) > MaxPayload {
		return identity, ErrPayloadTooLarge
	}
	key := clientRequestKey{DeviceID: req.DeviceID, RequestID: req.RequestID}
	ch := make(chan clientResult, 1)
	c.mu.Lock()
	if c.readErr != nil {
		err := c.readErr
		c.mu.Unlock()
		return identity, err
	}
	if _, dup := c.pending[key]; dup {
		c.mu.Unlock()
		return identity, &ProtocolError{Code: CodeUnavailable, DeviceID: req.DeviceID, RequestID: req.RequestID, Message: "request ID is already active for this device on this connection"}
	}
	c.pending[key] = ch
	c.mu.Unlock()

	if err := c.writeMessage(OpRequest, req.DeviceID, req.RequestID, req.Payload); err != nil {
		c.removePending(key, ch)
		return identity, err
	}

	select {
	case r := <-ch:
		return r.resp, r.err
	case <-ctx.Done():
		if c.removePending(key, ch) {
			// Best-effort scoped cancel so the server-side session ends too.
			_ = c.writeMessage(OpCancel, req.DeviceID, req.RequestID, nil)
		}
		return identity, ctx.Err()
	}
}

// Cancel asks the server to cancel the request currently active for
// (deviceID, requestID) on this connection.  The corresponding Submit, if
// any, resolves with a StatusCanceled response.  Canceling a pair that is
// not active is a harmless no-op.
func (c *Client) Cancel(deviceID byte, requestID uint16) error {
	return c.writeMessage(OpCancel, deviceID, requestID, nil)
}

// Close shuts the connection; pending and future Submits fail with the
// resulting read error.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) removePending(key clientRequestKey, ch chan clientResult) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending[key] == ch {
		delete(c.pending, key)
		return true
	}
	return false
}

func (c *Client) writeMessage(op byte, deviceID byte, requestID uint16, payload []byte) error {
	hdr := make([]byte, 8)
	hdr[0] = clientMagic0
	hdr[1] = clientMagic1
	hdr[2] = op
	hdr[3] = deviceID
	binary.BigEndian.PutUint16(hdr[4:6], requestID)
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(payload)))
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.conn.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) readLoop() {
	for {
		h, body, err := readClientMessage(c.conn)
		if err != nil {
			c.failAll(err)
			return
		}
		resp := ClientResponse{Status: h.Op, DeviceID: h.DeviceID, RequestID: h.RequestID}
		if h.Op == StatusOK {
			resp.Payload = body
		} else {
			var envelope struct {
				Error *ProtocolError `json:"error"`
			}
			if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error != nil {
				resp.Error = envelope.Error
			}
		}
		key := clientRequestKey{DeviceID: h.DeviceID, RequestID: h.RequestID}
		c.mu.Lock()
		ch := c.pending[key]
		delete(c.pending, key)
		c.mu.Unlock()
		if ch != nil {
			ch <- clientResult{resp: resp}
		}
		// Responses for unknown keys (for example the idempotent
		// acknowledgment of a cancel this client already forgot about)
		// are dropped.
	}
}

func (c *Client) failAll(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	pending := c.pending
	c.pending = make(map[clientRequestKey]chan clientResult)
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- clientResult{err: err}
	}
}
