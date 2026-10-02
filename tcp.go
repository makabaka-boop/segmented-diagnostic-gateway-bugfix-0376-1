package diaggate

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
)

// Client-side wire format is intentionally simple and independent of the
// eight-byte virtual-device frame protocol:
//
//   magic          2 bytes: "DG"
//   operation      1 byte:  1=request, 2=cancel
//   device ID      1 byte
//   request ID     2 bytes big-endian
//   payload length 2 bytes big-endian (0..512)
//   payload        N bytes
//
// Responses are the same header followed by status and body.  A cancel only
// affects the request currently active on that TCP connection.

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
	Status  byte           `json:"-"`
	Payload []byte         `json:"-"`
	Error   *ProtocolError `json:"error,omitempty"`
}

type ServerConfig struct {
	Gateway *Gateway
	Clock   Clock
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
		pending   = make(map[uint16]context.CancelFunc)
		writeMu   sync.Mutex
		wg        sync.WaitGroup
	)
	cancelAll := func() {
		pendingMu.Lock()
		for _, cancelRequest := range pending {
			cancelRequest()
		}
		pending = make(map[uint16]context.CancelFunc)
		pendingMu.Unlock()
	}
	defer cancelAll()
	defer wg.Wait()

	send := func(resp ClientResponse) error {
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
			_ = send(ClientResponse{Status: StatusProtocol, Error: &ProtocolError{Code: CodeMalformedFrame, Message: err.Error()}})
			return
		}

		if hdr.Op == OpCancel {
			pendingMu.Lock()
			cancelRequest := pending[hdr.RequestID]
			pendingMu.Unlock()
			if cancelRequest != nil {
				cancelRequest()
			}
			_ = send(ClientResponse{Status: StatusCanceled, Error: &ProtocolError{Code: CodeCanceled, DeviceID: hdr.DeviceID, RequestID: hdr.RequestID, Message: "cancel signaled"}})
			continue
		}
		if hdr.Op != OpRequest {
			_ = send(ClientResponse{Status: StatusProtocol, Error: frameError(hdr.DeviceID, hdr.RequestID, CodeMalformedFrame, "unknown operation %d", hdr.Op)})
			return
		}

		reqCtx, reqCancel := context.WithCancel(ctx)
		pendingMu.Lock()
		if _, exists := pending[hdr.RequestID]; exists {
			pendingMu.Unlock()
			reqCancel()
			_ = send(ClientResponse{Status: StatusError, Error: frameError(hdr.DeviceID, hdr.RequestID, CodeUnavailable, "duplicate active request ID on this connection")})
			continue
		}
		pending[hdr.RequestID] = reqCancel
		pendingMu.Unlock()
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer reqCancel()
			defer func() {
				pendingMu.Lock()
				if pending[hdr.RequestID] != nil {
					delete(pending, hdr.RequestID)
				}
				pendingMu.Unlock()
			}()

			result, callErr := s.cfg.Gateway.RoundTrip(reqCtx, hdr.DeviceID, hdr.RequestID, payload)
			if callErr != nil {
				resp := ClientResponse{Status: StatusError}
				var pe *ProtocolError
				if errors.As(callErr, &pe) {
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
			_ = send(ClientResponse{Status: StatusOK, Payload: result.Payload})
		}()
	}
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
	binary.BigEndian.PutUint16(hdr[4:6], 0)
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
// exactly one response.
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
	resp := ClientResponse{Status: h.Op}
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
