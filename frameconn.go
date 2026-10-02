package diaggate

import (
	"io"
	"net"
	"sync"
)

// FrameConn is a synchronous eight-byte-header frame transport over a byte
// stream.  The header's final byte gives the payload length, so Recv is
// stateless and cannot be poisoned by frames from a canceled predecessor.
type FrameConn struct {
	conn   net.Conn
	writeM sync.Mutex
}

func NewFrameConn(conn net.Conn) *FrameConn { return &FrameConn{conn: conn} }

func (f *FrameConn) Send(frame Frame) error {
	b, err := frame.marshal()
	if err != nil {
		return err
	}
	f.writeM.Lock()
	defer f.writeM.Unlock()
	_, err = f.conn.Write(b)
	if err != nil {
		return mapNetError(err)
	}
	return nil
}

func (f *FrameConn) Recv() (Frame, error) {
	var hb [8]byte
	if _, err := io.ReadFull(f.conn, hb[:]); err != nil {
		return Frame{}, mapNetError(err)
	}
	h, err := decodeHeader(hb[:])
	if err != nil {
		return Frame{}, err
	}
	payload := make([]byte, h.Length)
	if h.Length > 0 {
		if _, err := io.ReadFull(f.conn, payload); err != nil {
			return Frame{}, mapNetError(err)
		}
	}
	return Frame{Header: h, Payload: payload}, nil
}

func (f *FrameConn) Close() error         { return f.conn.Close() }
func (f *FrameConn) LocalAddr() net.Addr  { return f.conn.LocalAddr() }
func (f *FrameConn) RemoteAddr() net.Addr { return f.conn.RemoteAddr() }

type streamListener struct{ ch chan net.Conn }

func newStreamListener(buffer int) *streamListener {
	return &streamListener{ch: make(chan net.Conn, buffer)}
}

func (l *streamListener) Accept() (net.Conn, error) {
	conn, ok := <-l.ch
	if !ok {
		return nil, ErrDeviceClosed
	}
	return conn, nil
}

func (l *streamListener) Close() error {
	close(l.ch)
	return nil
}

func (l *streamListener) Addr() net.Addr { return dummyAddr{} }

func (l *streamListener) Dial() (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.ch <- server:
		return client, nil
	default:
		server.Close()
		client.Close()
		return nil, &ProtocolError{Code: CodeUnavailable, Message: "device listener is full"}
	}
}

type dummyAddr struct{}

func (dummyAddr) Network() string { return "virtual" }
func (dummyAddr) String() string  { return "virtual-device" }

func mapNetError(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ErrDeviceClosed
	}
	return err
}
