// Package diaggate implements a TCP-facing diagnostic gateway connected to
// 2..4 in-memory virtual devices.  The external TCP protocol and the gateway
// ↔ device eight-byte frame protocol are separated; see protocol.go and tcp.go.
package diaggate
