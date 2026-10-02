package diaggate

import (
	"errors"
	"fmt"
)

// ErrorCode identifies the class of a session-scoped failure.  Every failure
// terminates only the owning device/request session.
type ErrorCode string

const (
	CodeMalformedFrame  ErrorCode = "malformed_frame"
	CodeLengthMismatch  ErrorCode = "length_mismatch"
	CodeOutOfOrder      ErrorCode = "out_of_order"
	CodeQuotaExhausted  ErrorCode = "quota_exhausted"
	CodeTimeout         ErrorCode = "timeout"
	CodeCanceled        ErrorCode = "canceled"
	CodeDeviceClosed    ErrorCode = "device_closed"
	CodeUnavailable     ErrorCode = "device_unavailable"
	CodePayloadTooLarge ErrorCode = "payload_too_large"
	CodeDeviceError     ErrorCode = "device_error"
)

var (
	ErrPayloadTooLarge = errors.New("payload exceeds 512 bytes")
	ErrDeviceClosed    = errors.New("device connection closed")
	ErrCanceled        = errors.New("session canceled")
	ErrTimeout         = errors.New("session timed out")
	ErrMalformedFrame  = errors.New("malformed frame")
	ErrQuotaExhausted  = errors.New("device send quota exhausted")
)

// ProtocolError is the structured error exchanged over the wire and returned
// to callers.
type ProtocolError struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	DeviceID  byte      `json:"device_id"`
	RequestID uint16    `json:"request_id"`
	Op        string    `json:"op,omitempty"`
}

func (e *ProtocolError) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("%s: device=%d request=%d: %s: %s", e.Op, e.DeviceID, e.RequestID, e.Code, e.Message)
	}
	return fmt.Sprintf("device=%d request=%d: %s: %s", e.DeviceID, e.RequestID, e.Code, e.Message)
}

func frameError(deviceID byte, requestID uint16, code ErrorCode, format string, args ...any) *ProtocolError {
	return &ProtocolError{
		Code:      code,
		Message:   fmt.Sprintf(format, args...),
		DeviceID:  deviceID,
		RequestID: requestID,
	}
}
