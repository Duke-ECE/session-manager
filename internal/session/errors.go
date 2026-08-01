package session

import "errors"

// ErrNotFound is returned by the Store when the requested row does not exist.
var ErrNotFound = errors.New("store: not found")

// Kind classifies a domain Error so the transport layer can pick a status
// code without re-parsing messages. The zero value is invalid.
type Kind int

const (
	// KindInternal is an unexpected failure inside the service.
	KindInternal Kind = iota + 1
	// KindInvalidArgument is a malformed or missing request field.
	KindInvalidArgument
	// KindPermissionDenied is an authenticated caller without access.
	KindPermissionDenied
	// KindUnauthenticated is a missing or wrong credential.
	KindUnauthenticated
	// KindFailedPrecondition is a state conflict, e.g. writing to an
	// already-ended session.
	KindFailedPrecondition
)

// Error is a domain failure with a caller-facing message. The transport
// layer maps Kind to a gRPC status code and returns Message verbatim.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Message }

func internal(msg string) error        { return &Error{Kind: KindInternal, Message: msg} }
func invalidArgument(msg string) error { return &Error{Kind: KindInvalidArgument, Message: msg} }
func permissionDenied(msg string) error {
	return &Error{Kind: KindPermissionDenied, Message: msg}
}
func unauthenticated(msg string) error {
	return &Error{Kind: KindUnauthenticated, Message: msg}
}
func failedPrecondition(msg string) error {
	return &Error{Kind: KindFailedPrecondition, Message: msg}
}
