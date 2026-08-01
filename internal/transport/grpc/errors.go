package grpc

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Duke-ECE/session-manager/internal/session"
)

// toStatus is the only error→status mapping in the service. Domain errors
// (session.Error) carry their caller-facing message; store ErrNotFound maps
// to NotFound; anything else is an internal store failure.
func toStatus(err error) error {
	if errors.Is(err, session.ErrNotFound) {
		return status.Error(codes.NotFound, "session not found")
	}
	var domErr *session.Error
	if errors.As(err, &domErr) {
		code := codes.Internal
		switch domErr.Kind {
		case session.KindInvalidArgument:
			code = codes.InvalidArgument
		case session.KindPermissionDenied:
			code = codes.PermissionDenied
		case session.KindUnauthenticated:
			code = codes.Unauthenticated
		case session.KindFailedPrecondition:
			code = codes.FailedPrecondition
		}
		return status.Error(code, domErr.Message)
	}
	return status.Errorf(codes.Internal, "store: %v", err)
}
