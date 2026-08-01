// Package grpc exposes the session domain slice over gRPC: it adapts
// session.v1.SessionService to session.Service and owns the only
// error→status mapping (errors.go).
package grpc

import (
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/session"
)

// NewServer builds a grpc.Server with the SessionService registered and
// reflection enabled (for grpcurl).
func NewServer(svc *session.Service) *googlegrpc.Server {
	s := googlegrpc.NewServer()
	v1.RegisterSessionServiceServer(s, NewSessionHandler(svc))
	reflection.Register(s)
	return s
}
