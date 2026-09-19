// Package grpc exposes the session domain slice over gRPC: it adapts
// session.v1.SessionService and session.v2.SessionService to the v1 and durable
// services and owns the only error→status mapping (errors.go). Both contract
// versions stay registered while the platform migrates.
package grpc

import (
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/session"
)

// NewServer builds a grpc.Server with both SessionService versions registered
// and reflection enabled (for grpcurl). durable may be nil during a staged
// rollout, in which case only v1 is served.
func NewServer(svc *session.Service, durable *session.DurableService) *googlegrpc.Server {
	s := googlegrpc.NewServer()
	v1.RegisterSessionServiceServer(s, NewSessionHandler(svc))
	if durable != nil {
		v2.RegisterSessionServiceServer(s, NewDurableHandler(durable))
	}
	reflection.Register(s)
	return s
}
