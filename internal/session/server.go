// Package session implements the session.v1.SessionService gRPC handlers.
// The service is the platform's privilege enforcement point for session
// data: user-scoped RPCs require the caller's user_id and enforce row
// ownership, while runtime-internal RPCs (AppendTurn, and GetTranscript for
// a non-owner) require the shared service token.
package session

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/store"
)

// Server serves session.v1.SessionService on top of a store.Store.
type Server struct {
	v1.UnimplementedSessionServiceServer

	store        store.Store
	serviceToken string
}

// NewServer wires handlers to st. serviceToken authenticates runtime-internal
// RPCs via the x-service-token metadata header; an empty token fails those
// RPCs closed.
func NewServer(st store.Store, serviceToken string) *Server {
	return &Server{store: st, serviceToken: serviceToken}
}

func (s *Server) CreateSession(ctx context.Context, req *v1.CreateSessionRequest) (*v1.CreateSessionResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	id, err := newSessionID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate session id: %v", err)
	}
	sess, err := s.store.CreateSession(ctx, id, req.GetUserId(), req.GetLlmModel())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v1.CreateSessionResponse{Session: toProtoSession(sess)}, nil
}

func (s *Server) GetSession(ctx context.Context, req *v1.GetSessionRequest) (*v1.GetSessionResponse, error) {
	sess, err := s.ownedSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &v1.GetSessionResponse{Session: toProtoSession(sess)}, nil
}

func (s *Server) ListSessions(ctx context.Context, req *v1.ListSessionsRequest) (*v1.ListSessionsResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	sessions, err := s.store.ListSessions(ctx, req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &v1.ListSessionsResponse{}
	for _, sess := range sessions {
		resp.Sessions = append(resp.Sessions, toProtoSession(sess))
	}
	return resp, nil
}

func (s *Server) EndSession(ctx context.Context, req *v1.EndSessionRequest) (*v1.EndSessionResponse, error) {
	sess, err := s.ownedSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	if sess.Status == "ended" {
		return &v1.EndSessionResponse{}, nil
	}
	if err := s.store.EndSession(ctx, sess.ID, time.Now()); err != nil {
		return nil, toStatus(err)
	}
	return &v1.EndSessionResponse{}, nil
}

func (s *Server) AppendTurn(ctx context.Context, req *v1.AppendTurnRequest) (*v1.AppendTurnResponse, error) {
	if !s.tokenOK(ctx) {
		return nil, status.Error(codes.Unauthenticated, "valid x-service-token metadata is required")
	}
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if len(req.GetMessages()) == 0 {
		return &v1.AppendTurnResponse{}, nil
	}
	// Ensure the session exists so a typo'd id can't orphan messages.
	if _, err := s.store.GetSession(ctx, req.GetSessionId()); err != nil {
		return nil, toStatus(err)
	}
	msgs := make([]store.Message, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		msgs = append(msgs, store.Message{Role: m.GetRole(), ContentJSON: m.GetContentJson()})
	}
	if _, err := s.store.AppendMessages(ctx, req.GetSessionId(), msgs); err != nil {
		return nil, toStatus(err)
	}
	return &v1.AppendTurnResponse{}, nil
}

func (s *Server) GetTranscript(ctx context.Context, req *v1.GetTranscriptRequest) (*v1.GetTranscriptResponse, error) {
	if req.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := s.store.GetSession(ctx, req.GetSessionId())
	if err != nil {
		return nil, toStatus(err)
	}
	// The owner reads freely; anyone else (e.g. runtime hydration) needs the
	// service token.
	if req.GetUserId() != sess.UserID && !s.tokenOK(ctx) {
		return nil, status.Error(codes.Unauthenticated, "owner user_id or valid x-service-token metadata is required")
	}
	msgs, err := s.store.GetMessages(ctx, sess.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &v1.GetTranscriptResponse{}
	for _, m := range msgs {
		resp.Messages = append(resp.Messages, &v1.TurnMessage{
			Seq:         m.Seq,
			Role:        m.Role,
			ContentJson: m.ContentJSON,
			CreatedAt:   m.CreatedAt,
		})
	}
	return resp, nil
}

// ownedSession validates the user-scoped RPC preconditions and returns the
// session only if userID owns it.
func (s *Server) ownedSession(ctx context.Context, sessionID, userID string) (store.Session, error) {
	if sessionID == "" {
		return store.Session{}, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if userID == "" {
		return store.Session{}, status.Error(codes.InvalidArgument, "user_id is required")
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return store.Session{}, toStatus(err)
	}
	if sess.UserID != userID {
		return store.Session{}, status.Error(codes.PermissionDenied, "user_id does not own this session")
	}
	return sess, nil
}

// tokenOK reports whether the call carries the shared service token in
// x-service-token metadata.
func (s *Server) tokenOK(ctx context.Context) bool {
	if s.serviceToken == "" {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, v := range md.Get("x-service-token") {
		if subtle.ConstantTimeCompare([]byte(v), []byte(s.serviceToken)) == 1 {
			return true
		}
	}
	return false
}

// newSessionID returns "sess-<16 hex chars>" from crypto/rand.
func newSessionID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "sess-" + hex.EncodeToString(b[:]), nil
}

// toStatus maps store errors to gRPC status codes.
func toStatus(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return status.Error(codes.NotFound, "session not found")
	}
	return status.Errorf(codes.Internal, "store: %v", err)
}

func toProtoSession(sess store.Session) *v1.Session {
	return &v1.Session{
		Id:         sess.ID,
		UserId:     sess.UserID,
		Status:     sess.Status,
		LlmModel:   sess.LLMModel,
		CreatedAt:  sess.CreatedAt,
		LastActive: sess.LastActive,
		EndedAt:    sess.EndedAt,
	}
}
