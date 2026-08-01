package grpc

import (
	"context"

	"google.golang.org/grpc/metadata"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/session"
)

// SessionHandler serves session.v1.SessionService on top of the session
// domain slice. Handlers are thin: decode the request, extract credentials
// from metadata, call the slice service, and map errors via toStatus.
type SessionHandler struct {
	v1.UnimplementedSessionServiceServer

	svc *session.Service
}

// NewSessionHandler wires the gRPC handlers to svc.
func NewSessionHandler(svc *session.Service) *SessionHandler {
	return &SessionHandler{svc: svc}
}

func (h *SessionHandler) CreateSession(ctx context.Context, req *v1.CreateSessionRequest) (*v1.CreateSessionResponse, error) {
	sess, err := h.svc.CreateSession(ctx, req.GetUserId(), req.GetLlmModel())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v1.CreateSessionResponse{Session: toProtoSession(sess)}, nil
}

func (h *SessionHandler) GetSession(ctx context.Context, req *v1.GetSessionRequest) (*v1.GetSessionResponse, error) {
	sess, err := h.svc.GetSession(ctx, req.GetSessionId(), req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v1.GetSessionResponse{Session: toProtoSession(sess)}, nil
}

func (h *SessionHandler) ListSessions(ctx context.Context, req *v1.ListSessionsRequest) (*v1.ListSessionsResponse, error) {
	sessions, err := h.svc.ListSessions(ctx, req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &v1.ListSessionsResponse{}
	for _, sess := range sessions {
		resp.Sessions = append(resp.Sessions, toProtoSession(sess))
	}
	return resp, nil
}

func (h *SessionHandler) EndSession(ctx context.Context, req *v1.EndSessionRequest) (*v1.EndSessionResponse, error) {
	if err := h.svc.EndSession(ctx, req.GetSessionId(), req.GetUserId()); err != nil {
		return nil, toStatus(err)
	}
	return &v1.EndSessionResponse{}, nil
}

func (h *SessionHandler) AppendTurn(ctx context.Context, req *v1.AppendTurnRequest) (*v1.AppendTurnResponse, error) {
	msgs := make([]session.Message, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		msgs = append(msgs, session.Message{Role: m.GetRole(), ContentJSON: m.GetContentJson()})
	}
	if err := h.svc.AppendTurn(ctx, req.GetSessionId(), msgs, presentedTokens(ctx)); err != nil {
		return nil, toStatus(err)
	}
	return &v1.AppendTurnResponse{}, nil
}

func (h *SessionHandler) GetTranscript(ctx context.Context, req *v1.GetTranscriptRequest) (*v1.GetTranscriptResponse, error) {
	msgs, err := h.svc.GetTranscript(ctx, req.GetSessionId(), req.GetUserId(), presentedTokens(ctx))
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

// presentedTokens extracts the x-service-token metadata values from ctx.
func presentedTokens(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	return md.Get("x-service-token")
}

func toProtoSession(sess session.Session) *v1.Session {
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
