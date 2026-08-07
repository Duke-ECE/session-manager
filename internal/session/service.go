package session

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Service holds the session business rules on top of the Store port:
// ownership enforcement, server-generated ids, the end lifecycle, and the
// service-token policy for runtime-internal operations. serviceToken
// authenticates runtime-internal callers; an empty token fails those paths
// closed.
type Service struct {
	store        Store
	serviceToken string
}

// NewService wires the business rules to st.
func NewService(st Store, serviceToken string) *Service {
	return &Service{store: st, serviceToken: serviceToken}
}

// CreateSession validates the request, generates the id, and inserts the
// session row.
func (s *Service) CreateSession(ctx context.Context, userID, llmModel string) (Session, error) {
	if userID == "" {
		return Session{}, invalidArgument("user_id is required")
	}
	id, err := newSessionID()
	if err != nil {
		return Session{}, internal(fmt.Sprintf("generate session id: %v", err))
	}
	return s.store.CreateSession(ctx, id, userID, llmModel)
}

// GetSession returns the session only if userID owns it.
func (s *Service) GetSession(ctx context.Context, sessionID, userID string) (Session, error) {
	return s.ownedSession(ctx, sessionID, userID)
}

// ListSessions returns all of userID's sessions.
func (s *Service) ListSessions(ctx context.Context, userID string) ([]Session, error) {
	if userID == "" {
		return nil, invalidArgument("user_id is required")
	}
	return s.store.ListSessions(ctx, userID)
}

// EndSession marks the session ended; ending an already-ended session is a
// no-op.
func (s *Service) EndSession(ctx context.Context, sessionID, userID string) error {
	sess, err := s.ownedSession(ctx, sessionID, userID)
	if err != nil {
		return err
	}
	if sess.Status == "ended" {
		return nil
	}
	return s.store.EndSession(ctx, sess.ID, time.Now())
}

// AppendTurn appends one completed turn's messages to the transcript. It is
// runtime-internal: a valid service token is always required. Appending zero
// messages is a no-op. Ended sessions are terminal: appends fail with
// FailedPrecondition so a transcript stays exactly as the user left it.
func (s *Service) AppendTurn(ctx context.Context, sessionID string, msgs []Message, tokens []string) error {
	if !s.tokenOK(tokens) {
		return unauthenticated("valid x-service-token metadata is required")
	}
	if sessionID == "" {
		return invalidArgument("session_id is required")
	}
	if len(msgs) == 0 {
		return nil
	}
	// Ensure the session exists so a typo'd id can't orphan messages, and
	// refuse to extend an ended session: ended is terminal, the transcript
	// stays exactly as the user left it.
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess.Status == "ended" {
		return failedPrecondition("session has ended")
	}
	_, err = s.store.AppendMessages(ctx, sessionID, msgs)
	return err
}

// maxTitleLen caps stored titles; longer input is truncated, not rejected.
const maxTitleLen = 120

// SetTitle sets the session's display title (e.g. an LLM-generated summary
// of the first turn). It is runtime-internal like AppendTurn: a valid
// service token is always required. A title is metadata, not transcript, so
// it stays settable on ended sessions and never bumps last_active.
func (s *Service) SetTitle(ctx context.Context, sessionID, title string, tokens []string) (Session, error) {
	if !s.tokenOK(tokens) {
		return Session{}, unauthenticated("valid x-service-token metadata is required")
	}
	if sessionID == "" {
		return Session{}, invalidArgument("session_id is required")
	}
	title = strings.TrimSpace(title)
	if title == "" {
		return Session{}, invalidArgument("title is required")
	}
	if r := []rune(title); len(r) > maxTitleLen {
		title = string(r[:maxTitleLen])
	}
	return s.store.SetTitle(ctx, sessionID, title)
}

// GetTranscript returns the full transcript. The owner reads freely. A
// non-empty non-owner userID is an authenticated cross-user access →
// PermissionDenied. With no user identity at all (e.g. runtime hydration),
// the service token decides.
func (s *Service) GetTranscript(ctx context.Context, sessionID, userID string, tokens []string) ([]Message, error) {
	if sessionID == "" {
		return nil, invalidArgument("session_id is required")
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if userID != "" {
		if userID != sess.UserID {
			return nil, permissionDenied("session belongs to another user")
		}
	} else if !s.tokenOK(tokens) {
		return nil, unauthenticated("owner user_id or valid x-service-token metadata is required")
	}
	return s.store.GetMessages(ctx, sess.ID)
}

// ownedSession validates the user-scoped preconditions and returns the
// session only if userID owns it.
func (s *Service) ownedSession(ctx context.Context, sessionID, userID string) (Session, error) {
	if sessionID == "" {
		return Session{}, invalidArgument("session_id is required")
	}
	if userID == "" {
		return Session{}, invalidArgument("user_id is required")
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if sess.UserID != userID {
		return Session{}, permissionDenied("user_id does not own this session")
	}
	return sess, nil
}

// tokenOK reports whether any presented token matches the shared service
// token. An empty configured token fails closed.
func (s *Service) tokenOK(presented []string) bool {
	if s.serviceToken == "" {
		return false
	}
	for _, v := range presented {
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
