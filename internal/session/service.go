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
// session row. agentID is the opaque agent-template id; empty means none.
func (s *Service) CreateSession(ctx context.Context, userID, llmModel, agentID string) (Session, error) {
	if userID == "" {
		return Session{}, invalidArgument("user_id is required")
	}
	id, err := newSessionID()
	if err != nil {
		return Session{}, internal(fmt.Sprintf("generate session id: %v", err))
	}
	return s.store.CreateSession(ctx, id, userID, llmModel, agentID)
}

// GetSession returns the session only if userID owns it.
func (s *Service) GetSession(ctx context.Context, sessionID, userID string) (Session, error) {
	return s.ownedSession(ctx, sessionID, userID)
}

// ListSessions pagination bounds: limit 0 means the default page size;
// anything above the cap is clamped to it.
const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// ListSessions returns one page of userID's sessions, most recently active
// first, and whether another page exists after it.
func (s *Service) ListSessions(ctx context.Context, userID string, limit, offset int32) ([]Session, bool, error) {
	if userID == "" {
		return nil, false, invalidArgument("user_id is required")
	}
	if limit <= 0 {
		limit = defaultListLimit
	} else if limit > maxListLimit {
		limit = maxListLimit
	}
	if offset < 0 {
		offset = 0
	}
	return s.store.ListSessions(ctx, userID, limit, offset)
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

// DeleteSession removes the session and its whole transcript. Ownership is
// enforced like EndSession; deletion is allowed in any status — active or
// ended.
func (s *Service) DeleteSession(ctx context.Context, sessionID, userID string) error {
	sess, err := s.ownedSession(ctx, sessionID, userID)
	if err != nil {
		return err
	}
	return s.store.DeleteSession(ctx, sess.ID)
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
// of the first turn, or a user rename). It follows GetTranscript's
// owner-or-token pattern: the owner passes with just userID, a non-empty
// non-owner userID is PermissionDenied, and with no user identity the
// service token decides. A title is metadata, not transcript, so it stays
// settable on ended sessions and never bumps last_active.
func (s *Service) SetTitle(ctx context.Context, sessionID, title, userID string, tokens []string) (Session, error) {
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
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if userID != "" {
		if userID != sess.UserID {
			return Session{}, permissionDenied("session belongs to another user")
		}
	} else if !s.tokenOK(tokens) {
		return Session{}, unauthenticated("owner user_id or valid x-service-token metadata is required")
	}
	return s.store.SetTitle(ctx, sessionID, title)
}

// maxTranscriptLimit caps windowed GetTranscript pages; limit 0 means the
// full transcript (legacy behavior).
const maxTranscriptLimit = 1000

// GetTranscript returns transcript messages in ascending seq order. The
// owner reads freely. A non-empty non-owner userID is an authenticated
// cross-user access → PermissionDenied. With no user identity at all (e.g.
// runtime hydration), the service token decides. With limit = 0 the full
// transcript is returned (hasMore always false); with limit > 0 the latest
// window — the up to limit messages with seq < beforeSeq (beforeSeq = 0
// means "from the end") — and hasMore true when older messages exist before
// the window.
func (s *Service) GetTranscript(ctx context.Context, sessionID, userID string, tokens []string, beforeSeq, limit int32) ([]Message, bool, error) {
	if sessionID == "" {
		return nil, false, invalidArgument("session_id is required")
	}
	if limit < 0 {
		limit = 0
	} else if limit > maxTranscriptLimit {
		limit = maxTranscriptLimit
	}
	if beforeSeq < 0 {
		beforeSeq = 0
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return nil, false, err
	}
	if userID != "" {
		if userID != sess.UserID {
			return nil, false, permissionDenied("session belongs to another user")
		}
	} else if !s.tokenOK(tokens) {
		return nil, false, unauthenticated("owner user_id or valid x-service-token metadata is required")
	}
	return s.store.GetMessages(ctx, sess.ID, beforeSeq, limit)
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
