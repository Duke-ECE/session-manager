package session

import (
	"context"
	"time"
)

// Store is the persistence port. The PostgREST implementation
// (internal/infrastructure/postgrest) is the only production one; tests
// substitute fakes.
type Store interface {
	// CreateSession inserts a row with the given server-generated id and
	// returns it with database defaults (status, timestamps) filled in.
	CreateSession(ctx context.Context, id, userID, llmModel string) (Session, error)
	// GetSession returns one session or ErrNotFound.
	GetSession(ctx context.Context, id string) (Session, error)
	// ListSessions returns a user's sessions, most recently active first.
	ListSessions(ctx context.Context, userID string) ([]Session, error)
	// EndSession marks the session ended; ErrNotFound if it does not exist.
	EndSession(ctx context.Context, id string, endedAt time.Time) error
	// AppendMessages assigns seq = max(seq)+1... per session, inserts the
	// messages, bumps the session's last_active, and returns the inserted
	// rows with seq and created_at filled in.
	AppendMessages(ctx context.Context, sessionID string, msgs []Message) ([]Message, error)
	// GetMessages returns the full transcript ordered by seq.
	GetMessages(ctx context.Context, sessionID string) ([]Message, error)
}
