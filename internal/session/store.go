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
	// agentID is the opaque agent-template id; empty means none.
	CreateSession(ctx context.Context, id, userID, llmModel, agentID string) (Session, error)
	// GetSession returns one session or ErrNotFound.
	GetSession(ctx context.Context, id string) (Session, error)
	// ListSessions returns one page of a user's sessions, most recently
	// active first: up to limit rows starting at offset, and hasMore true
	// when another page exists after this one.
	ListSessions(ctx context.Context, userID string, limit, offset int32) (sessions []Session, hasMore bool, err error)
	// EndSession marks the session ended; ErrNotFound if it does not exist.
	EndSession(ctx context.Context, id string, endedAt time.Time) error
	// DeleteSession deletes the session row and all of its messages;
	// ErrNotFound if the session does not exist.
	DeleteSession(ctx context.Context, id string) error
	// ListEndedBefore returns ended sessions whose ended_at is older than
	// cutoff (retention janitor).
	ListEndedBefore(ctx context.Context, cutoff time.Time) ([]Session, error)
	// SetTitle sets the session's display title and returns the updated row;
	// ErrNotFound if the session does not exist. It does not touch
	// last_active: titling is metadata, not activity.
	SetTitle(ctx context.Context, id, title string) (Session, error)
	// AppendMessages assigns seq = max(seq)+1... per session, inserts the
	// messages, bumps the session's last_active, and returns the inserted
	// rows with seq and created_at filled in.
	AppendMessages(ctx context.Context, sessionID string, msgs []Message) ([]Message, error)
	// GetMessages returns transcript messages ordered by seq ascending. With
	// limit = 0 it returns the full transcript (hasMore always false). With
	// limit > 0 it returns the latest window — the up to limit messages with
	// seq < beforeSeq (beforeSeq = 0 means "from the end") — still ascending,
	// and hasMore true when older messages exist before the window.
	GetMessages(ctx context.Context, sessionID string, beforeSeq, limit int32) (messages []Message, hasMore bool, err error)
}
