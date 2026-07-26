// Package store persists session records and transcripts in Supabase
// Postgres via the PostgREST REST API, using the service role key (the
// tables have RLS enabled with no policies — service-role-only access).
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when the requested row does not exist.
var ErrNotFound = errors.New("store: not found")

// Session mirrors a row in agent_sessions. Timestamps are RFC 3339 strings
// as returned by PostgREST; EndedAt is empty unless the session has ended.
type Session struct {
	ID         string
	UserID     string
	Status     string
	LLMModel   string
	CreatedAt  string
	LastActive string
	EndedAt    string
}

// Message mirrors a row in agent_messages. ContentJSON is the raw JSON
// payload stored in the content jsonb column.
type Message struct {
	Seq         int32
	Role        string
	ContentJSON string
	CreatedAt   string
}

// Store is the persistence seam. The PostgREST implementation is the only
// production one; tests substitute fakes.
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
