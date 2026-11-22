// Package session is the session domain slice: the Session/Message types,
// the Store port, and the business rules (ownership, id generation, end
// lifecycle, service-token policy) on top of it. It is the platform's
// privilege enforcement point for session data: user-scoped operations
// require the caller's user id and enforce row ownership, while
// runtime-internal operations (AppendTurn, and GetTranscript without a user
// identity) require the shared service token. It imports nothing from the
// transport or infrastructure layers.
package session

// Session mirrors a row in agent_sessions. Timestamps are RFC 3339 strings
// as returned by PostgREST; EndedAt is empty unless the session has ended.
//
// LastSeq through LeaseExpiresAt are the durable-context (v2) columns; the v1
// transport leaves them unset but the same row carries them.
type Session struct {
	ID         string
	UserID     string
	Status     string
	LLMModel   string
	Title      string
	AgentID    string
	CreatedAt  string
	LastActive string
	EndedAt    string

	LastSeq                int64
	Revision               int64
	ActiveCheckpointID     string
	ActiveRequestMessageID string
	LeaseOwner             string
	LeaseGeneration        int64
	LeaseExpiresAt         string
}

// Message mirrors a row in agent_messages. ContentJSON is the raw JSON
// payload stored in the content jsonb column.
//
// ID through CancellationRequested are the canonical-message (v2) columns.
// FormatVersion 0 marks a legacy v1 transcript row.
type Message struct {
	Seq         int32
	Role        string
	ContentJSON string
	CreatedAt   string

	ID                    string
	RequestMessageID      string
	FormatVersion         int32
	Status                string
	ReplyToMessageID      string
	UsageJSON             string
	ProviderMetadataJSON  string
	ClientRequestID       string
	RequestHash           string
	ExecutionStatus       string
	StartedAt             string
	FinishedAt            string
	ErrorCode             string
	CancellationRequested bool
}
