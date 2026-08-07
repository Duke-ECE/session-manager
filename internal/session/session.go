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
}

// Message mirrors a row in agent_messages. ContentJSON is the raw JSON
// payload stored in the content jsonb column.
type Message struct {
	Seq         int32
	Role        string
	ContentJSON string
	CreatedAt   string
}
