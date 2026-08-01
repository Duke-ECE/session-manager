package postgrest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Duke-ECE/session-manager/internal/session"
)

var _ session.Store = (*Client)(nil)

// sessionRow maps the agent_sessions table. JSON null decodes into Go
// strings as a no-op, so nullable columns stay empty strings.
type sessionRow struct {
	ID         string `json:"id"`
	UserID     string `json:"user_id"`
	Status     string `json:"status"`
	LLMModel   string `json:"llm_model"`
	CreatedAt  string `json:"created_at"`
	LastActive string `json:"last_active"`
	EndedAt    string `json:"ended_at"`
}

func (r sessionRow) toSession() session.Session {
	return session.Session{
		ID:         r.ID,
		UserID:     r.UserID,
		Status:     r.Status,
		LLMModel:   r.LLMModel,
		CreatedAt:  r.CreatedAt,
		LastActive: r.LastActive,
		EndedAt:    r.EndedAt,
	}
}

// messageRow maps the agent_messages table.
type messageRow struct {
	Seq       int32           `json:"seq"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	CreatedAt string          `json:"created_at"`
}

func (r messageRow) toMessage() session.Message {
	return session.Message{
		Seq:         r.Seq,
		Role:        r.Role,
		ContentJSON: string(r.Content),
		CreatedAt:   r.CreatedAt,
	}
}

func (c *Client) CreateSession(ctx context.Context, id, userID, llmModel string) (session.Session, error) {
	row := map[string]string{"id": id, "user_id": userID}
	if llmModel != "" {
		row["llm_model"] = llmModel
	}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodPost, "/rest/v1/agent_sessions", nil, row, "return=representation", &rows); err != nil {
		return session.Session{}, err
	}
	if len(rows) != 1 {
		return session.Session{}, fmt.Errorf("insert agent_sessions: got %d rows back", len(rows))
	}
	return rows[0].toSession(), nil
}

func (c *Client) GetSession(ctx context.Context, id string) (session.Session, error) {
	q := url.Values{"id": {"eq." + id}, "limit": {"1"}}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_sessions", q, nil, "", &rows); err != nil {
		return session.Session{}, err
	}
	if len(rows) == 0 {
		return session.Session{}, session.ErrNotFound
	}
	return rows[0].toSession(), nil
}

func (c *Client) ListSessions(ctx context.Context, userID string) ([]session.Session, error) {
	q := url.Values{"user_id": {"eq." + userID}, "order": {"last_active.desc"}}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_sessions", q, nil, "", &rows); err != nil {
		return nil, err
	}
	sessions := make([]session.Session, 0, len(rows))
	for _, r := range rows {
		sessions = append(sessions, r.toSession())
	}
	return sessions, nil
}

func (c *Client) EndSession(ctx context.Context, id string, endedAt time.Time) error {
	q := url.Values{"id": {"eq." + id}}
	body := map[string]string{
		"status":   "ended",
		"ended_at": endedAt.UTC().Format(time.RFC3339),
	}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodPatch, "/rest/v1/agent_sessions", q, body, "return=representation", &rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return session.ErrNotFound
	}
	return nil
}

func (c *Client) AppendMessages(ctx context.Context, sessionID string, msgs []session.Message) ([]session.Message, error) {
	// Server-assigned seq: continue after the current max for this session.
	q := url.Values{
		"session_id": {"eq." + sessionID},
		"select":     {"seq"},
		"order":      {"seq.desc"},
		"limit":      {"1"},
	}
	var maxRows []messageRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_messages", q, nil, "", &maxRows); err != nil {
		return nil, err
	}
	next := int32(1)
	if len(maxRows) > 0 {
		next = maxRows[0].Seq + 1
	}

	rows := make([]map[string]any, 0, len(msgs))
	for i, m := range msgs {
		rows = append(rows, map[string]any{
			"session_id": sessionID,
			"seq":        next + int32(i),
			"role":       m.Role,
			"content":    json.RawMessage(m.ContentJSON),
		})
	}
	var inserted []messageRow
	if err := c.do(ctx, http.MethodPost, "/rest/v1/agent_messages", nil, rows, "return=representation", &inserted); err != nil {
		return nil, err
	}

	// Keep last_active fresh so ListSessions ordering reflects real activity.
	patch := map[string]string{"last_active": time.Now().UTC().Format(time.RFC3339)}
	if err := c.do(ctx, http.MethodPatch, "/rest/v1/agent_sessions", url.Values{"id": {"eq." + sessionID}}, patch, "", nil); err != nil {
		return nil, err
	}

	out := make([]session.Message, 0, len(inserted))
	for _, r := range inserted {
		out = append(out, r.toMessage())
	}
	return out, nil
}

func (c *Client) GetMessages(ctx context.Context, sessionID string) ([]session.Message, error) {
	q := url.Values{"session_id": {"eq." + sessionID}, "order": {"seq.asc"}}
	var rows []messageRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_messages", q, nil, "", &rows); err != nil {
		return nil, err
	}
	msgs := make([]session.Message, 0, len(rows))
	for _, r := range rows {
		msgs = append(msgs, r.toMessage())
	}
	return msgs, nil
}
