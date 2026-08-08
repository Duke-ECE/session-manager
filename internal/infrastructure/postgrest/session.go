package postgrest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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
	Title      string `json:"title"`
	AgentID    string `json:"agent_id"`
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
		Title:      r.Title,
		AgentID:    r.AgentID,
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

func (c *Client) CreateSession(ctx context.Context, id, userID, llmModel, agentID string) (session.Session, error) {
	row := map[string]string{"id": id, "user_id": userID}
	if llmModel != "" {
		row["llm_model"] = llmModel
	}
	if agentID != "" {
		row["agent_id"] = agentID
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

func (c *Client) ListSessions(ctx context.Context, userID string, limit, offset int32) ([]session.Session, bool, error) {
	// Fetch one extra row to learn whether another page exists.
	q := url.Values{
		"user_id": {"eq." + userID},
		"order":   {"last_active.desc"},
		"limit":   {strconv.Itoa(int(limit) + 1)},
		"offset":  {strconv.Itoa(int(offset))},
	}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_sessions", q, nil, "", &rows); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	sessions := make([]session.Session, 0, len(rows))
	for _, r := range rows {
		sessions = append(sessions, r.toSession())
	}
	return sessions, hasMore, nil
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

func (c *Client) SetTitle(ctx context.Context, id, title string) (session.Session, error) {
	q := url.Values{"id": {"eq." + id}}
	// Only the title column: titling must not bump last_active.
	body := map[string]string{"title": title}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodPatch, "/rest/v1/agent_sessions", q, body, "return=representation", &rows); err != nil {
		return session.Session{}, err
	}
	if len(rows) == 0 {
		return session.Session{}, session.ErrNotFound
	}
	return rows[0].toSession(), nil
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

func (c *Client) GetMessages(ctx context.Context, sessionID string, beforeSeq, limit int32) ([]session.Message, bool, error) {
	q := url.Values{"session_id": {"eq." + sessionID}}
	if limit > 0 {
		// Latest window: fetch limit+1 rows below beforeSeq, newest first, so
		// the extra row tells us older messages exist before the window.
		if beforeSeq > 0 {
			q.Set("seq", "lt."+strconv.Itoa(int(beforeSeq)))
		}
		q.Set("order", "seq.desc")
		q.Set("limit", strconv.Itoa(int(limit)+1))
	} else {
		q.Set("order", "seq.asc")
	}
	var rows []messageRow
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_messages", q, nil, "", &rows); err != nil {
		return nil, false, err
	}
	hasMore := false
	if limit > 0 {
		hasMore = len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}
		// Back to ascending seq for the caller.
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}
	msgs := make([]session.Message, 0, len(rows))
	for _, r := range rows {
		msgs = append(msgs, r.toMessage())
	}
	return msgs, hasMore, nil
}

func (c *Client) DeleteSession(ctx context.Context, id string) error {
	// Messages first so no orphans are left if the session delete fails.
	if err := c.do(ctx, http.MethodDelete, "/rest/v1/agent_messages", url.Values{"session_id": {"eq." + id}}, nil, "", nil); err != nil {
		return err
	}
	q := url.Values{"id": {"eq." + id}}
	var rows []sessionRow
	if err := c.do(ctx, http.MethodDelete, "/rest/v1/agent_sessions", q, nil, "return=representation", &rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return session.ErrNotFound
	}
	return nil
}

func (c *Client) ListEndedBefore(ctx context.Context, cutoff time.Time) ([]session.Session, error) {
	q := url.Values{
		"status":   {"eq.ended"},
		"ended_at": {"lt." + cutoff.UTC().Format(time.RFC3339)},
	}
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
