package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// postgrest reads and writes public.agent_sessions / public.agent_messages
// through the Supabase PostgREST REST API. Access requires the service role
// key; the tables have no anon/authenticated policies.
type postgrest struct {
	url        string
	serviceKey string
	client     *http.Client
}

// NewPostgREST returns a Store backed by Supabase PostgREST. A nil
// httpClient uses http.DefaultClient.
func NewPostgREST(url, serviceKey string, httpClient *http.Client) Store {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &postgrest{url: url, serviceKey: serviceKey, client: httpClient}
}

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

func (r sessionRow) toSession() Session {
	return Session{
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

func (r messageRow) toMessage() Message {
	return Message{
		Seq:         r.Seq,
		Role:        r.Role,
		ContentJSON: string(r.Content),
		CreatedAt:   r.CreatedAt,
	}
}

func (p *postgrest) CreateSession(ctx context.Context, id, userID, llmModel string) (Session, error) {
	row := map[string]string{"id": id, "user_id": userID}
	if llmModel != "" {
		row["llm_model"] = llmModel
	}
	var rows []sessionRow
	if err := p.do(ctx, http.MethodPost, "/rest/v1/agent_sessions", nil, row, "return=representation", &rows); err != nil {
		return Session{}, err
	}
	if len(rows) != 1 {
		return Session{}, fmt.Errorf("insert agent_sessions: got %d rows back", len(rows))
	}
	return rows[0].toSession(), nil
}

func (p *postgrest) GetSession(ctx context.Context, id string) (Session, error) {
	q := url.Values{"id": {"eq." + id}, "limit": {"1"}}
	var rows []sessionRow
	if err := p.do(ctx, http.MethodGet, "/rest/v1/agent_sessions", q, nil, "", &rows); err != nil {
		return Session{}, err
	}
	if len(rows) == 0 {
		return Session{}, ErrNotFound
	}
	return rows[0].toSession(), nil
}

func (p *postgrest) ListSessions(ctx context.Context, userID string) ([]Session, error) {
	q := url.Values{"user_id": {"eq." + userID}, "order": {"last_active.desc"}}
	var rows []sessionRow
	if err := p.do(ctx, http.MethodGet, "/rest/v1/agent_sessions", q, nil, "", &rows); err != nil {
		return nil, err
	}
	sessions := make([]Session, 0, len(rows))
	for _, r := range rows {
		sessions = append(sessions, r.toSession())
	}
	return sessions, nil
}

func (p *postgrest) EndSession(ctx context.Context, id string, endedAt time.Time) error {
	q := url.Values{"id": {"eq." + id}}
	body := map[string]string{
		"status":   "ended",
		"ended_at": endedAt.UTC().Format(time.RFC3339),
	}
	var rows []sessionRow
	if err := p.do(ctx, http.MethodPatch, "/rest/v1/agent_sessions", q, body, "return=representation", &rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *postgrest) AppendMessages(ctx context.Context, sessionID string, msgs []Message) ([]Message, error) {
	// Server-assigned seq: continue after the current max for this session.
	q := url.Values{
		"session_id": {"eq." + sessionID},
		"select":     {"seq"},
		"order":      {"seq.desc"},
		"limit":      {"1"},
	}
	var maxRows []messageRow
	if err := p.do(ctx, http.MethodGet, "/rest/v1/agent_messages", q, nil, "", &maxRows); err != nil {
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
	if err := p.do(ctx, http.MethodPost, "/rest/v1/agent_messages", nil, rows, "return=representation", &inserted); err != nil {
		return nil, err
	}

	// Keep last_active fresh so ListSessions ordering reflects real activity.
	patch := map[string]string{"last_active": time.Now().UTC().Format(time.RFC3339)}
	if err := p.do(ctx, http.MethodPatch, "/rest/v1/agent_sessions", url.Values{"id": {"eq." + sessionID}}, patch, "", nil); err != nil {
		return nil, err
	}

	out := make([]Message, 0, len(inserted))
	for _, r := range inserted {
		out = append(out, r.toMessage())
	}
	return out, nil
}

func (p *postgrest) GetMessages(ctx context.Context, sessionID string) ([]Message, error) {
	q := url.Values{"session_id": {"eq." + sessionID}, "order": {"seq.asc"}}
	var rows []messageRow
	if err := p.do(ctx, http.MethodGet, "/rest/v1/agent_messages", q, nil, "", &rows); err != nil {
		return nil, err
	}
	msgs := make([]Message, 0, len(rows))
	for _, r := range rows {
		msgs = append(msgs, r.toMessage())
	}
	return msgs, nil
}

// do performs one PostgREST request and decodes the JSON body into out
// (skipped when out is nil). prefer sets the PostgREST Prefer header.
func (p *postgrest) do(ctx context.Context, method, path string, query url.Values, body any, prefer string, out any) error {
	u := p.url + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("apikey", p.serviceKey)
	req.Header.Set("Authorization", "Bearer "+p.serviceKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: unexpected status %d", method, path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}
