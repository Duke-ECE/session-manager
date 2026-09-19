package postgrest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Duke-ECE/session-manager/internal/session"
)

var _ session.DurableStore = (*Client)(nil)

// The durable paths call the Postgres transaction functions in
// supabase/migrations/20260919002000_durable_context_functions.sql through
// PostgREST /rpc. Each call is one database transaction, so the client never
// emulates a transition with a read-max-write sequence.

// sessionJSON is the session_json() projection.
type sessionJSON struct {
	ID                     string `json:"id"`
	UserID                 string `json:"user_id"`
	Status                 string `json:"status"`
	AgentID                string `json:"agent_id"`
	Title                  string `json:"title"`
	LastSeq                int64  `json:"last_seq"`
	Revision               int64  `json:"revision"`
	ActiveCheckpointID     string `json:"active_checkpoint_id"`
	ActiveRequestMessageID string `json:"active_request_message_id"`
	LeaseOwner             string `json:"lease_owner"`
	LeaseGeneration        int64  `json:"lease_generation"`
	LeaseExpiresAt         string `json:"lease_expires_at"`
	CreatedAt              string `json:"created_at"`
	LastActive             string `json:"last_active"`
	EndedAt                string `json:"ended_at"`
}

func (r sessionJSON) toSession() session.Session {
	return session.Session{
		ID:                     r.ID,
		UserID:                 r.UserID,
		Status:                 r.Status,
		Title:                  r.Title,
		AgentID:                r.AgentID,
		CreatedAt:              r.CreatedAt,
		LastActive:             r.LastActive,
		EndedAt:                r.EndedAt,
		LastSeq:                r.LastSeq,
		Revision:               r.Revision,
		ActiveCheckpointID:     r.ActiveCheckpointID,
		ActiveRequestMessageID: r.ActiveRequestMessageID,
		LeaseOwner:             r.LeaseOwner,
		LeaseGeneration:        r.LeaseGeneration,
		LeaseExpiresAt:         r.LeaseExpiresAt,
	}
}

// messageJSON is the session_message_json() projection.
type messageJSON struct {
	ID                    string          `json:"id"`
	SessionID             string          `json:"session_id"`
	Seq                   int64           `json:"seq"`
	RequestMessageID      string          `json:"request_message_id"`
	Role                  string          `json:"role"`
	Content               json.RawMessage `json:"content"`
	FormatVersion         int32           `json:"format_version"`
	MessageStatus         string          `json:"message_status"`
	ReplyToMessageID      string          `json:"reply_to_message_id"`
	Usage                 json.RawMessage `json:"usage"`
	ProviderMetadata      json.RawMessage `json:"provider_metadata"`
	ClientRequestID       string          `json:"client_request_id"`
	RequestHash           string          `json:"request_hash"`
	ExecutionStatus       string          `json:"execution_status"`
	StartedAt             string          `json:"started_at"`
	FinishedAt            string          `json:"finished_at"`
	ErrorCode             string          `json:"error_code"`
	CancellationRequested bool            `json:"cancellation_requested"`
	CreatedAt             string          `json:"created_at"`
}

func (r messageJSON) toMessage() session.Message {
	return session.Message{
		Seq:                   int32(r.Seq),
		Role:                  r.Role,
		ContentJSON:           rawOrEmpty(r.Content),
		CreatedAt:             r.CreatedAt,
		ID:                    r.ID,
		RequestMessageID:      r.RequestMessageID,
		FormatVersion:         r.FormatVersion,
		Status:                r.MessageStatus,
		ReplyToMessageID:      r.ReplyToMessageID,
		UsageJSON:             rawOrEmpty(r.Usage),
		ProviderMetadataJSON:  rawOrEmpty(r.ProviderMetadata),
		ClientRequestID:       r.ClientRequestID,
		RequestHash:           r.RequestHash,
		ExecutionStatus:       r.ExecutionStatus,
		StartedAt:             r.StartedAt,
		FinishedAt:            r.FinishedAt,
		ErrorCode:             r.ErrorCode,
		CancellationRequested: r.CancellationRequested,
	}
}

// configJSON is the session_config_json() projection.
type configJSON struct {
	SessionID           string          `json:"session_id"`
	FormatVersion       int32           `json:"format_version"`
	ConfigHash          string          `json:"config_hash"`
	SystemPrompt        string          `json:"system_prompt"`
	Provider            string          `json:"provider"`
	Model               string          `json:"model"`
	BaseURL             string          `json:"base_url"`
	CredentialRef       string          `json:"credential_ref"`
	ContextWindowTokens int64           `json:"context_window_tokens"`
	OutputLimitTokens   int64           `json:"output_limit_tokens"`
	Tools               json.RawMessage `json:"tools"`
	CreatedAt           string          `json:"created_at"`
}

func (r configJSON) toConfig() session.SessionConfig {
	cfg := session.SessionConfig{
		SessionID:           r.SessionID,
		FormatVersion:       r.FormatVersion,
		ConfigHash:          r.ConfigHash,
		SystemPrompt:        r.SystemPrompt,
		Provider:            r.Provider,
		Model:               r.Model,
		BaseURL:             r.BaseURL,
		CredentialRef:       r.CredentialRef,
		ContextWindowTokens: r.ContextWindowTokens,
		OutputLimitTokens:   r.OutputLimitTokens,
		CreatedAt:           r.CreatedAt,
	}
	if len(r.Tools) > 0 && string(r.Tools) != "null" {
		_ = json.Unmarshal(r.Tools, &cfg.Tools)
	}
	return cfg
}

// checkpointJSON is the session_checkpoint_json() projection.
type checkpointJSON struct {
	ID                     string          `json:"id"`
	SessionID              string          `json:"session_id"`
	ParentCheckpointID     string          `json:"parent_checkpoint_id"`
	CoveredThroughSeq      int64           `json:"covered_through_seq"`
	SourceHeadSeq          int64           `json:"source_head_seq"`
	SourceRevision         int64           `json:"source_revision"`
	ConfigHash             string          `json:"config_hash"`
	ActiveRequestMessageID string          `json:"active_request_message_id"`
	ResumeAfterSeq         int64           `json:"resume_after_seq"`
	Summary                json.RawMessage `json:"summary"`
	FormatVersion          int32           `json:"format_version"`
	PromptVersion          string          `json:"prompt_version"`
	SummarizerProvider     string          `json:"summarizer_provider"`
	SummarizerModel        string          `json:"summarizer_model"`
	EstimatedTokensBefore  int64           `json:"estimated_tokens_before"`
	EstimatedTokensAfter   int64           `json:"estimated_tokens_after"`
	GenerationUsage        json.RawMessage `json:"generation_usage"`
	CreatedAt              string          `json:"created_at"`
}

func (r checkpointJSON) toCheckpoint() session.Checkpoint {
	cp := session.Checkpoint{
		ID:                     r.ID,
		SessionID:              r.SessionID,
		ParentCheckpointID:     r.ParentCheckpointID,
		CoveredThroughSeq:      r.CoveredThroughSeq,
		SourceHeadSeq:          r.SourceHeadSeq,
		SourceRevision:         r.SourceRevision,
		ConfigHash:             r.ConfigHash,
		ActiveRequestMessageID: r.ActiveRequestMessageID,
		ResumeAfterSeq:         r.ResumeAfterSeq,
		FormatVersion:          r.FormatVersion,
		PromptVersion:          r.PromptVersion,
		SummarizerProvider:     r.SummarizerProvider,
		SummarizerModel:        r.SummarizerModel,
		EstimatedTokensBefore:  r.EstimatedTokensBefore,
		EstimatedTokensAfter:   r.EstimatedTokensAfter,
		GenerationUsageJSON:    rawOrEmpty(r.GenerationUsage),
		CreatedAt:              r.CreatedAt,
	}
	if len(r.Summary) > 0 && string(r.Summary) != "null" {
		_ = json.Unmarshal(r.Summary, &cp.Summary)
	}
	return cp
}

type leaseJSON struct {
	Owner      string `json:"owner"`
	Generation int64  `json:"generation"`
	ExpiresAt  string `json:"expires_at"`
}

func (r leaseJSON) toLease() session.Lease {
	return session.Lease{Owner: r.Owner, Generation: r.Generation, ExpiresAt: r.ExpiresAt}
}

// ------------------------------------------------------------ payload builders

func configPayload(c session.SessionConfig) map[string]any {
	tools, _ := json.Marshal(c.Tools)
	return map[string]any{
		"format_version":        c.FormatVersion,
		"config_hash":           c.ConfigHash,
		"system_prompt":         c.SystemPrompt,
		"provider":              c.Provider,
		"model":                 c.Model,
		"base_url":              c.BaseURL,
		"credential_ref":        c.CredentialRef,
		"context_window_tokens": c.ContextWindowTokens,
		"output_limit_tokens":   c.OutputLimitTokens,
		"tools":                 json.RawMessage(tools),
	}
}

func contentPayload(blocks []session.ContentBlock) json.RawMessage {
	if blocks == nil {
		blocks = []session.ContentBlock{}
	}
	b, _ := json.Marshal(blocks)
	return b
}

func draftPayload(d session.MessageDraft) map[string]any {
	m := map[string]any{
		"id":             d.ID,
		"role":           d.Role,
		"content":        contentPayload(d.Content),
		"format_version": d.FormatVersion,
		"message_status": d.Status,
	}
	if d.ReplyToMessageID != "" {
		m["reply_to_message_id"] = d.ReplyToMessageID
	}
	if d.UsageJSON != "" {
		m["usage"] = json.RawMessage(d.UsageJSON)
	}
	if d.ProviderMetadataJSON != "" {
		m["provider_metadata"] = json.RawMessage(d.ProviderMetadataJSON)
	}
	return m
}

func draftsPayload(drafts []session.MessageDraft) []map[string]any {
	out := make([]map[string]any, 0, len(drafts))
	for _, d := range drafts {
		out = append(out, draftPayload(d))
	}
	return out
}

func checkpointPayload(cp session.Checkpoint) map[string]any {
	m := map[string]any{
		"id":                        cp.ID,
		"parent_checkpoint_id":      cp.ParentCheckpointID,
		"covered_through_seq":       cp.CoveredThroughSeq,
		"source_head_seq":           cp.SourceHeadSeq,
		"source_revision":           cp.SourceRevision,
		"config_hash":               cp.ConfigHash,
		"active_request_message_id": cp.ActiveRequestMessageID,
		"resume_after_seq":          cp.ResumeAfterSeq,
		"summary":                   contentPayload(cp.Summary),
		"format_version":            cp.FormatVersion,
		"prompt_version":            cp.PromptVersion,
		"summarizer_provider":       cp.SummarizerProvider,
		"summarizer_model":          cp.SummarizerModel,
		"estimated_tokens_before":   cp.EstimatedTokensBefore,
		"estimated_tokens_after":    cp.EstimatedTokensAfter,
	}
	if cp.GenerationUsageJSON != "" {
		m["generation_usage"] = json.RawMessage(cp.GenerationUsageJSON)
	}
	return m
}

// ------------------------------------------------------------------- methods

func (c *Client) CreateDurableSession(ctx context.Context, id, userID, agentID string, cfg session.SessionConfig) (session.Session, session.SessionConfig, error) {
	var out struct {
		Session sessionJSON `json:"session"`
		Config  configJSON  `json:"config"`
	}
	body := map[string]any{
		"p_id":       id,
		"p_user_id":  userID,
		"p_agent_id": agentID,
		"p_config":   configPayload(cfg),
	}
	if err := c.rpc(ctx, "session_create_session", body, &out); err != nil {
		return session.Session{}, session.SessionConfig{}, err
	}
	return out.Session.toSession(), out.Config.toConfig(), nil
}

func (c *Client) GetSessionConfig(ctx context.Context, sessionID string) (session.SessionConfig, error) {
	q := url.Values{"session_id": {"eq." + sessionID}, "limit": {"1"}}
	var rows []configJSON
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_session_config", q, nil, "", &rows); err != nil {
		return session.SessionConfig{}, err
	}
	if len(rows) == 0 {
		return session.SessionConfig{}, session.ErrNotFound
	}
	return rows[0].toConfig(), nil
}

func (c *Client) BeginRequest(ctx context.Context, p session.BeginRequestParams) (session.BeginRequestResult, error) {
	var out struct {
		Session        sessionJSON `json:"session"`
		RequestMessage messageJSON `json:"request_message"`
		Deduplicated   bool        `json:"deduplicated"`
	}
	body := map[string]any{
		"p_session_id":         p.SessionID,
		"p_user_id":            p.UserID,
		"p_request_message_id": p.RequestMessageID,
		"p_client_request_id":  p.ClientRequestID,
		"p_request_hash":       p.RequestHash,
		"p_content":            contentPayload(p.Content),
		"p_format_version":     p.FormatVersion,
	}
	if err := c.rpc(ctx, "session_begin_request", body, &out); err != nil {
		return session.BeginRequestResult{}, err
	}
	return session.BeginRequestResult{
		Session:        out.Session.toSession(),
		RequestMessage: out.RequestMessage.toMessage(),
		Deduplicated:   out.Deduplicated,
	}, nil
}

func (c *Client) AcquireLease(ctx context.Context, p session.LeaseParams) (session.LeaseResult, error) {
	var out struct {
		Session sessionJSON `json:"session"`
		Lease   leaseJSON   `json:"lease"`
	}
	body := map[string]any{
		"p_session_id":         p.SessionID,
		"p_request_message_id": p.RequestMessageID,
		"p_owner":              p.Owner,
		"p_ttl_seconds":        p.TTLSeconds,
	}
	if err := c.rpc(ctx, "session_acquire_lease", body, &out); err != nil {
		return session.LeaseResult{}, err
	}
	return session.LeaseResult{Session: out.Session.toSession(), Lease: out.Lease.toLease()}, nil
}

func (c *Client) RenewLease(ctx context.Context, p session.RenewLeaseParams) (session.Lease, error) {
	var out struct {
		Lease leaseJSON `json:"lease"`
	}
	body := map[string]any{
		"p_session_id":  p.SessionID,
		"p_owner":       p.Owner,
		"p_generation":  p.Generation,
		"p_ttl_seconds": p.TTLSeconds,
	}
	if err := c.rpc(ctx, "session_renew_lease", body, &out); err != nil {
		return session.Lease{}, err
	}
	return out.Lease.toLease(), nil
}

func (c *Client) ReleaseLease(ctx context.Context, p session.ReleaseLeaseParams) (bool, error) {
	var out struct {
		Released bool `json:"released"`
	}
	body := map[string]any{
		"p_session_id": p.SessionID,
		"p_owner":      p.Owner,
		"p_generation": p.Generation,
	}
	if err := c.rpc(ctx, "session_release_lease", body, &out); err != nil {
		return false, err
	}
	return out.Released, nil
}

func (c *Client) AppendMessageBatch(ctx context.Context, p session.AppendParams) (session.AppendResult, error) {
	var out struct {
		Session      sessionJSON   `json:"session"`
		Messages     []messageJSON `json:"messages"`
		Deduplicated bool          `json:"deduplicated"`
	}
	body := map[string]any{
		"p_session_id":         p.SessionID,
		"p_request_message_id": p.RequestMessageID,
		"p_mutation_id":        p.Guard.MutationID,
		"p_mutation_hash":      p.Guard.MutationHash,
		"p_expected_revision":  p.Guard.ExpectedRevision,
		"p_lease_generation":   p.Guard.LeaseGeneration,
		"p_messages":           draftsPayload(p.Messages),
	}
	if err := c.rpc(ctx, "session_append_messages", body, &out); err != nil {
		return session.AppendResult{}, err
	}
	return session.AppendResult{
		Session:      out.Session.toSession(),
		Messages:     toMessages(out.Messages),
		Deduplicated: out.Deduplicated,
	}, nil
}

func (c *Client) FinishRequest(ctx context.Context, p session.FinishParams) (session.FinishResult, error) {
	var out struct {
		Session        sessionJSON   `json:"session"`
		RequestMessage messageJSON   `json:"request_message"`
		Messages       []messageJSON `json:"messages"`
		Deduplicated   bool          `json:"deduplicated"`
	}
	body := map[string]any{
		"p_session_id":         p.SessionID,
		"p_request_message_id": p.RequestMessageID,
		"p_mutation_id":        p.Guard.MutationID,
		"p_mutation_hash":      p.Guard.MutationHash,
		"p_expected_revision":  p.Guard.ExpectedRevision,
		"p_lease_generation":   p.Guard.LeaseGeneration,
		"p_status":             p.Status,
		"p_messages":           draftsPayload(p.Messages),
		"p_error_code":         p.ErrorCode,
	}
	if err := c.rpc(ctx, "session_finish_request", body, &out); err != nil {
		return session.FinishResult{}, err
	}
	return session.FinishResult{
		Session:        out.Session.toSession(),
		RequestMessage: out.RequestMessage.toMessage(),
		Messages:       toMessages(out.Messages),
		Deduplicated:   out.Deduplicated,
	}, nil
}

func (c *Client) CancelRequest(ctx context.Context, p session.CancelRequestParams) (session.CancelResult, error) {
	var out struct {
		RequestMessage messageJSON `json:"request_message"`
		Deduplicated   bool        `json:"deduplicated"`
	}
	body := map[string]any{
		"p_session_id":         p.SessionID,
		"p_user_id":            p.UserID,
		"p_request_message_id": p.RequestMessageID,
	}
	if err := c.rpc(ctx, "session_cancel_request", body, &out); err != nil {
		return session.CancelResult{}, err
	}
	return session.CancelResult{
		RequestMessage: out.RequestMessage.toMessage(),
		Deduplicated:   out.Deduplicated,
	}, nil
}

func (c *Client) PublishCheckpoint(ctx context.Context, p session.PublishParams) (session.PublishResult, error) {
	var out struct {
		Session      sessionJSON    `json:"session"`
		Checkpoint   checkpointJSON `json:"checkpoint"`
		Deduplicated bool           `json:"deduplicated"`
	}
	body := map[string]any{
		"p_session_id":        p.SessionID,
		"p_mutation_id":       p.Guard.MutationID,
		"p_mutation_hash":     p.Guard.MutationHash,
		"p_expected_revision": p.Guard.ExpectedRevision,
		"p_lease_generation":  p.Guard.LeaseGeneration,
		"p_checkpoint":        checkpointPayload(p.Checkpoint),
	}
	if err := c.rpc(ctx, "session_publish_checkpoint", body, &out); err != nil {
		return session.PublishResult{}, err
	}
	return session.PublishResult{
		Session:      out.Session.toSession(),
		Checkpoint:   out.Checkpoint.toCheckpoint(),
		Deduplicated: out.Deduplicated,
	}, nil
}

func (c *Client) GetMessageWindow(ctx context.Context, sessionID string, beforeSeq int64, limit int32) ([]session.Message, bool, error) {
	q := url.Values{"session_id": {"eq." + sessionID}}
	if limit > 0 {
		if beforeSeq > 0 {
			q.Set("seq", "lt."+strconv.FormatInt(beforeSeq, 10))
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

func (c *Client) GetActiveCheckpoint(ctx context.Context, sessionID string) (session.Checkpoint, bool, error) {
	sess, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return session.Checkpoint{}, false, err
	}
	if sess.ActiveCheckpointID == "" {
		return session.Checkpoint{}, false, nil
	}
	q := url.Values{
		"session_id": {"eq." + sessionID},
		"id":         {"eq." + sess.ActiveCheckpointID},
		"limit":      {"1"},
	}
	var rows []checkpointJSON
	if err := c.do(ctx, http.MethodGet, "/rest/v1/agent_context_checkpoints", q, nil, "", &rows); err != nil {
		return session.Checkpoint{}, false, err
	}
	if len(rows) == 0 {
		return session.Checkpoint{}, false, session.ErrNotFound
	}
	return rows[0].toCheckpoint(), true, nil
}

func toMessages(rows []messageJSON) []session.Message {
	out := make([]session.Message, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.toMessage())
	}
	return out
}

// rpc posts one PostgREST /rpc call and maps a failure to a domain error.
func (c *Client) rpc(ctx context.Context, fn string, body, out any) error {
	if err := c.do(ctx, http.MethodPost, "/rest/v1/rpc/"+fn, nil, body, "", out); err != nil {
		return mapError(err)
	}
	return nil
}

// mapError converts a PostgREST failure into the closest domain error. The
// transaction functions report a machine-readable reason in the SQLSTATE DETAIL
// field (PostgREST's `details`), so no prose parsing is involved.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	switch apiErr.Details {
	case "not_found":
		return session.ErrNotFound
	case "permission_denied":
		return &session.Error{Kind: session.KindPermissionDenied, Message: apiErr.Message}
	case "invalid_argument", "hash_conflict":
		return &session.Error{Kind: session.KindInvalidArgument, Message: apiErr.Message}
	case "stale_lease", "lease_expired", "lease_conflict", "revision_conflict", "mutation_conflict":
		return &session.Error{Kind: session.KindAborted, Message: apiErr.Message}
	case "session_ended", "request_busy", "request_terminal", "request_not_active", "request_cancelled",
		"parent_conflict", "config_mismatch":
		return &session.Error{Kind: session.KindFailedPrecondition, Message: apiErr.Message}
	}
	switch apiErr.Code {
	case "P0002", "02000":
		return session.ErrNotFound
	case "23505":
		return &session.Error{Kind: session.KindFailedPrecondition, Message: "conflicting concurrent update"}
	case "23514", "23503", "22P02":
		return &session.Error{Kind: session.KindInvalidArgument, Message: "invalid session data"}
	}
	return fmt.Errorf("postgrest %s: %w", apiErr.Code, err)
}
