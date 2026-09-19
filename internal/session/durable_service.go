package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// DurableService holds the canonical-message business rules on top of the
// DurableStore port: request admission, fenced execution, incremental
// persistence, terminal transitions, cancellation, and checkpoint publication.
//
// Trust model: user-scoped operations enforce ownership against the caller's
// verified identity; internal operations (admission, leases, appends, finish,
// checkpoint publication, and context reads) require the shared service token,
// because a bare caller-supplied user id is not proof of identity. The store
// re-checks ownership and fencing inside each transaction, so the service's
// checks are an early, cheap rejection rather than the security boundary.
type DurableService struct {
	store        DurableStore
	serviceToken string
}

// NewDurableService wires the v2 business rules to st. An empty serviceToken
// fails every internal path closed.
func NewDurableService(st DurableStore, serviceToken string) *DurableService {
	return &DurableService{store: st, serviceToken: serviceToken}
}

// ListSessions pagination bounds are shared with the v1 contract.
const (
	durableDefaultListLimit = 50
	durableMaxListLimit     = 200
	durableMaxMessageLimit  = 1000
)

// ComputeConfigHash returns a deterministic hash of the resolved configuration,
// used to bind checkpoints to the frozen configuration. Callers may supply their
// own ConfigHash; this is the default that keeps the binding stable across
// services.
func (c SessionConfig) ComputeConfigHash() string {
	h := sha256.New()
	fmt.Fprintf(h, "session-config/v%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00",
		c.FormatVersion, c.Provider, c.Model, c.BaseURL, c.CredentialRef,
		c.SystemPrompt, c.ContextWindowTokens, c.OutputLimitTokens)
	tools, _ := json.Marshal(c.Tools)
	h.Write(tools)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// CreateSession validates the resolved configuration, generates the session id,
// and persists the session and its frozen configuration atomically.
func (s *DurableService) CreateSession(ctx context.Context, userID, agentID string, cfg SessionConfig) (Session, SessionConfig, error) {
	if userID == "" {
		return Session{}, SessionConfig{}, invalidArgument("user_id is required")
	}
	if cfg.FormatVersion == 0 {
		cfg.FormatVersion = CanonicalFormatVersion
	}
	if cfg.ConfigHash == "" {
		cfg.ConfigHash = cfg.ComputeConfigHash()
	}
	if err := cfg.Validate(); err != nil {
		return Session{}, SessionConfig{}, invalidArgument(err.Error())
	}
	id, err := newSessionID()
	if err != nil {
		return Session{}, SessionConfig{}, internal(fmt.Sprintf("generate session id: %v", err))
	}
	return s.store.CreateDurableSession(ctx, id, userID, agentID, cfg)
}

// GetSession returns the session only if userID owns it.
func (s *DurableService) GetSession(ctx context.Context, sessionID, userID string) (Session, error) {
	return s.ownedSession(ctx, sessionID, userID)
}

// ListSessions returns one page of userID's sessions, most recently active first.
func (s *DurableService) ListSessions(ctx context.Context, userID string, limit, offset int32) ([]Session, bool, error) {
	if userID == "" {
		return nil, false, invalidArgument("user_id is required")
	}
	if limit <= 0 {
		limit = durableDefaultListLimit
	} else if limit > durableMaxListLimit {
		limit = durableMaxListLimit
	}
	if offset < 0 {
		offset = 0
	}
	return s.store.ListSessions(ctx, userID, limit, offset)
}

// EndSession marks the session ended and returns the resulting record; ending an
// already-ended session is a no-op.
func (s *DurableService) EndSession(ctx context.Context, sessionID, userID string) (Session, error) {
	sess, err := s.ownedSession(ctx, sessionID, userID)
	if err != nil {
		return Session{}, err
	}
	if sess.Status == "ended" {
		return sess, nil
	}
	if err := s.store.EndSession(ctx, sess.ID, time.Now()); err != nil {
		return Session{}, err
	}
	return s.store.GetSession(ctx, sess.ID)
}

// DeleteSession removes the session and everything it owns.
func (s *DurableService) DeleteSession(ctx context.Context, sessionID, userID string) error {
	sess, err := s.ownedSession(ctx, sessionID, userID)
	if err != nil {
		return err
	}
	return s.store.DeleteSession(ctx, sess.ID)
}

// SetTitle follows the owner-or-token pattern of the v1 contract.
func (s *DurableService) SetTitle(ctx context.Context, sessionID, title, userID string, tokens []string) (Session, error) {
	if sessionID == "" {
		return Session{}, invalidArgument("session_id is required")
	}
	if title == "" {
		return Session{}, invalidArgument("title is required")
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

// BeginRequest admits one user request. It is internal: the session-manager is
// the only writer of canonical history, so a service token is required.
func (s *DurableService) BeginRequest(ctx context.Context, tokens []string, p BeginRequestParams) (BeginRequestResult, error) {
	if !s.tokenOK(tokens) {
		return BeginRequestResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" {
		return BeginRequestResult{}, invalidArgument("session_id is required")
	}
	if p.UserID == "" {
		return BeginRequestResult{}, invalidArgument("user_id is required")
	}
	if p.RequestMessageID == "" || p.ClientRequestID == "" || p.RequestHash == "" {
		return BeginRequestResult{}, invalidArgument("request_message_id, client_request_id and request_hash are required")
	}
	if p.FormatVersion <= 0 {
		p.FormatVersion = CanonicalFormatVersion
	}
	if err := ValidateContent(p.Content); err != nil {
		return BeginRequestResult{}, invalidArgument(err.Error())
	}
	return s.store.BeginRequest(ctx, p)
}

// AcquireLease takes fenced ownership of the active request.
func (s *DurableService) AcquireLease(ctx context.Context, tokens []string, p LeaseParams) (LeaseResult, error) {
	if !s.tokenOK(tokens) {
		return LeaseResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" || p.RequestMessageID == "" || p.Owner == "" {
		return LeaseResult{}, invalidArgument("session_id, request_message_id and owner are required")
	}
	if p.TTLSeconds <= 0 {
		p.TTLSeconds = DefaultLeaseTTLSeconds
	}
	return s.store.AcquireLease(ctx, p)
}

// DefaultLeaseTTLSeconds is the initial lease duration from the design.
const DefaultLeaseTTLSeconds = 90

// RenewLease extends a held lease.
func (s *DurableService) RenewLease(ctx context.Context, tokens []string, p RenewLeaseParams) (Lease, error) {
	if !s.tokenOK(tokens) {
		return Lease{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" || p.Owner == "" || p.Generation <= 0 {
		return Lease{}, invalidArgument("session_id, owner and generation are required")
	}
	if p.TTLSeconds <= 0 {
		p.TTLSeconds = DefaultLeaseTTLSeconds
	}
	return s.store.RenewLease(ctx, p)
}

// ReleaseLease releases a held lease.
func (s *DurableService) ReleaseLease(ctx context.Context, tokens []string, p ReleaseLeaseParams) (bool, error) {
	if !s.tokenOK(tokens) {
		return false, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" || p.Owner == "" || p.Generation <= 0 {
		return false, invalidArgument("session_id, owner and generation are required")
	}
	return s.store.ReleaseLease(ctx, p)
}

// AppendMessages commits one idempotent, fenced batch of completed assistant or
// tool messages. Tool-call ids must be unique within the batch so result links
// are unambiguous.
func (s *DurableService) AppendMessages(ctx context.Context, tokens []string, p AppendParams) (AppendResult, error) {
	if !s.tokenOK(tokens) {
		return AppendResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" || p.RequestMessageID == "" {
		return AppendResult{}, invalidArgument("session_id and request_message_id are required")
	}
	if p.Guard.MutationID == "" || p.Guard.MutationHash == "" {
		return AppendResult{}, invalidArgument("mutation_id and mutation_hash are required")
	}
	if len(p.Messages) == 0 {
		return AppendResult{}, invalidArgument("messages must not be empty")
	}
	if err := validateDrafts(p.Messages); err != nil {
		return AppendResult{}, invalidArgument(err.Error())
	}
	return s.store.AppendMessageBatch(ctx, p)
}

// FinishRequest writes the final batch and the root's terminal state.
func (s *DurableService) FinishRequest(ctx context.Context, tokens []string, p FinishParams) (FinishResult, error) {
	if !s.tokenOK(tokens) {
		return FinishResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" || p.RequestMessageID == "" {
		return FinishResult{}, invalidArgument("session_id and request_message_id are required")
	}
	if p.Guard.MutationID == "" || p.Guard.MutationHash == "" {
		return FinishResult{}, invalidArgument("mutation_id and mutation_hash are required")
	}
	switch p.Status {
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled, ExecutionInterrupted:
	default:
		return FinishResult{}, invalidArgument(fmt.Sprintf("finish status %q is not terminal", p.Status))
	}
	if err := validateDrafts(p.Messages); err != nil {
		return FinishResult{}, invalidArgument(err.Error())
	}
	return s.store.FinishRequest(ctx, p)
}

// CancelRequest cancels a request. The owner may cancel through the backend; with
// no user identity the runtime's service token decides. Ownership is re-checked
// in the store transaction.
func (s *DurableService) CancelRequest(ctx context.Context, sessionID, userID string, tokens []string, requestMessageID string) (CancelResult, error) {
	if sessionID == "" {
		return CancelResult{}, invalidArgument("session_id is required")
	}
	if userID == "" {
		if !s.tokenOK(tokens) {
			return CancelResult{}, unauthenticated("owner user_id or valid x-service-token metadata is required")
		}
	}
	return s.store.CancelRequest(ctx, CancelRequestParams{
		SessionID:        sessionID,
		UserID:           userID,
		RequestMessageID: requestMessageID,
	})
}

// PublishCheckpoint activates a compaction checkpoint under the caller's lease.
func (s *DurableService) PublishCheckpoint(ctx context.Context, tokens []string, p PublishParams) (PublishResult, error) {
	if !s.tokenOK(tokens) {
		return PublishResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	if p.SessionID == "" {
		return PublishResult{}, invalidArgument("session_id is required")
	}
	if p.Guard.MutationID == "" || p.Guard.MutationHash == "" {
		return PublishResult{}, invalidArgument("mutation_id and mutation_hash are required")
	}
	if p.Checkpoint.ID == "" {
		return PublishResult{}, invalidArgument("checkpoint id is required")
	}
	if p.Checkpoint.FormatVersion <= 0 {
		p.Checkpoint.FormatVersion = CanonicalFormatVersion
	}
	if err := ValidateContent(p.Checkpoint.Summary); err != nil {
		return PublishResult{}, invalidArgument(fmt.Sprintf("checkpoint summary: %v", err))
	}
	if p.Checkpoint.CoveredThroughSeq < 0 ||
		p.Checkpoint.SourceHeadSeq < p.Checkpoint.CoveredThroughSeq ||
		p.Checkpoint.ResumeAfterSeq < p.Checkpoint.CoveredThroughSeq {
		return PublishResult{}, invalidArgument("checkpoint sequence bounds are invalid")
	}
	return s.store.PublishCheckpoint(ctx, p)
}

// GetMessages returns canonical history and the session revision the window was
// read at. The owner reads freely; a non-owner user id is PermissionDenied; with
// no user identity the service token decides. Owner-path reads redact LLM
// api_keys from legacy config turns, exactly as the v1 transcript contract does.
func (s *DurableService) GetMessages(ctx context.Context, sessionID, userID string, tokens []string, beforeSeq int64, limit int32) ([]Message, bool, int64, error) {
	if sessionID == "" {
		return nil, false, 0, invalidArgument("session_id is required")
	}
	if limit < 0 {
		limit = 0
	} else if limit > durableMaxMessageLimit {
		limit = durableMaxMessageLimit
	}
	if beforeSeq < 0 {
		beforeSeq = 0
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return nil, false, 0, err
	}
	if userID != "" {
		if userID != sess.UserID {
			return nil, false, 0, permissionDenied("session belongs to another user")
		}
	} else if !s.tokenOK(tokens) {
		return nil, false, 0, unauthenticated("owner user_id or valid x-service-token metadata is required")
	}
	msgs, hasMore, err := s.store.GetMessageWindow(ctx, sess.ID, beforeSeq, limit)
	if err != nil {
		return nil, false, 0, err
	}
	if userID != "" {
		redactConfigAPIKeys(msgs)
	}
	return msgs, hasMore, sess.Revision, nil
}

// GetExecutionContext returns the durable input a runtime hydrates from: the
// session high-water mark, the frozen configuration, and the active checkpoint.
//
// Canonical messages are deliberately not embedded: the retained range is read
// through GetMessages windowed pagination, so a hydration response stays bounded
// and a truncated history can never be mistaken for a complete one. It is an
// internal operation and requires the service token.
func (s *DurableService) GetExecutionContext(ctx context.Context, sessionID string, tokens []string) (ContextState, error) {
	if !s.tokenOK(tokens) {
		return ContextState{}, unauthenticated("valid x-service-token metadata is required")
	}
	if sessionID == "" {
		return ContextState{}, invalidArgument("session_id is required")
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return ContextState{}, err
	}
	cfg, err := s.store.GetSessionConfig(ctx, sessionID)
	if err != nil {
		return ContextState{}, err
	}
	cp, ok, err := s.store.GetActiveCheckpoint(ctx, sessionID)
	if err != nil {
		return ContextState{}, err
	}
	return ContextState{Session: sess, Config: cfg, Checkpoint: cp, HasCheckpoint: ok}, nil
}

// validateDrafts checks every draft and rejects duplicate tool-call ids across
// the batch.
func validateDrafts(drafts []MessageDraft) error {
	seen := map[string]bool{}
	for _, d := range drafts {
		if err := d.Validate(); err != nil {
			return err
		}
		var ids []string
		toolCallIDs(d.Content, &ids)
		for _, id := range ids {
			if seen[id] {
				return fmt.Errorf("tool_call id %q appears more than once in the batch", id)
			}
			seen[id] = true
		}
	}
	return nil
}

// ownedSession validates the user-scoped preconditions and returns the session
// only if userID owns it.
func (s *DurableService) ownedSession(ctx context.Context, sessionID, userID string) (Session, error) {
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

// tokenOK reports whether any presented token matches the shared service token.
func (s *DurableService) tokenOK(presented []string) bool {
	return presentedTokenOK(s.serviceToken, presented)
}
