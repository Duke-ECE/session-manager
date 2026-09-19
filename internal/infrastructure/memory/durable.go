// Package memory is the in-memory implementation of the session ports. It is
// the behavior-parity reference for the PostgREST adapter: the same state
// machine, sequence allocation, revision accounting, lease fencing, idempotency
// receipts, and immutability rules, without a database. It exists for unit
// tests and for zero-dependency local runs.
package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Duke-ECE/session-manager/internal/session"
)

// Store is an in-memory session.DurableStore and session.Store.
type Store struct {
	mu       sync.Mutex
	now      func() time.Time
	sessions map[string]*state
}

type state struct {
	sess        session.Session
	config      session.SessionConfig
	messages    []session.Message
	checkpoints map[string]session.Checkpoint
	// receipts keyed by mutation id; only the three mutated operations use them.
	appendReceipts  map[string]appendReceipt
	finishReceipts  map[string]finishReceipt
	publishReceipts map[string]publishReceipt
}

type appendReceipt struct {
	hash   string
	result session.AppendResult
}
type finishReceipt struct {
	hash   string
	result session.FinishResult
}
type publishReceipt struct {
	hash   string
	result session.PublishResult
}

// New returns an empty in-memory store.
func New() *Store {
	return &Store{now: time.Now, sessions: map[string]*state{}}
}

// SetClock overrides the clock; tests use it to drive lease expiry.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

var (
	_ session.DurableStore = (*Store)(nil)
	_ session.Store        = (*Store)(nil)
)

func errKind(kind session.Kind, msg string) error {
	return &session.Error{Kind: kind, Message: msg}
}

func invalidArg(msg string) error   { return errKind(session.KindInvalidArgument, msg) }
func notOwned(msg string) error     { return errKind(session.KindPermissionDenied, msg) }
func precondition(msg string) error { return errKind(session.KindFailedPrecondition, msg) }
func aborted(msg string) error      { return errKind(session.KindAborted, msg) }

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// --------------------------------------------------------------- lifecycle

func (s *Store) CreateDurableSession(_ context.Context, id, userID, agentID string, cfg session.SessionConfig) (session.Session, session.SessionConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[id]; ok {
		return session.Session{}, session.SessionConfig{}, precondition("session id already exists")
	}
	now := ts(s.now())
	sess := session.Session{
		ID:         id,
		UserID:     userID,
		Status:     "active",
		AgentID:    agentID,
		LLMModel:   cfg.Model,
		CreatedAt:  now,
		LastActive: now,
	}
	cfg.SessionID = id
	cfg.CreatedAt = now
	s.sessions[id] = &state{
		sess:            sess,
		config:          cfg,
		checkpoints:     map[string]session.Checkpoint{},
		appendReceipts:  map[string]appendReceipt{},
		finishReceipts:  map[string]finishReceipt{},
		publishReceipts: map[string]publishReceipt{},
	}
	return sess, cfg, nil
}

// CreateSession implements the v1 port: a session without a frozen configuration.
func (s *Store) CreateSession(_ context.Context, id, userID, llmModel, agentID string) (session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := ts(s.now())
	sess := session.Session{ID: id, UserID: userID, Status: "active", LLMModel: llmModel, AgentID: agentID, CreatedAt: now, LastActive: now}
	s.sessions[id] = &state{
		sess:            sess,
		checkpoints:     map[string]session.Checkpoint{},
		appendReceipts:  map[string]appendReceipt{},
		finishReceipts:  map[string]finishReceipt{},
		publishReceipts: map[string]publishReceipt{},
	}
	return sess, nil
}

func (s *Store) GetSession(_ context.Context, id string) (session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[id]
	if !ok {
		return session.Session{}, session.ErrNotFound
	}
	return st.sess, nil
}

func (s *Store) GetSessionConfig(_ context.Context, sessionID string) (session.SessionConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[sessionID]
	if !ok {
		return session.SessionConfig{}, session.ErrNotFound
	}
	if st.config.ConfigHash == "" {
		return session.SessionConfig{}, session.ErrNotFound
	}
	return st.config, nil
}

func (s *Store) ListSessions(_ context.Context, userID string, limit, offset int32) ([]session.Session, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []session.Session
	for _, st := range s.sessions {
		if st.sess.UserID == userID {
			out = append(out, st.sess)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastActive != out[j].LastActive {
			return out[i].LastActive > out[j].LastActive
		}
		return out[i].ID < out[j].ID
	})
	hasMore := len(out) > int(limit)+int(offset)
	if offset > 0 {
		if int(offset) >= len(out) {
			return nil, false, nil
		}
		out = out[offset:]
	}
	if len(out) > int(limit) {
		out = out[:limit]
	}
	return out, hasMore, nil
}

func (s *Store) EndSession(_ context.Context, id string, endedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[id]
	if !ok {
		return session.ErrNotFound
	}
	st.sess.Status = "ended"
	st.sess.EndedAt = ts(endedAt)
	return nil
}

func (s *Store) DeleteSession(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[id]; !ok {
		return session.ErrNotFound
	}
	delete(s.sessions, id)
	return nil
}

func (s *Store) SetTitle(_ context.Context, id, title string) (session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[id]
	if !ok {
		return session.Session{}, session.ErrNotFound
	}
	st.sess.Title = title
	return st.sess, nil
}

// ------------------------------------------------------------------ helpers

func (s *Store) lockSession(id string) (*state, error) {
	st, ok := s.sessions[id]
	if !ok {
		return nil, session.ErrNotFound
	}
	return st, nil
}

func (st *state) findMessage(id string) (int, bool) {
	for i := range st.messages {
		if st.messages[i].ID == id {
			return i, true
		}
	}
	return 0, false
}

func (st *state) findMessageBySeq(seq int32) (int, bool) {
	for i := range st.messages {
		if st.messages[i].Seq == seq {
			return i, true
		}
	}
	return 0, false
}

// leaseLive reports whether st holds an unexpired lease (now is the caller's
// clock reading).
func (st *state) leaseLive(now time.Time) bool {
	if st.sess.LeaseOwner == "" {
		return false
	}
	exp, err := time.Parse(time.RFC3339Nano, st.sess.LeaseExpiresAt)
	if err != nil {
		return false
	}
	return exp.After(now)
}

func (s *Store) appendMessages(st *state, requestMessageID string, drafts []session.MessageDraft, now time.Time) []session.Message {
	out := make([]session.Message, 0, len(drafts))
	for _, d := range drafts {
		st.sess.LastSeq++
		content, _ := session.EncodeContentBlocks(d.Content)
		m := session.Message{
			Seq:                  int32(st.sess.LastSeq),
			Role:                 d.Role,
			ContentJSON:          content,
			CreatedAt:            ts(now),
			ID:                   d.ID,
			RequestMessageID:     requestMessageID,
			FormatVersion:        d.FormatVersion,
			Status:               d.Status,
			ReplyToMessageID:     d.ReplyToMessageID,
			UsageJSON:            d.UsageJSON,
			ProviderMetadataJSON: d.ProviderMetadataJSON,
		}
		st.messages = append(st.messages, m)
		out = append(out, m)
	}
	st.sess.LastActive = ts(now)
	return out
}

func copyMessages(msgs []session.Message) []session.Message {
	return append([]session.Message(nil), msgs...)
}

// ---------------------------------------------------------- request admission

func (s *Store) BeginRequest(_ context.Context, p session.BeginRequestParams) (session.BeginRequestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.BeginRequestResult{}, err
	}
	if st.sess.UserID != p.UserID {
		return session.BeginRequestResult{}, notOwned("session belongs to another user")
	}
	if st.sess.Status == "ended" {
		return session.BeginRequestResult{}, precondition("session has ended")
	}
	for _, m := range st.messages {
		if m.ClientRequestID == p.ClientRequestID {
			if m.RequestHash == p.RequestHash {
				return session.BeginRequestResult{Session: st.sess, RequestMessage: m, Deduplicated: true}, nil
			}
			return session.BeginRequestResult{}, invalidArg("client_request_id was already used with different content")
		}
	}
	if st.sess.ActiveRequestMessageID != "" {
		return session.BeginRequestResult{}, precondition("another request is already active on this session")
	}
	now := s.now()
	content, _ := session.EncodeContentBlocks(p.Content)
	st.sess.LastSeq++
	st.sess.Revision++
	st.sess.ActiveRequestMessageID = p.RequestMessageID
	st.sess.LastActive = ts(now)
	root := session.Message{
		Seq:              int32(st.sess.LastSeq),
		Role:             session.RoleUser,
		ContentJSON:      content,
		CreatedAt:        ts(now),
		ID:               p.RequestMessageID,
		RequestMessageID: p.RequestMessageID,
		FormatVersion:    p.FormatVersion,
		Status:           session.MessageStatusComplete,
		ClientRequestID:  p.ClientRequestID,
		RequestHash:      p.RequestHash,
		ExecutionStatus:  session.ExecutionQueued,
	}
	st.messages = append(st.messages, root)
	return session.BeginRequestResult{Session: st.sess, RequestMessage: root}, nil
}

func (s *Store) AcquireLease(_ context.Context, p session.LeaseParams) (session.LeaseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.LeaseResult{}, err
	}
	if st.sess.Status == "ended" {
		return session.LeaseResult{}, precondition("session has ended")
	}
	if st.sess.ActiveRequestMessageID != p.RequestMessageID {
		return session.LeaseResult{}, precondition("request is not the active request of this session")
	}
	i, ok := st.findMessage(p.RequestMessageID)
	if !ok {
		return session.LeaseResult{}, session.ErrNotFound
	}
	root := &st.messages[i]
	switch root.ExecutionStatus {
	case session.ExecutionQueued, session.ExecutionRunning:
	default:
		return session.LeaseResult{}, precondition("request already finished")
	}
	if root.CancellationRequested {
		return session.LeaseResult{}, precondition("request was cancelled")
	}
	now := s.now()
	if st.sess.LeaseOwner != "" && st.sess.LeaseOwner != p.Owner && st.leaseLive(now) {
		return session.LeaseResult{}, aborted("session is leased by another runtime")
	}
	st.sess.LeaseOwner = p.Owner
	st.sess.LeaseGeneration++
	st.sess.LeaseExpiresAt = ts(now.Add(time.Duration(p.TTLSeconds) * time.Second))
	st.sess.Revision++
	if root.ExecutionStatus == session.ExecutionQueued {
		root.ExecutionStatus = session.ExecutionRunning
		root.StartedAt = ts(now)
	}
	return session.LeaseResult{
		Session: st.sess,
		Lease:   session.Lease{Owner: st.sess.LeaseOwner, Generation: st.sess.LeaseGeneration, ExpiresAt: st.sess.LeaseExpiresAt},
	}, nil
}

func (s *Store) RenewLease(_ context.Context, p session.RenewLeaseParams) (session.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.Lease{}, err
	}
	if st.sess.LeaseOwner != p.Owner || st.sess.LeaseGeneration != p.Generation {
		return session.Lease{}, aborted("lease owner or generation does not match")
	}
	now := s.now()
	if !st.leaseLive(now) {
		return session.Lease{}, aborted("lease has expired")
	}
	st.sess.LeaseExpiresAt = ts(now.Add(time.Duration(p.TTLSeconds) * time.Second))
	return session.Lease{Owner: st.sess.LeaseOwner, Generation: st.sess.LeaseGeneration, ExpiresAt: st.sess.LeaseExpiresAt}, nil
}

func (s *Store) ReleaseLease(_ context.Context, p session.ReleaseLeaseParams) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return false, err
	}
	if st.sess.LeaseOwner == p.Owner && st.sess.LeaseGeneration == p.Generation {
		st.sess.LeaseOwner = ""
		st.sess.LeaseExpiresAt = ""
		return true, nil
	}
	return false, nil
}

// requiresLease checks the fencing preconditions common to every mutation.
func (s *Store) requiresLease(st *state, requestMessageID string, guard session.MutationGuard, now time.Time) error {
	if guard.LeaseGeneration <= 0 || st.sess.LeaseOwner == "" || st.sess.LeaseGeneration != guard.LeaseGeneration {
		return aborted("a current lease generation is required")
	}
	if !st.leaseLive(now) {
		return aborted("lease has expired")
	}
	if st.sess.ActiveRequestMessageID != requestMessageID {
		return precondition("request is not the active request of this session")
	}
	i, ok := st.findMessage(requestMessageID)
	if !ok {
		return session.ErrNotFound
	}
	root := st.messages[i]
	switch root.ExecutionStatus {
	case session.ExecutionQueued, session.ExecutionRunning:
	default:
		return precondition("request already finished")
	}
	if guard.ExpectedRevision > 0 && st.sess.Revision != guard.ExpectedRevision {
		return aborted(fmt.Sprintf("revision is %d, expected %d", st.sess.Revision, guard.ExpectedRevision))
	}
	return nil
}

func (s *Store) AppendMessageBatch(_ context.Context, p session.AppendParams) (session.AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.AppendResult{}, err
	}
	if r, ok := st.appendReceipts[p.Guard.MutationID]; ok {
		if r.hash == p.Guard.MutationHash {
			out := r.result
			out.Deduplicated = true
			return out, nil
		}
		return session.AppendResult{}, aborted("mutation_id was reused with a different payload")
	}
	now := s.now()
	if st.sess.Status == "ended" {
		return session.AppendResult{}, precondition("session has ended")
	}
	if err := s.requiresLease(st, p.RequestMessageID, p.Guard, now); err != nil {
		return session.AppendResult{}, err
	}
	i, _ := st.findMessage(p.RequestMessageID)
	if st.messages[i].CancellationRequested {
		return session.AppendResult{}, precondition("request was cancelled")
	}
	msgs := s.appendMessages(st, p.RequestMessageID, p.Messages, now)
	st.sess.Revision++
	res := session.AppendResult{Session: st.sess, Messages: msgs}
	st.appendReceipts[p.Guard.MutationID] = appendReceipt{hash: p.Guard.MutationHash, result: res}
	return res, nil
}

func (s *Store) FinishRequest(_ context.Context, p session.FinishParams) (session.FinishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.FinishResult{}, err
	}
	if r, ok := st.finishReceipts[p.Guard.MutationID]; ok {
		if r.hash == p.Guard.MutationHash {
			out := r.result
			out.Deduplicated = true
			return out, nil
		}
		return session.FinishResult{}, aborted("mutation_id was reused with a different payload")
	}
	now := s.now()
	if err := s.requiresLease(st, p.RequestMessageID, p.Guard, now); err != nil {
		return session.FinishResult{}, err
	}
	msgs := s.appendMessages(st, p.RequestMessageID, p.Messages, now)
	i, _ := st.findMessage(p.RequestMessageID)
	root := &st.messages[i]
	root.ExecutionStatus = p.Status
	root.FinishedAt = ts(now)
	root.ErrorCode = p.ErrorCode
	st.sess.ActiveRequestMessageID = ""
	st.sess.LeaseOwner = ""
	st.sess.LeaseExpiresAt = ""
	st.sess.Revision++
	res := session.FinishResult{Session: st.sess, RequestMessage: *root, Messages: msgs}
	st.finishReceipts[p.Guard.MutationID] = finishReceipt{hash: p.Guard.MutationHash, result: res}
	return res, nil
}

func (s *Store) CancelRequest(_ context.Context, p session.CancelRequestParams) (session.CancelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.CancelResult{}, err
	}
	if st.sess.UserID != p.UserID {
		return session.CancelResult{}, notOwned("session belongs to another user")
	}
	target := p.RequestMessageID
	if target == "" {
		target = st.sess.ActiveRequestMessageID
	}
	if target == "" {
		return session.CancelResult{}, session.ErrNotFound
	}
	i, ok := st.findMessage(target)
	if !ok {
		return session.CancelResult{}, session.ErrNotFound
	}
	root := &st.messages[i]
	switch root.ExecutionStatus {
	case session.ExecutionQueued, session.ExecutionRunning:
	default:
		return session.CancelResult{RequestMessage: *root, Deduplicated: true}, nil
	}
	now := s.now()
	root.CancellationRequested = true
	if root.ExecutionStatus == session.ExecutionQueued || !st.leaseLive(now) {
		if root.ExecutionStatus == session.ExecutionRunning {
			root.ExecutionStatus = session.ExecutionInterrupted
		} else {
			root.ExecutionStatus = session.ExecutionCancelled
		}
		root.FinishedAt = ts(now)
		st.sess.ActiveRequestMessageID = ""
		st.sess.LeaseOwner = ""
		st.sess.LeaseExpiresAt = ""
	}
	st.sess.Revision++
	return session.CancelResult{RequestMessage: *root}, nil
}

func (s *Store) PublishCheckpoint(_ context.Context, p session.PublishParams) (session.PublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(p.SessionID)
	if err != nil {
		return session.PublishResult{}, err
	}
	if r, ok := st.publishReceipts[p.Guard.MutationID]; ok {
		if r.hash == p.Guard.MutationHash {
			out := r.result
			out.Deduplicated = true
			return out, nil
		}
		return session.PublishResult{}, aborted("mutation_id was reused with a different payload")
	}
	now := s.now()
	if p.Guard.LeaseGeneration <= 0 || st.sess.LeaseOwner == "" || st.sess.LeaseGeneration != p.Guard.LeaseGeneration {
		return session.PublishResult{}, aborted("a current lease generation is required")
	}
	if !st.leaseLive(now) {
		return session.PublishResult{}, aborted("lease has expired")
	}
	if st.sess.ActiveRequestMessageID == "" {
		return session.PublishResult{}, precondition("session has no active request")
	}
	i, ok := st.findMessage(st.sess.ActiveRequestMessageID)
	if !ok {
		return session.PublishResult{}, session.ErrNotFound
	}
	if st.messages[i].ExecutionStatus != session.ExecutionRunning || st.messages[i].CancellationRequested {
		return session.PublishResult{}, precondition("active request is not running")
	}
	if p.Checkpoint.ActiveRequestMessageID != st.sess.ActiveRequestMessageID {
		return session.PublishResult{}, precondition("checkpoint active_request_message_id does not match the active request")
	}
	if st.sess.ActiveCheckpointID != p.Checkpoint.ParentCheckpointID {
		return session.PublishResult{}, precondition("checkpoint parent is not the active checkpoint")
	}
	if p.Checkpoint.CoveredThroughSeq < 0 ||
		p.Checkpoint.SourceHeadSeq < p.Checkpoint.CoveredThroughSeq ||
		p.Checkpoint.ResumeAfterSeq < p.Checkpoint.CoveredThroughSeq {
		return session.PublishResult{}, invalidArg("checkpoint sequence bounds are invalid")
	}
	if p.Checkpoint.SourceHeadSeq > st.sess.LastSeq {
		return session.PublishResult{}, invalidArg("source_head_seq is beyond the allocated session sequence")
	}
	if st.config.ConfigHash == "" || st.config.ConfigHash != p.Checkpoint.ConfigHash {
		return session.PublishResult{}, precondition("checkpoint config_hash does not match the frozen session configuration")
	}
	if p.Guard.ExpectedRevision > 0 && st.sess.Revision != p.Guard.ExpectedRevision {
		return session.PublishResult{}, aborted(fmt.Sprintf("revision is %d, expected %d", st.sess.Revision, p.Guard.ExpectedRevision))
	}
	cp := p.Checkpoint
	cp.SessionID = p.SessionID
	cp.CreatedAt = ts(now)
	st.checkpoints[cp.ID] = cp
	st.sess.ActiveCheckpointID = cp.ID
	st.sess.Revision++
	res := session.PublishResult{Session: st.sess, Checkpoint: cp}
	st.publishReceipts[p.Guard.MutationID] = publishReceipt{hash: p.Guard.MutationHash, result: res}
	return res, nil
}

func (s *Store) GetActiveCheckpoint(_ context.Context, sessionID string) (session.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(sessionID)
	if err != nil {
		return session.Checkpoint{}, false, err
	}
	if st.sess.ActiveCheckpointID == "" {
		return session.Checkpoint{}, false, nil
	}
	cp, ok := st.checkpoints[st.sess.ActiveCheckpointID]
	return cp, ok, nil
}

func (s *Store) GetMessageWindow(_ context.Context, sessionID string, beforeSeq int64, limit int32) ([]session.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(sessionID)
	if err != nil {
		return nil, false, err
	}
	var eligible []session.Message
	for _, m := range st.messages {
		if beforeSeq > 0 && int64(m.Seq) >= beforeSeq {
			continue
		}
		eligible = append(eligible, m)
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].Seq < eligible[j].Seq })
	if limit <= 0 {
		return copyMessages(eligible), false, nil
	}
	hasMore := len(eligible) > int(limit)
	if hasMore {
		eligible = eligible[len(eligible)-int(limit):]
	}
	return copyMessages(eligible), hasMore, nil
}

// GetMessages implements the v1 port over the same rows, translating the int32
// limit the v1 contract uses.
func (s *Store) GetMessages(ctx context.Context, sessionID string, beforeSeq, limit int32) ([]session.Message, bool, error) {
	return s.GetMessageWindow(ctx, sessionID, int64(beforeSeq), limit)
}

// AppendMessages implements the v1 port: a plain append with no lease fencing.
func (s *Store) AppendMessages(_ context.Context, sessionID string, msgs []session.Message) ([]session.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.lockSession(sessionID)
	if err != nil {
		return nil, err
	}
	now := s.now()
	out := make([]session.Message, 0, len(msgs))
	for _, m := range msgs {
		st.sess.LastSeq++
		m.Seq = int32(st.sess.LastSeq)
		m.CreatedAt = ts(now)
		if m.ID == "" {
			m.ID = fmt.Sprintf("legacy-%d", st.sess.LastSeq)
		}
		st.messages = append(st.messages, m)
		out = append(out, m)
	}
	st.sess.LastActive = ts(now)
	return out, nil
}

func (s *Store) ListEndedBefore(_ context.Context, cutoff time.Time) ([]session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []session.Session
	for _, st := range s.sessions {
		if st.sess.Status != "ended" || st.sess.EndedAt == "" {
			continue
		}
		ended, err := time.Parse(time.RFC3339Nano, st.sess.EndedAt)
		if err != nil {
			continue
		}
		if ended.Before(cutoff) {
			out = append(out, st.sess)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// UserSessions is a small test helper: every session id owned by userID.
func (s *Store) UserSessions(userID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, st := range s.sessions {
		if st.sess.UserID == userID {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// TruncateTitle trims a title the way the SQL layer would; exported for parity
// checks.
func TruncateTitle(title string) string {
	if len([]rune(strings.TrimSpace(title))) > 120 {
		return string([]rune(strings.TrimSpace(title))[:120])
	}
	return strings.TrimSpace(title)
}
