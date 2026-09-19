package session_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/session"
)

const (
	testToken = "test-service-token"
	userA     = "11111111-1111-1111-1111-111111111111"
	userB     = "22222222-2222-2222-2222-222222222222"
)

func token() []string { return []string{testToken} }

func testConfig() session.SessionConfig {
	return session.SessionConfig{
		FormatVersion:       1,
		ConfigHash:          "cfg-hash-1",
		SystemPrompt:        "you are a test assistant",
		Provider:            "openai",
		Model:               "gpt-4o-mini",
		BaseURL:             "https://llm.example.com/v1",
		CredentialRef:       "cred-ref-1",
		ContextWindowTokens: 128000,
		OutputLimitTokens:   4096,
		Tools:               []session.ToolDefinition{{Name: "bash", Version: "1"}},
	}
}

func newFixture(t *testing.T) (*session.DurableService, *memory.Store, session.Session) {
	t.Helper()
	st := memory.New()
	svc := session.NewDurableService(st, testToken)
	sess, _, err := svc.CreateSession(context.Background(), userA, "agent-1", testConfig())
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return svc, st, sess
}

func text(s string) session.ContentBlock {
	return session.ContentBlock{Type: session.BlockText, Text: s}
}

func toolCall(id, name string) session.ContentBlock {
	return session.ContentBlock{Type: session.BlockToolCall, ID: id, Name: name, ArgumentsJSON: `{"command":"pwd"}`}
}

func toolResult(callID, body string) session.ContentBlock {
	return session.ContentBlock{Type: session.BlockToolResult, ToolCallID: callID, Status: session.ToolResultSuccess, Content: []session.ContentBlock{text(body)}}
}

func draft(id, role string, blocks ...session.ContentBlock) session.MessageDraft {
	return session.MessageDraft{ID: id, Role: role, Content: blocks, FormatVersion: 1, Status: session.MessageStatusComplete}
}

func begin(t *testing.T, svc *session.DurableService, sid, reqID, hash string) session.BeginRequestResult {
	t.Helper()
	res, err := svc.BeginRequest(context.Background(), token(), session.BeginRequestParams{
		SessionID: sid, UserID: userA, RequestMessageID: reqID,
		ClientRequestID: reqID, RequestHash: hash, Content: []session.ContentBlock{text("hi")}, FormatVersion: 1,
	})
	if err != nil {
		t.Fatalf("BeginRequest(%s): %v", reqID, err)
	}
	return res
}

func acquire(t *testing.T, svc *session.DurableService, sid, reqID, owner string) session.LeaseResult {
	t.Helper()
	res, err := svc.AcquireLease(context.Background(), token(), session.LeaseParams{
		SessionID: sid, RequestMessageID: reqID, Owner: owner, TTLSeconds: 90,
	})
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	return res
}

func kindOf(t *testing.T, err error) session.Kind {
	t.Helper()
	var domErr *session.Error
	if !errors.As(err, &domErr) {
		t.Fatalf("error %v is not a domain error", err)
	}
	return domErr.Kind
}

func TestDurableAdmissionAndDeduplication(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	// A request from a different user is rejected before anything is written.
	_, err := svc.BeginRequest(ctx, token(), session.BeginRequestParams{
		SessionID: sess.ID, UserID: userB, RequestMessageID: "msg-x",
		ClientRequestID: "req-x", RequestHash: "h", Content: []session.ContentBlock{text("hi")}, FormatVersion: 1,
	})
	if got := kindOf(t, err); got != session.KindPermissionDenied {
		t.Fatalf("cross-user BeginRequest kind = %v, want PermissionDenied", got)
	}

	// Internal admission requires the service token.
	if _, err := svc.BeginRequest(ctx, nil, session.BeginRequestParams{SessionID: sess.ID, UserID: userA}); err == nil {
		t.Fatal("BeginRequest without a service token succeeded")
	}

	res := begin(t, svc, sess.ID, "msg-r1", "hash-1")
	if res.Deduplicated {
		t.Error("first request reported as deduplicated")
	}
	if res.RequestMessage.ExecutionStatus != session.ExecutionQueued || res.RequestMessage.Seq != 1 {
		t.Errorf("root = status %q seq %d, want queued/1", res.RequestMessage.ExecutionStatus, res.RequestMessage.Seq)
	}
	if res.Session.Revision != 1 || res.Session.ActiveRequestMessageID != "msg-r1" {
		t.Errorf("session = revision %d active %q, want 1/msg-r1", res.Session.Revision, res.Session.ActiveRequestMessageID)
	}

	// Same id + hash: the existing root is returned and nothing is allocated.
	replay := begin(t, svc, sess.ID, "msg-r1", "hash-1")
	if !replay.Deduplicated || replay.RequestMessage.ID != "msg-r1" {
		t.Errorf("replay = deduplicated %v id %q, want true/msg-r1", replay.Deduplicated, replay.RequestMessage.ID)
	}
	if replay.Session.LastSeq != 1 || replay.Session.Revision != 1 {
		t.Errorf("replay changed durable state: last_seq %d revision %d", replay.Session.LastSeq, replay.Session.Revision)
	}

	// Same id, different content: a conflict, never a silent overwrite.
	_, err = svc.BeginRequest(ctx, token(), session.BeginRequestParams{
		SessionID: sess.ID, UserID: userA, RequestMessageID: "msg-r1b",
		ClientRequestID: "msg-r1", RequestHash: "hash-different",
		Content: []session.ContentBlock{text("changed")}, FormatVersion: 1,
	})
	if got := kindOf(t, err); got != session.KindInvalidArgument {
		t.Fatalf("hash conflict kind = %v, want InvalidArgument", got)
	}

	// A second concurrent request is refused while one is active.
	_, err = svc.BeginRequest(ctx, token(), session.BeginRequestParams{
		SessionID: sess.ID, UserID: userA, RequestMessageID: "msg-r2",
		ClientRequestID: "req-2", RequestHash: "hash-2",
		Content: []session.ContentBlock{text("again")}, FormatVersion: 1,
	})
	if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("second concurrent request kind = %v, want FailedPrecondition", got)
	}
}

func TestDurableLeaseFencingAndExpiry(t *testing.T) {
	svc, st, sess := newFixture(t)
	ctx := context.Background()
	base := time.Now()
	st.SetClock(func() time.Time { return base })

	begin(t, svc, sess.ID, "msg-r1", "hash-1")

	res := acquire(t, svc, sess.ID, "msg-r1", "runtime-a")
	if res.Lease.Generation != 1 {
		t.Fatalf("first lease generation = %d, want 1", res.Lease.Generation)
	}
	if root, _, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 0); err != nil {
		t.Fatalf("GetMessages: %v", err)
	} else if root[0].ExecutionStatus != session.ExecutionRunning || root[0].StartedAt == "" {
		t.Errorf("root after acquire = %q started %q, want running with started_at", root[0].ExecutionStatus, root[0].StartedAt)
	}

	// A live lease held by someone else cannot be taken.
	if _, err := svc.AcquireLease(ctx, token(), session.LeaseParams{SessionID: sess.ID, RequestMessageID: "msg-r1", Owner: "runtime-b", TTLSeconds: 90}); err == nil {
		t.Fatal("second runtime acquired a live lease")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("live-lease conflict kind = %v, want Aborted", got)
	}

	// Renewal is not semantic state: no revision bump.
	rev := res.Session.Revision
	lease, err := svc.RenewLease(ctx, token(), session.RenewLeaseParams{SessionID: sess.ID, Owner: "runtime-a", Generation: 1, TTLSeconds: 90})
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if lease.Generation != 1 {
		t.Errorf("renewal changed generation to %d", lease.Generation)
	}
	if after, err := st.GetSession(ctx, sess.ID); err != nil {
		t.Fatalf("GetSession: %v", err)
	} else if after.Revision != rev {
		t.Errorf("renewal advanced revision %d -> %d", rev, after.Revision)
	}

	// Wrong generation is stale.
	if _, err := svc.RenewLease(ctx, token(), session.RenewLeaseParams{SessionID: sess.ID, Owner: "runtime-a", Generation: 7, TTLSeconds: 90}); err == nil {
		t.Fatal("renewal with a wrong generation succeeded")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("stale renewal kind = %v, want Aborted", got)
	}

	// Expiry: writes are refused, then another runtime takes over with a bumped
	// generation that fences the old owner out.
	base = base.Add(2 * time.Minute)
	appendParams := session.AppendParams{
		SessionID: sess.ID, RequestMessageID: "msg-r1",
		Guard:    session.MutationGuard{MutationID: "m-expired", MutationHash: "h", LeaseGeneration: 1},
		Messages: []session.MessageDraft{draft("msg-a1", session.RoleAssistant, text("x"))},
	}
	if _, err := svc.AppendMessages(ctx, token(), appendParams); err == nil {
		t.Fatal("append under an expired lease succeeded")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("expired-lease append kind = %v, want Aborted", got)
	}

	takeover := acquire(t, svc, sess.ID, "msg-r1", "runtime-b")
	if takeover.Lease.Generation != 2 {
		t.Fatalf("takeover generation = %d, want 2", takeover.Lease.Generation)
	}
	appendParams.Guard = session.MutationGuard{MutationID: "m-stale", MutationHash: "h", LeaseGeneration: 1}
	if _, err := svc.AppendMessages(ctx, token(), appendParams); err == nil {
		t.Fatal("the fenced-out owner still wrote")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("fenced-out append kind = %v, want Aborted", got)
	}
}

func TestDurableAppendIdempotencyAndRevisionConflict(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	lease := acquire(t, svc, sess.ID, "msg-r1", "runtime-a")

	batch := session.AppendParams{
		SessionID: sess.ID, RequestMessageID: "msg-r1",
		Guard: session.MutationGuard{
			MutationID: "m-1", MutationHash: "mh-1",
			ExpectedRevision: lease.Session.Revision, LeaseGeneration: lease.Lease.Generation,
		},
		Messages: []session.MessageDraft{
			draft("msg-a1", session.RoleAssistant, text("running pwd"), toolCall("call_1", "bash")),
		},
	}
	res, err := svc.AppendMessages(ctx, token(), batch)
	if err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	if len(res.Messages) != 1 || res.Messages[0].Seq != 2 {
		t.Fatalf("append = %d messages seq %v, want 1 at seq 2", len(res.Messages), res.Messages)
	}

	// Idempotent replay: the stored result comes back, nothing is written twice.
	replay, err := svc.AppendMessages(ctx, token(), batch)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Deduplicated || replay.Session.LastSeq != res.Session.LastSeq {
		t.Errorf("replay = deduplicated %v last_seq %d, want true/%d", replay.Deduplicated, replay.Session.LastSeq, res.Session.LastSeq)
	}

	// Same mutation id, different payload: a conflict.
	conflict := batch
	conflict.Guard.MutationHash = "mh-other"
	if _, err := svc.AppendMessages(ctx, token(), conflict); err == nil {
		t.Fatal("mutation id reuse with a different payload succeeded")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("mutation conflict kind = %v, want Aborted", got)
	}

	// A stale expected revision is refused.
	stale := batch
	stale.Guard = session.MutationGuard{MutationID: "m-2", MutationHash: "mh-2", ExpectedRevision: 999, LeaseGeneration: lease.Lease.Generation}
	if _, err := svc.AppendMessages(ctx, token(), stale); err == nil {
		t.Fatal("append with a stale expected revision succeeded")
	} else if got := kindOf(t, err); got != session.KindAborted {
		t.Fatalf("revision conflict kind = %v, want Aborted", got)
	}

	// Tool results link to their call by id.
	result, err := svc.AppendMessages(ctx, token(), session.AppendParams{
		SessionID: sess.ID, RequestMessageID: "msg-r1",
		Guard:    session.MutationGuard{MutationID: "m-3", MutationHash: "mh-3", LeaseGeneration: lease.Lease.Generation},
		Messages: []session.MessageDraft{draft("msg-t1", session.RoleTool, toolResult("call_1", "/workspace"))},
	})
	if err != nil {
		t.Fatalf("append tool result: %v", err)
	}
	if result.Messages[0].ReplyToMessageID != "" {
		t.Errorf("unexpected reply link %q", result.Messages[0].ReplyToMessageID)
	}
	if blocks, err := session.ContentBlocks(result.Messages[0].ContentJSON); err != nil || blocks[0].ToolCallID != "call_1" {
		t.Errorf("tool result blocks = %v (err %v), want tool_call_id call_1", blocks, err)
	}

	// Duplicate tool-call ids inside one batch are rejected up front.
	dup := session.AppendParams{
		SessionID: sess.ID, RequestMessageID: "msg-r1",
		Guard:    session.MutationGuard{MutationID: "m-4", MutationHash: "mh-4", LeaseGeneration: lease.Lease.Generation},
		Messages: []session.MessageDraft{draft("msg-a2", session.RoleAssistant, toolCall("call_1", "bash"), toolCall("call_1", "bash"))},
	}
	if _, err := svc.AppendMessages(ctx, token(), dup); err == nil {
		t.Fatal("duplicate tool_call ids in one batch were accepted")
	} else if got := kindOf(t, err); got != session.KindInvalidArgument {
		t.Fatalf("duplicate tool_call kind = %v, want InvalidArgument", got)
	}
}

func TestDurableFinishTerminalityAndImmutability(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	lease := acquire(t, svc, sess.ID, "msg-r1", "runtime-a")

	fin, err := svc.FinishRequest(ctx, token(), session.FinishParams{
		SessionID: sess.ID, RequestMessageID: "msg-r1",
		Guard:    session.MutationGuard{MutationID: "f-1", MutationHash: "fh-1", LeaseGeneration: lease.Lease.Generation},
		Status:   session.ExecutionCompleted,
		Messages: []session.MessageDraft{draft("msg-a1", session.RoleAssistant, text("done"))},
	})
	if err != nil {
		t.Fatalf("FinishRequest: %v", err)
	}
	if fin.RequestMessage.ExecutionStatus != session.ExecutionCompleted || fin.RequestMessage.FinishedAt == "" {
		t.Errorf("root = %q finished %q, want completed with finished_at", fin.RequestMessage.ExecutionStatus, fin.RequestMessage.FinishedAt)
	}
	if fin.Session.ActiveRequestMessageID != "" || fin.Session.LeaseOwner != "" {
		t.Errorf("finish left active request %q / lease %q", fin.Session.ActiveRequestMessageID, fin.Session.LeaseOwner)
	}
	if fin.Session.LastSeq != 2 {
		t.Errorf("last_seq after finish = %d, want 2", fin.Session.LastSeq)
	}

	// Terminal requests reject new appends even with a fresh generation.
	lease2, err := svc.AcquireLease(ctx, token(), session.LeaseParams{SessionID: sess.ID, RequestMessageID: "msg-r1", Owner: "runtime-a", TTLSeconds: 90})
	if err == nil {
		_, err = svc.AppendMessages(ctx, token(), session.AppendParams{
			SessionID: sess.ID, RequestMessageID: "msg-r1",
			Guard:    session.MutationGuard{MutationID: "a-late", MutationHash: "lh", LeaseGeneration: lease2.Lease.Generation},
			Messages: []session.MessageDraft{draft("msg-late", session.RoleAssistant, text("late"))},
		})
	}
	if err == nil {
		t.Fatal("appending to a finished request succeeded")
	}
	if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("append after finish kind = %v, want FailedPrecondition", got)
	}

	// A finished session admits a new request, and history is intact.
	second := begin(t, svc, sess.ID, "msg-r2", "hash-2")
	if second.RequestMessage.Seq != 3 {
		t.Errorf("second root seq = %d, want 3", second.RequestMessage.Seq)
	}
	msgs, _, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 0)
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("history = %d messages, want 3", len(msgs))
	}
	if msgs[0].ContentJSON == "" || msgs[0].Status != session.MessageStatusComplete {
		t.Errorf("completed content is not intact: %+v", msgs[0])
	}
}

func TestDurableCancellation(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	// Cancelling a queued request terminates it outright: nobody is executing it.
	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	res, err := svc.CancelRequest(ctx, sess.ID, userA, nil, "")
	if err != nil {
		t.Fatalf("CancelRequest: %v", err)
	}
	if res.RequestMessage.ExecutionStatus != session.ExecutionCancelled || !res.RequestMessage.CancellationRequested {
		t.Errorf("queued cancel = status %q flag %v, want cancelled/true", res.RequestMessage.ExecutionStatus, res.RequestMessage.CancellationRequested)
	}
	if after, err := svc.GetSession(ctx, sess.ID, userA); err != nil {
		t.Fatalf("GetSession: %v", err)
	} else if after.ActiveRequestMessageID != "" {
		t.Errorf("queued cancel left active request %q", after.ActiveRequestMessageID)
	}

	// A non-owner cannot cancel.
	begin(t, svc, sess.ID, "msg-r2", "hash-2")
	if _, err := svc.CancelRequest(ctx, sess.ID, userB, nil, ""); err == nil {
		t.Fatal("cross-user cancel succeeded")
	} else if got := kindOf(t, err); got != session.KindPermissionDenied {
		t.Fatalf("cross-user cancel kind = %v, want PermissionDenied", got)
	}

	// A running request with a live lease is only flagged: its runtime finishes
	// with the partial output it already holds.
	lease := acquire(t, svc, sess.ID, "msg-r2", "runtime-a")
	res, err = svc.CancelRequest(ctx, sess.ID, userA, nil, "")
	if err != nil {
		t.Fatalf("CancelRequest(live): %v", err)
	}
	if !res.RequestMessage.CancellationRequested || res.RequestMessage.ExecutionStatus != session.ExecutionRunning {
		t.Errorf("live cancel = status %q flag %v, want running/true", res.RequestMessage.ExecutionStatus, res.RequestMessage.CancellationRequested)
	}

	appendAfterCancel := session.AppendParams{
		SessionID: sess.ID, RequestMessageID: "msg-r2",
		Guard:    session.MutationGuard{MutationID: "c-1", MutationHash: "ch", LeaseGeneration: lease.Lease.Generation},
		Messages: []session.MessageDraft{draft("msg-more", session.RoleAssistant, text("more"))},
	}
	if _, err := svc.AppendMessages(ctx, token(), appendAfterCancel); err == nil {
		t.Fatal("append after cancellation was accepted")
	} else if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("append after cancel kind = %v, want FailedPrecondition", got)
	}

	// A cancelled request can never publish a checkpoint.
	_, err = svc.PublishCheckpoint(ctx, token(), session.PublishParams{
		SessionID: sess.ID,
		Guard:     session.MutationGuard{MutationID: "cp-1", MutationHash: "cph", LeaseGeneration: lease.Lease.Generation},
		Checkpoint: session.Checkpoint{
			ID: "cp-1", CoveredThroughSeq: 1, SourceHeadSeq: 2, SourceRevision: 1,
			ConfigHash: "cfg-hash-1", ActiveRequestMessageID: "msg-r2", ResumeAfterSeq: 2,
			Summary: []session.ContentBlock{text("s")}, FormatVersion: 1,
		},
	})
	if err == nil {
		t.Fatal("checkpoint published for a cancelled request")
	} else if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("cancelled publish kind = %v, want FailedPrecondition", got)
	}

	// The runtime finishes the cancelled request with a partial snapshot.
	fin, err := svc.FinishRequest(ctx, token(), session.FinishParams{
		SessionID: sess.ID, RequestMessageID: "msg-r2",
		Guard:     session.MutationGuard{MutationID: "f-cancel", MutationHash: "fh-cancel", LeaseGeneration: lease.Lease.Generation},
		Status:    session.ExecutionCancelled,
		Messages:  []session.MessageDraft{{ID: "msg-partial", Role: session.RoleAssistant, Content: []session.ContentBlock{text("half a sen")}, FormatVersion: 1, Status: session.MessageStatusPartial}},
		ErrorCode: "cancelled_by_user",
	})
	if err != nil {
		t.Fatalf("FinishRequest(cancelled): %v", err)
	}
	if fin.Messages[0].Status != session.MessageStatusPartial {
		t.Errorf("partial message status = %q, want partial", fin.Messages[0].Status)
	}
	if fin.RequestMessage.ErrorCode != "cancelled_by_user" {
		t.Errorf("error_code = %q, want cancelled_by_user", fin.RequestMessage.ErrorCode)
	}
}

func TestDurableCheckpointPublication(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	lease := acquire(t, svc, sess.ID, "msg-r1", "runtime-a")

	pub, err := svc.PublishCheckpoint(ctx, token(), session.PublishParams{
		SessionID: sess.ID,
		Guard:     session.MutationGuard{MutationID: "cp-1", MutationHash: "cph-1", LeaseGeneration: lease.Lease.Generation},
		Checkpoint: session.Checkpoint{
			ID: "cp-1", CoveredThroughSeq: 1, SourceHeadSeq: 1, SourceRevision: lease.Session.Revision,
			ConfigHash: "cfg-hash-1", ActiveRequestMessageID: "msg-r1", ResumeAfterSeq: 1,
			Summary: []session.ContentBlock{text("the user said hi")}, FormatVersion: 1,
			PromptVersion: "v1", SummarizerProvider: "openai", SummarizerModel: "gpt-4o-mini",
			EstimatedTokensBefore: 9000, EstimatedTokensAfter: 1200,
		},
	})
	if err != nil {
		t.Fatalf("PublishCheckpoint: %v", err)
	}
	if pub.Session.ActiveCheckpointID != "cp-1" || pub.Checkpoint.Summary[0].Text != "the user said hi" {
		t.Errorf("publish = active %q summary %v", pub.Session.ActiveCheckpointID, pub.Checkpoint.Summary)
	}

	// The active checkpoint is readable through the context path.
	state, err := svc.GetExecutionContext(ctx, sess.ID, token())
	if err != nil {
		t.Fatalf("GetExecutionContext: %v", err)
	}
	if !state.HasCheckpoint || state.Checkpoint.ID != "cp-1" {
		t.Errorf("context checkpoint = %+v, want cp-1", state.Checkpoint)
	}
	if state.Config.ConfigHash != "cfg-hash-1" {
		t.Errorf("context config hash = %q", state.Config.ConfigHash)
	}

	// A parent that is not the active checkpoint is refused.
	bad := session.PublishParams{
		SessionID: sess.ID,
		Guard:     session.MutationGuard{MutationID: "cp-2", MutationHash: "cph-2", LeaseGeneration: lease.Lease.Generation},
		Checkpoint: session.Checkpoint{
			ID: "cp-2", ParentCheckpointID: "cp-bogus", CoveredThroughSeq: 1, SourceHeadSeq: 1,
			ConfigHash: "cfg-hash-1", ActiveRequestMessageID: "msg-r1", ResumeAfterSeq: 1,
			Summary: []session.ContentBlock{text("s")}, FormatVersion: 1,
		},
	}
	if _, err := svc.PublishCheckpoint(ctx, token(), bad); err == nil {
		t.Fatal("checkpoint with a bogus parent was published")
	} else if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("parent conflict kind = %v, want FailedPrecondition", got)
	}

	// A config hash that does not match the frozen configuration is refused.
	bad.Guard.MutationID, bad.Guard.MutationHash = "cp-3", "cph-3"
	bad.Checkpoint.ID, bad.Checkpoint.ParentCheckpointID = "cp-3", "cp-1"
	bad.Checkpoint.ConfigHash = "wrong"
	if _, err := svc.PublishCheckpoint(ctx, token(), bad); err == nil {
		t.Fatal("checkpoint with a mismatched config hash was published")
	} else if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("config mismatch kind = %v, want FailedPrecondition", got)
	}

	// Replaying the same publication returns the stored result.
	replay, err := svc.PublishCheckpoint(ctx, token(), session.PublishParams{
		SessionID: sess.ID,
		Guard:     session.MutationGuard{MutationID: "cp-1", MutationHash: "cph-1", LeaseGeneration: lease.Lease.Generation},
		Checkpoint: session.Checkpoint{
			ID: "cp-1", CoveredThroughSeq: 1, SourceHeadSeq: 1, ConfigHash: "cfg-hash-1",
			ActiveRequestMessageID: "msg-r1", ResumeAfterSeq: 1,
			Summary: []session.ContentBlock{text("the user said hi")}, FormatVersion: 1,
		},
	})
	if err != nil {
		t.Fatalf("checkpoint replay: %v", err)
	}
	if !replay.Deduplicated || replay.Session.ActiveCheckpointID != "cp-1" {
		t.Errorf("checkpoint replay = deduplicated %v active %q", replay.Deduplicated, replay.Session.ActiveCheckpointID)
	}
}

func TestDurablePaginationWindows(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	lease := acquire(t, svc, sess.ID, "msg-r1", "runtime-a")
	for i := 0; i < 5; i++ {
		if _, err := svc.AppendMessages(ctx, token(), session.AppendParams{
			SessionID: sess.ID, RequestMessageID: "msg-r1",
			Guard:    session.MutationGuard{MutationID: "m-" + string(rune('a'+i)), MutationHash: "h", LeaseGeneration: lease.Lease.Generation},
			Messages: []session.MessageDraft{draft("msg-"+string(rune('a'+i)), session.RoleAssistant, text("chunk"))},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	latest, hasMore, revision, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 3)
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if !hasMore || len(latest) != 3 || latest[0].Seq != 4 || latest[2].Seq != 6 {
		t.Fatalf("latest window = %v hasMore %v, want seq 4..6 and hasMore", latest, hasMore)
	}
	if revision == 0 {
		t.Error("window revision is zero")
	}

	older, hasMore, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, int64(latest[0].Seq), 3)
	if err != nil {
		t.Fatalf("GetMessages(before): %v", err)
	}
	if hasMore || len(older) != 3 || older[0].Seq != 1 || older[2].Seq != 3 {
		t.Fatalf("older window = %v hasMore %v, want seq 1..3 and no more", older, hasMore)
	}
}

func TestDurableAuthorizationAndRedaction(t *testing.T) {
	svc, st, sess := newFixture(t)
	ctx := context.Background()

	// Seed a legacy v1 config turn (with an api_key) so the owner-path redaction
	// is exercised through the v2 read too.
	if _, err := st.AppendMessages(ctx, sess.ID, []session.Message{
		{Role: "config", ContentJSON: `{"llm":{"api_key":"sk-platform-secret","model":"m"}}`},
	}); err != nil {
		t.Fatalf("seed legacy turn: %v", err)
	}

	if _, _, _, err := svc.GetMessages(ctx, sess.ID, userB, nil, 0, 0); err == nil {
		t.Fatal("cross-user read succeeded")
	} else if got := kindOf(t, err); got != session.KindPermissionDenied {
		t.Fatalf("cross-user read kind = %v, want PermissionDenied", got)
	}

	owner, _, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 0)
	if err != nil {
		t.Fatalf("owner read: %v", err)
	}
	if len(owner) != 1 || contains(owner[0].ContentJSON, "sk-platform-secret") {
		t.Errorf("owner read leaked the api_key: %s", owner[0].ContentJSON)
	}

	runtimeView, _, _, err := svc.GetMessages(ctx, sess.ID, "", token(), 0, 0)
	if err != nil {
		t.Fatalf("token read: %v", err)
	}
	if !contains(runtimeView[0].ContentJSON, "sk-platform-secret") {
		t.Errorf("token path lost the api_key: %s", runtimeView[0].ContentJSON)
	}

	// Internal operations reject a bare user id with no token.
	if _, err := svc.GetExecutionContext(ctx, sess.ID, nil); err == nil {
		t.Fatal("GetExecutionContext without a service token succeeded")
	} else if got := kindOf(t, err); got != session.KindUnauthenticated {
		t.Fatalf("context read kind = %v, want Unauthenticated", got)
	}
}

func TestDurableEndAndDelete(t *testing.T) {
	svc, _, sess := newFixture(t)
	ctx := context.Background()

	begin(t, svc, sess.ID, "msg-r1", "hash-1")
	ended, err := svc.EndSession(ctx, sess.ID, userA)
	if err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if ended.Status != "ended" || ended.EndedAt == "" {
		t.Errorf("ended session = %+v", ended)
	}

	// Ending is idempotent.
	if again, err := svc.EndSession(ctx, sess.ID, userA); err != nil || again.Status != "ended" {
		t.Errorf("second EndSession = %+v, %v", again, err)
	}

	// An ended session refuses new requests.
	if _, err := svc.BeginRequest(ctx, token(), session.BeginRequestParams{
		SessionID: sess.ID, UserID: userA, RequestMessageID: "msg-r2",
		ClientRequestID: "req-2", RequestHash: "h2",
		Content: []session.ContentBlock{text("after end")}, FormatVersion: 1,
	}); err == nil {
		t.Fatal("ended session admitted a request")
	} else if got := kindOf(t, err); got != session.KindFailedPrecondition {
		t.Fatalf("ended-session admission kind = %v, want FailedPrecondition", got)
	}

	// History stays readable after the end.
	if msgs, _, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 0); err != nil || len(msgs) != 1 {
		t.Errorf("history after end = %d messages, %v", len(msgs), err)
	}

	// Deletion removes the session and its transcript.
	if err := svc.DeleteSession(ctx, sess.ID, userA); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := svc.GetSession(ctx, sess.ID, userA); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("GetSession after delete = %v, want ErrNotFound", err)
	}
	if _, _, _, err := svc.GetMessages(ctx, sess.ID, userA, nil, 0, 0); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("GetMessages after delete = %v, want ErrNotFound", err)
	}

	// A non-owner cannot delete.
	if _, _, err := svc.CreateSession(ctx, userB, "", testConfig()); err != nil {
		t.Fatalf("create second session: %v", err)
	}
	if err := svc.DeleteSession(ctx, sess.ID, userB); !errors.Is(err, session.ErrNotFound) {
		t.Errorf("cross-user delete = %v, want ErrNotFound", err)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
