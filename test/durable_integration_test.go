package test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/session"
	transportgrpc "github.com/Duke-ECE/session-manager/internal/transport/grpc"
)

// newDurableServiceClient assembles the whole service exactly like
// cmd/server/main.go's zero-dependency mode (in-memory store → v1 + durable
// services → gRPC transport) and returns a v2 client over a real TCP loopback
// listener plus the store, so lease expiry can be driven from the test.
func newDurableServiceClient(t *testing.T) (v2.SessionServiceClient, *memory.Store) {
	t.Helper()

	st := memory.New()
	svcV1 := session.NewService(st, testServiceToken)
	svcV2 := session.NewDurableService(st, testServiceToken)
	srv := transportgrpc.NewServer(svcV1, svcV2)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", lis.Addr(), err)
	}
	t.Cleanup(func() { conn.Close() })

	return v2.NewSessionServiceClient(conn), st
}

func integrationCtx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-service-token", testServiceToken)
}

func textBlock(s string) *v2.ContentBlock {
	return &v2.ContentBlock{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: s}}}
}

func TestDurableIntegrationHappyPathAndTakeover(t *testing.T) {
	client, st := newDurableServiceClient(t)
	ctx := integrationCtx()
	const user = "11111111-1111-1111-1111-111111111111"

	created, err := client.CreateSession(ctx, &v2.CreateSessionRequest{
		UserId: user,
		Config: &v2.SessionConfig{
			FormatVersion: 1, Provider: "openai", Model: "gpt-4o-mini",
			BaseUrl: "https://llm.example.com/v1", ContextWindowTokens: 128000, OutputLimitTokens: 4096,
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := created.GetSession().GetId()

	// The frozen configuration hash is resolved once at creation; the runtime
	// binds each checkpoint to it.
	stateBefore, err := client.GetExecutionContext(ctx, &v2.GetExecutionContextRequest{SessionId: sid})
	if err != nil {
		t.Fatalf("GetExecutionContext: %v", err)
	}
	cfgHash := stateBefore.GetConfig().GetConfigHash()
	if cfgHash == "" {
		t.Fatal("session configuration has no hash")
	}

	begun, err := client.BeginRequest(ctx, &v2.BeginRequestRequest{
		SessionId: sid, UserId: user, ClientRequestId: "req-1", RequestHash: "hash-1",
		FormatVersion: 1, Content: []*v2.ContentBlock{textBlock("run pwd")},
	})
	if err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	reqID := begun.GetRequestMessage().GetId()

	// Reconnect: the same client request id returns the same root.
	again, err := client.BeginRequest(ctx, &v2.BeginRequestRequest{
		SessionId: sid, UserId: user, ClientRequestId: "req-1", RequestHash: "hash-1",
		FormatVersion: 1, Content: []*v2.ContentBlock{textBlock("run pwd")},
	})
	if err != nil {
		t.Fatalf("BeginRequest (reconnect): %v", err)
	}
	if !again.GetDeduplicated() || again.GetRequestMessage().GetId() != reqID {
		t.Fatalf("reconnect = deduplicated %v id %q, want true/%q", again.GetDeduplicated(), again.GetRequestMessage().GetId(), reqID)
	}

	leaseA, err := client.AcquireLease(ctx, &v2.AcquireLeaseRequest{SessionId: sid, RequestMessageId: reqID, Owner: "runtime-a", TtlSeconds: 90})
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	gen := leaseA.GetLease().GetGeneration()

	// Persist the assistant tool-call before dispatching the tool, then the
	// observed result with its explicit link.
	if _, err := client.AppendMessages(ctx, &v2.AppendMessagesRequest{
		SessionId: sid, RequestMessageId: reqID,
		Guard: &v2.MutationGuard{MutationId: "m-1", MutationHash: "mh-1", LeaseGeneration: gen},
		Messages: []*v2.MessageDraft{{
			Id: "msg-a1", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1,
			Status: v2.MessageStatus_MESSAGE_STATUS_COMPLETE,
			Content: []*v2.ContentBlock{
				textBlock("running pwd"),
				{Value: &v2.ContentBlock_ToolCall{ToolCall: &v2.ToolCallBlock{Id: "call_1", Name: "bash", ArgumentsJson: `{"command":"pwd"}`}}},
			},
		}},
	}); err != nil {
		t.Fatalf("append tool call: %v", err)
	}
	if _, err := client.AppendMessages(ctx, &v2.AppendMessagesRequest{
		SessionId: sid, RequestMessageId: reqID,
		Guard: &v2.MutationGuard{MutationId: "m-2", MutationHash: "mh-2", LeaseGeneration: gen},
		Messages: []*v2.MessageDraft{{
			Id: "msg-t1", Role: v2.MessageRole_MESSAGE_ROLE_TOOL, FormatVersion: 1,
			Status:           v2.MessageStatus_MESSAGE_STATUS_COMPLETE,
			ReplyToMessageId: "msg-a1",
			Content: []*v2.ContentBlock{{Value: &v2.ContentBlock_ToolResult{ToolResult: &v2.ToolResultBlock{
				ToolCallId: "call_1", Status: v2.ToolResultStatus_TOOL_RESULT_STATUS_SUCCESS,
				Content: []*v2.ContentBlock{textBlock("/workspace")},
			}}}},
		}},
	}); err != nil {
		t.Fatalf("append tool result: %v", err)
	}

	// Mid-request compaction: publish a checkpoint and keep going in the same
	// request. The original messages stay untouched.
	published, err := client.PublishCheckpoint(ctx, &v2.PublishCheckpointRequest{
		SessionId: sid,
		Guard:     &v2.MutationGuard{MutationId: "cp-1", MutationHash: "cph-1", LeaseGeneration: gen},
		Checkpoint: &v2.ContextCheckpoint{
			Id: "cp-1", CoveredThroughSeq: 3, SourceHeadSeq: 3, SourceRevision: 3,
			ActiveRequestMessageId: reqID, ResumeAfterSeq: 3, FormatVersion: 1, ConfigHash: cfgHash,
			Summary: []*v2.ContentBlock{textBlock("the user asked to run pwd; it printed /workspace")},
		},
	})
	if err != nil {
		t.Fatalf("PublishCheckpoint: %v", err)
	}
	if published.GetSession().GetActiveCheckpointId() != "cp-1" {
		t.Fatalf("active checkpoint = %q", published.GetSession().GetActiveCheckpointId())
	}

	finished, err := client.FinishRequest(ctx, &v2.FinishRequestRequest{
		SessionId: sid, RequestMessageId: reqID,
		Guard:  &v2.MutationGuard{MutationId: "m-3", MutationHash: "mh-3", LeaseGeneration: gen},
		Status: v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
		Messages: []*v2.MessageDraft{{
			Id: "msg-a2", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1,
			Status:  v2.MessageStatus_MESSAGE_STATUS_COMPLETE,
			Content: []*v2.ContentBlock{textBlock("done")},
			Usage:   &v2.TokenUsage{InputTokens: 42, OutputTokens: 7},
		}},
	})
	if err != nil {
		t.Fatalf("FinishRequest: %v", err)
	}
	if finished.GetRequestMessage().GetRequest().GetStatus() != v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("root status = %v", finished.GetRequestMessage().GetRequest().GetStatus())
	}

	// History is the original messages, in canonical order, with the compaction
	// indicator as metadata rather than an invented assistant reply.
	history, err := client.GetMessages(ctx, &v2.GetMessagesRequest{SessionId: sid, UserId: user})
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(history.GetMessages()) != 4 {
		t.Fatalf("history = %d messages, want 4", len(history.GetMessages()))
	}
	if history.GetMessages()[2].GetRole() != v2.MessageRole_MESSAGE_ROLE_TOOL {
		t.Errorf("message 3 role = %v, want tool", history.GetMessages()[2].GetRole())
	}
	if got := history.GetMessages()[3].GetUsage().GetInputTokens(); got != 42 {
		t.Errorf("persisted usage input_tokens = %d, want 42", got)
	}

	// A second request: the first replica's lease is gone, but if a runtime dies
	// mid-request another replica takes over with a bumped generation.
	begun2, err := client.BeginRequest(ctx, &v2.BeginRequestRequest{
		SessionId: sid, UserId: user, ClientRequestId: "req-2", RequestHash: "hash-2",
		FormatVersion: 1, Content: []*v2.ContentBlock{textBlock("long task")},
	})
	if err != nil {
		t.Fatalf("BeginRequest (second): %v", err)
	}
	req2 := begun2.GetRequestMessage().GetId()
	dead, err := client.AcquireLease(ctx, &v2.AcquireLeaseRequest{SessionId: sid, RequestMessageId: req2, Owner: "runtime-a", TtlSeconds: 90})
	if err != nil {
		t.Fatalf("AcquireLease (runtime-a): %v", err)
	}

	// Kill the clock forward so the lease lapses, then take over.
	base := time.Now().Add(10 * time.Minute)
	st.SetClock(func() time.Time { return base })
	takeover, err := client.AcquireLease(ctx, &v2.AcquireLeaseRequest{SessionId: sid, RequestMessageId: req2, Owner: "runtime-b", TtlSeconds: 90})
	if err != nil {
		t.Fatalf("AcquireLease (takeover): %v", err)
	}
	if takeover.GetLease().GetGeneration() != dead.GetLease().GetGeneration()+1 {
		t.Fatalf("takeover generation = %d, want %d", takeover.GetLease().GetGeneration(), dead.GetLease().GetGeneration()+1)
	}

	// The fenced-out replica's write is rejected.
	if _, err := client.AppendMessages(ctx, &v2.AppendMessagesRequest{
		SessionId: sid, RequestMessageId: req2,
		Guard:    &v2.MutationGuard{MutationId: "m-stale", MutationHash: "sh", LeaseGeneration: dead.GetLease().GetGeneration()},
		Messages: []*v2.MessageDraft{{Id: "msg-stale", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1, Status: v2.MessageStatus_MESSAGE_STATUS_COMPLETE, Content: []*v2.ContentBlock{textBlock("x")}}},
	}); err == nil {
		t.Fatal("fenced-out replica wrote successfully")
	}

	// The owner cancels; the live runtime keeps the lease and finishes with the
	// partial output it holds.
	cancelled, err := client.CancelRequest(ctx, &v2.CancelRequestRequest{SessionId: sid, UserId: user, RequestMessageId: req2})
	if err != nil {
		t.Fatalf("CancelRequest: %v", err)
	}
	if !cancelled.GetRequestMessage().GetRequest().GetCancellationRequested() {
		t.Fatal("cancellation was not flagged")
	}
	final, err := client.FinishRequest(ctx, &v2.FinishRequestRequest{
		SessionId: sid, RequestMessageId: req2,
		Guard:  &v2.MutationGuard{MutationId: "m-cancel", MutationHash: "mch", LeaseGeneration: takeover.GetLease().GetGeneration()},
		Status: v2.ExecutionStatus_EXECUTION_STATUS_CANCELLED,
		Messages: []*v2.MessageDraft{{
			Id: "msg-partial", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1,
			Status: v2.MessageStatus_MESSAGE_STATUS_PARTIAL, Content: []*v2.ContentBlock{textBlock("half a sen")},
		}},
		ErrorCode: "cancelled_by_user",
	})
	if err != nil {
		t.Fatalf("FinishRequest (cancelled): %v", err)
	}
	if final.GetMessages()[0].GetStatus() != v2.MessageStatus_MESSAGE_STATUS_PARTIAL {
		t.Errorf("partial status = %v", final.GetMessages()[0].GetStatus())
	}

	// Ending and deleting remain owner-scoped and leave no history behind.
	if _, err := client.EndSession(ctx, &v2.EndSessionRequest{SessionId: sid, UserId: user}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if _, err := client.DeleteSession(ctx, &v2.DeleteSessionRequest{SessionId: sid, UserId: user}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := client.GetMessages(ctx, &v2.GetMessagesRequest{SessionId: sid, UserId: user}); err == nil {
		t.Fatal("history survived deletion")
	}
}
