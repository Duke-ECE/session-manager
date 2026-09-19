package grpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/session"
)

// newDurableTestClient assembles the v2 service over the in-memory store behind
// a real gRPC connection, exactly as main() does in memory mode.
func newDurableTestClient(t *testing.T) v2.SessionServiceClient {
	t.Helper()

	st := memory.New()
	lis := bufconn.Listen(1024 * 1024)
	s := NewServer(session.NewService(st, "tok"), session.NewDurableService(st, "tok"))
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return v2.NewSessionServiceClient(conn)
}

func serviceCtx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-service-token", "tok")
}

func TestDurableGRPCRoundTrip(t *testing.T) {
	client := newDurableTestClient(t)
	ctx := serviceCtx()
	const user = "11111111-1111-1111-1111-111111111111"

	created, err := client.CreateSession(ctx, &v2.CreateSessionRequest{
		UserId:  user,
		AgentId: "agent-1",
		Config: &v2.SessionConfig{
			FormatVersion:       1,
			SystemPrompt:        "test",
			Provider:            "openai",
			Model:               "gpt-4o-mini",
			BaseUrl:             "https://llm.example.com/v1",
			CredentialRef:       "cred-1",
			ContextWindowTokens: 128000,
			OutputLimitTokens:   4096,
			Tools:               []*v2.ToolDefinition{{Name: "bash", Version: "1"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := created.GetSession().GetId()
	if sid == "" || created.GetSession().GetStatus() != v2.SessionStatus_SESSION_STATUS_ACTIVE {
		t.Fatalf("created session = %+v", created.GetSession())
	}

	begun, err := client.BeginRequest(ctx, &v2.BeginRequestRequest{
		SessionId:       sid,
		UserId:          user,
		ClientRequestId: "req-1",
		RequestHash:     "hash-1",
		FormatVersion:   1,
		Content:         []*v2.ContentBlock{{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: "hi"}}}},
	})
	if err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	reqID := begun.GetRequestMessage().GetId()
	if reqID == "" || begun.GetRequestMessage().GetRequest().GetStatus() != v2.ExecutionStatus_EXECUTION_STATUS_QUEUED {
		t.Fatalf("request root = %+v", begun.GetRequestMessage())
	}

	lease, err := client.AcquireLease(ctx, &v2.AcquireLeaseRequest{
		SessionId: sid, RequestMessageId: reqID, Owner: "runtime-a", TtlSeconds: 90,
	})
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if lease.GetLease().GetGeneration() != 1 {
		t.Fatalf("lease generation = %d, want 1", lease.GetLease().GetGeneration())
	}

	appended, err := client.AppendMessages(ctx, &v2.AppendMessagesRequest{
		SessionId: sid, RequestMessageId: reqID,
		Guard: &v2.MutationGuard{MutationId: "m-1", MutationHash: "mh-1", LeaseGeneration: lease.GetLease().GetGeneration()},
		Messages: []*v2.MessageDraft{{
			Id: "msg-a1", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1,
			Status: v2.MessageStatus_MESSAGE_STATUS_COMPLETE,
			Content: []*v2.ContentBlock{
				{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: "running pwd"}}},
				{Value: &v2.ContentBlock_ToolCall{ToolCall: &v2.ToolCallBlock{Id: "call_1", Name: "bash", ArgumentsJson: `{"command":"pwd"}`}}},
			},
			Usage: &v2.TokenUsage{InputTokens: 10, OutputTokens: 5},
		}},
	})
	if err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	if len(appended.GetMessages()) != 1 || appended.GetMessages()[0].GetSeq() != 2 {
		t.Fatalf("appended = %+v", appended.GetMessages())
	}
	if appended.GetMessages()[0].GetUsage().GetInputTokens() != 10 {
		t.Errorf("usage round-trip = %+v", appended.GetMessages()[0].GetUsage())
	}

	finished, err := client.FinishRequest(ctx, &v2.FinishRequestRequest{
		SessionId: sid, RequestMessageId: reqID,
		Guard:  &v2.MutationGuard{MutationId: "m-2", MutationHash: "mh-2", LeaseGeneration: lease.GetLease().GetGeneration()},
		Status: v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
		Messages: []*v2.MessageDraft{{
			Id: "msg-a2", Role: v2.MessageRole_MESSAGE_ROLE_ASSISTANT, FormatVersion: 1,
			Status:  v2.MessageStatus_MESSAGE_STATUS_COMPLETE,
			Content: []*v2.ContentBlock{{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: "done"}}}},
		}},
	})
	if err != nil {
		t.Fatalf("FinishRequest: %v", err)
	}
	if finished.GetRequestMessage().GetRequest().GetStatus() != v2.ExecutionStatus_EXECUTION_STATUS_COMPLETED {
		t.Errorf("root status = %v", finished.GetRequestMessage().GetRequest().GetStatus())
	}
	if finished.GetSession().GetActiveRequestMessageId() != "" {
		t.Errorf("active request not cleared: %q", finished.GetSession().GetActiveRequestMessageId())
	}

	history, err := client.GetMessages(ctx, &v2.GetMessagesRequest{SessionId: sid, UserId: user})
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(history.GetMessages()) != 3 {
		t.Fatalf("history = %d messages, want 3", len(history.GetMessages()))
	}
	if history.GetRevision() == 0 {
		t.Error("history revision is zero")
	}
	blocks := history.GetMessages()[1].GetContent()
	if len(blocks) != 2 || blocks[1].GetToolCall().GetId() != "call_1" {
		t.Errorf("tool-call block did not round-trip: %+v", blocks)
	}

	state, err := client.GetExecutionContext(ctx, &v2.GetExecutionContextRequest{SessionId: sid})
	if err != nil {
		t.Fatalf("GetExecutionContext: %v", err)
	}
	if state.GetConfig().GetConfigHash() == "" || state.GetConfig().GetModel() != "gpt-4o-mini" {
		t.Errorf("context config = %+v", state.GetConfig())
	}
}

func TestDurableGRPCRequiresServiceToken(t *testing.T) {
	client := newDurableTestClient(t)
	// No metadata: internal operations must fail closed.
	_, err := client.BeginRequest(context.Background(), &v2.BeginRequestRequest{
		SessionId: "sess-x", UserId: "u", ClientRequestId: "r", RequestHash: "h",
		Content: []*v2.ContentBlock{{Value: &v2.ContentBlock_Text{Text: &v2.TextBlock{Text: "hi"}}}},
	})
	if err == nil {
		t.Fatal("BeginRequest without a service token succeeded")
	}
}
