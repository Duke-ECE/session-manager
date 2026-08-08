package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/postgrest"
	"github.com/Duke-ECE/session-manager/internal/session"
)

const testToken = "test-service-token"

// newTestClient serves the real gRPC handlers over an in-memory bufconn
// listener, backed by the real PostgREST store pointed at a fake Supabase
// httptest server. This exercises the full path — proto contract, privilege
// checks, PostgREST query building — without a database.
func newTestClient(t *testing.T, serviceToken string) v1.SessionServiceClient {
	t.Helper()
	c, _ := newTestClientWithFake(t, serviceToken)
	return c
}

// newTestClientWithFake additionally returns the fake Supabase so tests can
// inspect raw rows (e.g. that deletes really removed messages).
func newTestClientWithFake(t *testing.T, serviceToken string) (v1.SessionServiceClient, *fakeSupabase) {
	t.Helper()

	fake := newFakeSupabase()
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	lis := bufconn.Listen(1024 * 1024)
	s := NewServer(session.NewService(postgrest.NewClient(ts.URL, "test-key", nil), serviceToken))
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

	return v1.NewSessionServiceClient(conn), fake
}

func tokenCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-service-token", token)
}

func mustCreate(t *testing.T, c v1.SessionServiceClient, userID string) *v1.Session {
	t.Helper()
	resp, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		UserId:   userID,
		LlmModel: "gpt-4o-mini",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return resp.GetSession()
}

func TestCreateSession(t *testing.T) {
	c := newTestClient(t, testToken)

	sess := mustCreate(t, c, "user-a")
	if matched, _ := regexp.MatchString(`^sess-[0-9a-f]{16}$`, sess.GetId()); !matched {
		t.Errorf("id = %q, want sess-<16 hex chars>", sess.GetId())
	}
	if sess.GetStatus() != "active" {
		t.Errorf("status = %q, want active", sess.GetStatus())
	}
	if sess.GetUserId() != "user-a" || sess.GetLlmModel() != "gpt-4o-mini" {
		t.Errorf("unexpected session: %v", sess)
	}
	if _, err := time.Parse(time.RFC3339, sess.GetCreatedAt()); err != nil {
		t.Errorf("created_at not RFC 3339: %v", err)
	}
	if sess.GetEndedAt() != "" {
		t.Errorf("ended_at = %q, want empty", sess.GetEndedAt())
	}
}

func TestCreateSessionEmptyUser(t *testing.T) {
	c := newTestClient(t, testToken)

	_, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestCreateSessionAgentID(t *testing.T) {
	c := newTestClient(t, testToken)

	// With an agent id: it persists and round-trips through
	// GetSession/ListSessions.
	resp, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		UserId:   "user-a",
		LlmModel: "gpt-4o-mini",
		AgentId:  "agent-coder",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sess := resp.GetSession()
	if sess.GetAgentId() != "agent-coder" {
		t.Errorf("created agent_id = %q, want %q", sess.GetAgentId(), "agent-coder")
	}

	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.GetSession().GetAgentId() != "agent-coder" {
		t.Errorf("got agent_id = %q, want %q", got.GetSession().GetAgentId(), "agent-coder")
	}

	// Without an agent id: still works, exposed as empty string.
	plain := mustCreate(t, c, "user-a")
	if plain.GetAgentId() != "" {
		t.Errorf("agentless session agent_id = %q, want empty", plain.GetAgentId())
	}

	list, err := c.ListSessions(context.Background(), &v1.ListSessionsRequest{UserId: "user-a"})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	byID := map[string]string{}
	for _, s := range list.GetSessions() {
		byID[s.GetId()] = s.GetAgentId()
	}
	if byID[sess.GetId()] != "agent-coder" || byID[plain.GetId()] != "" {
		t.Errorf("listed agent_ids = %v, want %q and empty", byID, "agent-coder")
	}
}

func TestGetSessionOwnership(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetSession as owner: %v", err)
	}
	if got.GetSession().GetId() != sess.GetId() {
		t.Errorf("got session %q, want %q", got.GetSession().GetId(), sess.GetId())
	}

	_, err = c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("non-owner: code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}

	_, err = c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId()})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty user: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	_, err = c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: "sess-nope", UserId: "user-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("missing: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestListSessionsPerUser(t *testing.T) {
	c := newTestClient(t, testToken)
	mustCreate(t, c, "user-a")
	mustCreate(t, c, "user-a")
	mustCreate(t, c, "user-b")

	resp, err := c.ListSessions(context.Background(), &v1.ListSessionsRequest{UserId: "user-a"})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.GetSessions()) != 2 {
		t.Fatalf("user-a sessions = %d, want 2", len(resp.GetSessions()))
	}
	for _, s := range resp.GetSessions() {
		if s.GetUserId() != "user-a" {
			t.Errorf("listed session owned by %q", s.GetUserId())
		}
	}

	_, err = c.ListSessions(context.Background(), &v1.ListSessionsRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty user: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestEndSession(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	if _, err := c.EndSession(context.Background(), &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.GetSession().GetStatus() != "ended" {
		t.Errorf("status = %q, want ended", got.GetSession().GetStatus())
	}
	if _, err := time.Parse(time.RFC3339, got.GetSession().GetEndedAt()); err != nil {
		t.Errorf("ended_at not RFC 3339: %v", err)
	}
}

func TestEndSessionNotOwner(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	_, err := c.EndSession(context.Background(), &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}

	_, err = c.EndSession(context.Background(), &v1.EndSessionRequest{SessionId: "sess-nope", UserId: "user-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("missing: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestAppendTurnRequiresToken(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	req := &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"hi"}`}},
	}

	_, err := c.AppendTurn(context.Background(), req)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	_, err = c.AppendTurn(tokenCtx(context.Background(), "wrong"), req)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	if _, err := c.AppendTurn(tokenCtx(context.Background(), testToken), req); err != nil {
		t.Errorf("valid token: %v", err)
	}
}

func TestAppendTurnUnknownSession(t *testing.T) {
	c := newTestClient(t, testToken)

	_, err := c.AppendTurn(tokenCtx(context.Background(), testToken), &v1.AppendTurnRequest{
		SessionId: "sess-nope",
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{}`}},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestAppendTurnEndedSession(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	ctx := tokenCtx(context.Background(), testToken)

	if _, err := c.AppendTurn(ctx, &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"hi"}`}},
	}); err != nil {
		t.Fatalf("AppendTurn active: %v", err)
	}
	if _, err := c.EndSession(context.Background(), &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	_, err := c.AppendTurn(ctx, &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"late"}`}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ended: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	// The transcript is untouched: still exactly the one pre-end turn.
	got, err := c.GetTranscript(ctx, &v1.GetTranscriptRequest{SessionId: sess.GetId()})
	if err != nil {
		t.Fatalf("GetTranscript: %v", err)
	}
	if len(got.GetMessages()) != 1 {
		t.Errorf("transcript len = %d, want 1 (ended append must not persist)", len(got.GetMessages()))
	}
}

func TestSetTitleRequiresToken(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	req := &v1.SetTitleRequest{SessionId: sess.GetId(), Title: "my chat"}

	_, err := c.SetTitle(context.Background(), req)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	_, err = c.SetTitle(tokenCtx(context.Background(), "wrong"), req)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	resp, err := c.SetTitle(tokenCtx(context.Background(), testToken), req)
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if resp.GetSession().GetTitle() != "my chat" {
		t.Errorf("response title = %q, want %q", resp.GetSession().GetTitle(), "my chat")
	}
	got, err := c.GetSession(context.Background(), &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got.GetSession().GetTitle() != "my chat" {
		t.Errorf("stored title = %q, want %q", got.GetSession().GetTitle(), "my chat")
	}
}

func TestSetTitleEmptyRejected(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	ctx := tokenCtx(context.Background(), testToken)

	for _, title := range []string{"", "   "} {
		_, err := c.SetTitle(ctx, &v1.SetTitleRequest{SessionId: sess.GetId(), Title: title})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("title %q: code = %v, want InvalidArgument (err=%v)", title, status.Code(err), err)
		}
	}

	// Surrounding whitespace is trimmed, not rejected.
	resp, err := c.SetTitle(ctx, &v1.SetTitleRequest{SessionId: sess.GetId(), Title: "  hi  "})
	if err != nil {
		t.Fatalf("padded title: %v", err)
	}
	if resp.GetSession().GetTitle() != "hi" {
		t.Errorf("title = %q, want %q (trimmed)", resp.GetSession().GetTitle(), "hi")
	}
}

func TestSetTitleUnknownSession(t *testing.T) {
	c := newTestClient(t, testToken)

	_, err := c.SetTitle(tokenCtx(context.Background(), testToken), &v1.SetTitleRequest{
		SessionId: "sess-nope",
		Title:     "my chat",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestSetTitleEndedSession(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	if _, err := c.EndSession(context.Background(), &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	// A title is metadata, not transcript: settable on ended sessions.
	resp, err := c.SetTitle(tokenCtx(context.Background(), testToken), &v1.SetTitleRequest{
		SessionId: sess.GetId(),
		Title:     "retrospective",
	})
	if err != nil {
		t.Fatalf("SetTitle on ended session: %v", err)
	}
	if resp.GetSession().GetTitle() != "retrospective" {
		t.Errorf("title = %q, want %q", resp.GetSession().GetTitle(), "retrospective")
	}
	if resp.GetSession().GetStatus() != "ended" {
		t.Errorf("status = %q, want ended (untouched)", resp.GetSession().GetStatus())
	}
}

func TestSetTitleTruncation(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	// 130 runes, multibyte: truncates to the 120-rune cap without splitting
	// a character.
	long := strings.Repeat("é", 130)
	resp, err := c.SetTitle(tokenCtx(context.Background(), testToken), &v1.SetTitleRequest{
		SessionId: sess.GetId(),
		Title:     long,
	})
	if err != nil {
		t.Fatalf("SetTitle: %v", err)
	}
	want := strings.Repeat("é", 120)
	if resp.GetSession().GetTitle() != want {
		t.Errorf("title len = %d runes, want 120", len([]rune(resp.GetSession().GetTitle())))
	}
}

func TestAppendGetRoundTrip(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	ctx := tokenCtx(context.Background(), testToken)

	append := func(msgs ...*v1.TurnMessage) {
		t.Helper()
		if _, err := c.AppendTurn(ctx, &v1.AppendTurnRequest{SessionId: sess.GetId(), UserId: "user-a", Messages: msgs}); err != nil {
			t.Fatalf("AppendTurn: %v", err)
		}
	}
	append(
		&v1.TurnMessage{Role: "user", ContentJson: `{"text":"hi"}`},
		&v1.TurnMessage{Role: "assistant", ContentJson: `{"text":"hello"}`},
	)
	append(&v1.TurnMessage{Role: "tool_result", ContentJson: `{"exit_code":0}`})

	// Owner reads the transcript without any token.
	resp, err := c.GetTranscript(context.Background(), &v1.GetTranscriptRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetTranscript: %v", err)
	}
	msgs := resp.GetMessages()
	if len(msgs) != 3 {
		t.Fatalf("transcript = %d messages, want 3", len(msgs))
	}
	for i, want := range []struct {
		seq     int32
		role    string
		content string
	}{
		{1, "user", `{"text":"hi"}`},
		{2, "assistant", `{"text":"hello"}`},
		{3, "tool_result", `{"exit_code":0}`},
	} {
		if msgs[i].GetSeq() != want.seq || msgs[i].GetRole() != want.role || msgs[i].GetContentJson() != want.content {
			t.Errorf("message %d = (%d, %q, %q), want (%d, %q, %q)",
				i, msgs[i].GetSeq(), msgs[i].GetRole(), msgs[i].GetContentJson(), want.seq, want.role, want.content)
		}
		if _, err := time.Parse(time.RFC3339, msgs[i].GetCreatedAt()); err != nil {
			t.Errorf("message %d created_at not RFC 3339: %v", i, err)
		}
	}
}

func TestGetTranscriptNonOwner(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	_, err := c.GetTranscript(context.Background(), &v1.GetTranscriptRequest{SessionId: sess.GetId(), UserId: "user-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("non-owner: code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}

	_, err = c.GetTranscript(context.Background(), &v1.GetTranscriptRequest{SessionId: sess.GetId()})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("no identity no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	// Runtime hydration: service token substitutes for ownership.
	resp, err := c.GetTranscript(tokenCtx(context.Background(), testToken), &v1.GetTranscriptRequest{SessionId: sess.GetId()})
	if err != nil {
		t.Errorf("service token: %v", err)
	}
	if len(resp.GetMessages()) != 0 {
		t.Errorf("expected empty transcript, got %d messages", len(resp.GetMessages()))
	}

	_, err = c.GetTranscript(context.Background(), &v1.GetTranscriptRequest{SessionId: "sess-nope", UserId: "user-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("missing: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestListSessionsPagination(t *testing.T) {
	c := newTestClient(t, testToken)
	var ids []string
	for range 7 {
		ids = append(ids, mustCreate(t, c, "user-a").GetId())
	}
	mustCreate(t, c, "user-b") // never listed

	page := func(limit, offset int32) *v1.ListSessionsResponse {
		t.Helper()
		resp, err := c.ListSessions(context.Background(), &v1.ListSessionsRequest{UserId: "user-a", Limit: limit, Offset: offset})
		if err != nil {
			t.Fatalf("ListSessions(limit=%d, offset=%d): %v", limit, offset, err)
		}
		return resp
	}

	// Walk the pages: 3 + 3 + 1, has_more true until the last page.
	var walked []string
	p1 := page(3, 0)
	if len(p1.GetSessions()) != 3 || !p1.GetHasMore() {
		t.Errorf("page 1: %d sessions, has_more=%t; want 3, true", len(p1.GetSessions()), p1.GetHasMore())
	}
	p2 := page(3, 3)
	if len(p2.GetSessions()) != 3 || !p2.GetHasMore() {
		t.Errorf("page 2: %d sessions, has_more=%t; want 3, true", len(p2.GetSessions()), p2.GetHasMore())
	}
	p3 := page(3, 6)
	if len(p3.GetSessions()) != 1 || p3.GetHasMore() {
		t.Errorf("page 3: %d sessions, has_more=%t; want 1, false", len(p3.GetSessions()), p3.GetHasMore())
	}
	for _, p := range []*v1.ListSessionsResponse{p1, p2, p3} {
		for _, s := range p.GetSessions() {
			walked = append(walked, s.GetId())
		}
	}
	// Same-second last_active ties keep creation order; the walk covers all
	// seven exactly once, in order.
	if strings.Join(walked, ",") != strings.Join(ids, ",") {
		t.Errorf("walked order = %v, want creation order %v", walked, ids)
	}

	// An offset past the end returns an empty page, not an error.
	if resp := page(3, 100); len(resp.GetSessions()) != 0 || resp.GetHasMore() {
		t.Errorf("offset past end: %d sessions, has_more=%t; want 0, false", len(resp.GetSessions()), resp.GetHasMore())
	}
}

func TestListSessionsDefaultAndCap(t *testing.T) {
	c := newTestClient(t, testToken)
	for range 205 {
		mustCreate(t, c, "user-a")
	}
	list := func(limit, offset int32) *v1.ListSessionsResponse {
		t.Helper()
		resp, err := c.ListSessions(context.Background(), &v1.ListSessionsRequest{UserId: "user-a", Limit: limit, Offset: offset})
		if err != nil {
			t.Fatalf("ListSessions(limit=%d, offset=%d): %v", limit, offset, err)
		}
		return resp
	}

	// limit 0 = server default 50.
	if resp := list(0, 0); len(resp.GetSessions()) != 50 || !resp.GetHasMore() {
		t.Errorf("default page: %d sessions, has_more=%t; want 50, true", len(resp.GetSessions()), resp.GetHasMore())
	}
	// Limit is capped at 200.
	if resp := list(1000, 0); len(resp.GetSessions()) != 200 || !resp.GetHasMore() {
		t.Errorf("capped page: %d sessions, has_more=%t; want 200, true", len(resp.GetSessions()), resp.GetHasMore())
	}
	// The remaining 5 sit behind the cap.
	if resp := list(1000, 200); len(resp.GetSessions()) != 5 || resp.GetHasMore() {
		t.Errorf("tail page: %d sessions, has_more=%t; want 5, false", len(resp.GetSessions()), resp.GetHasMore())
	}
}

func TestGetTranscriptWindows(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")
	ctx := context.Background()

	msgs := make([]*v1.TurnMessage, 0, 10)
	for i := 1; i <= 10; i++ {
		msgs = append(msgs, &v1.TurnMessage{Role: "user", ContentJson: fmt.Sprintf(`{"n":%d}`, i)})
	}
	if _, err := c.AppendTurn(tokenCtx(ctx, testToken), &v1.AppendTurnRequest{SessionId: sess.GetId(), UserId: "user-a", Messages: msgs}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}

	window := func(beforeSeq, limit int32) ([]int32, bool) {
		t.Helper()
		resp, err := c.GetTranscript(ctx, &v1.GetTranscriptRequest{
			SessionId: sess.GetId(), UserId: "user-a", BeforeSeq: beforeSeq, Limit: limit,
		})
		if err != nil {
			t.Fatalf("GetTranscript(before_seq=%d, limit=%d): %v", beforeSeq, limit, err)
		}
		var seqs []int32
		for _, m := range resp.GetMessages() {
			seqs = append(seqs, m.GetSeq())
		}
		return seqs, resp.GetHasMore()
	}
	wantSeqs := func(got []int32, want ...int32) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("seqs = %v, want %v", got, want)
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("seqs = %v, want %v (ascending)", got, want)
				return
			}
		}
	}

	// Walk backwards through the transcript in windows of 4: the latest
	// window, then the ones before it, ascending inside each window.
	seqs, hasMore := window(0, 4)
	wantSeqs(seqs, 7, 8, 9, 10)
	if !hasMore {
		t.Errorf("latest window: has_more = false, want true (older messages exist)")
	}
	seqs, hasMore = window(7, 4)
	wantSeqs(seqs, 3, 4, 5, 6)
	if !hasMore {
		t.Errorf("middle window: has_more = false, want true")
	}
	seqs, hasMore = window(3, 4)
	wantSeqs(seqs, 1, 2)
	if hasMore {
		t.Errorf("oldest window: has_more = true, want false (start of transcript)")
	}

	// limit = 0: legacy full transcript, has_more always false.
	seqs, hasMore = window(0, 0)
	wantSeqs(seqs, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if hasMore {
		t.Errorf("full transcript: has_more = true, want false")
	}

	// Limit is capped at 1000 — far above this transcript, so still full.
	seqs, hasMore = window(0, 5000)
	wantSeqs(seqs, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if hasMore {
		t.Errorf("capped window: has_more = true, want false")
	}
}

func TestDeleteSession(t *testing.T) {
	c, fake := newTestClientWithFake(t, testToken)
	ctx := context.Background()
	sess := mustCreate(t, c, "user-a")
	if _, err := c.AppendTurn(tokenCtx(ctx, testToken), &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"hi"}`}},
	}); err != nil {
		t.Fatalf("AppendTurn: %v", err)
	}

	// A foreign user cannot delete it.
	_, err := c.DeleteSession(ctx, &v1.DeleteSessionRequest{SessionId: sess.GetId(), UserId: "user-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("non-owner: code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}
	// A missing session is NotFound.
	_, err = c.DeleteSession(ctx, &v1.DeleteSessionRequest{SessionId: "sess-nope", UserId: "user-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("missing: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}

	// The owner deletes an active session: row and all messages are gone.
	if _, err := c.DeleteSession(ctx, &v1.DeleteSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := c.GetSession(ctx, &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetSession after delete: code = %v, want NotFound", status.Code(err))
	}
	if got := fake.messageCount(sess.GetId()); got != 0 {
		t.Errorf("messages left after delete = %d, want 0", got)
	}
	// Deleting again is NotFound.
	if _, err := c.DeleteSession(ctx, &v1.DeleteSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); status.Code(err) != codes.NotFound {
		t.Errorf("second delete: code = %v, want NotFound", status.Code(err))
	}

	// Deletion is allowed in any status — an ended session deletes the same.
	ended := mustCreate(t, c, "user-a")
	if _, err := c.EndSession(ctx, &v1.EndSessionRequest{SessionId: ended.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if _, err := c.DeleteSession(ctx, &v1.DeleteSessionRequest{SessionId: ended.GetId(), UserId: "user-a"}); err != nil {
		t.Errorf("DeleteSession on ended session: %v", err)
	}
}

func TestSetTitleOwnerPath(t *testing.T) {
	c := newTestClient(t, testToken)
	sess := mustCreate(t, c, "user-a")

	// The owner sets the title with just user_id — no token needed.
	resp, err := c.SetTitle(context.Background(), &v1.SetTitleRequest{
		SessionId: sess.GetId(), Title: "owner rename", UserId: "user-a",
	})
	if err != nil {
		t.Fatalf("SetTitle as owner: %v", err)
	}
	if resp.GetSession().GetTitle() != "owner rename" {
		t.Errorf("title = %q, want %q", resp.GetSession().GetTitle(), "owner rename")
	}

	// A non-empty non-owner user_id is PermissionDenied — with or without a
	// valid token, mirroring GetTranscript's owner-or-token pattern.
	for _, c2 := range []context.Context{context.Background(), tokenCtx(context.Background(), testToken)} {
		_, err := c.SetTitle(c2, &v1.SetTitleRequest{SessionId: sess.GetId(), Title: "hijack", UserId: "user-b"})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("non-owner: code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
		}
	}

	// The service-token path (runtime auto-titles) still works.
	resp, err = c.SetTitle(tokenCtx(context.Background(), testToken), &v1.SetTitleRequest{
		SessionId: sess.GetId(), Title: "auto title",
	})
	if err != nil {
		t.Fatalf("SetTitle with token: %v", err)
	}
	if resp.GetSession().GetTitle() != "auto title" {
		t.Errorf("title = %q, want %q", resp.GetSession().GetTitle(), "auto title")
	}

	// Neither owner nor token is Unauthenticated.
	_, err = c.SetTitle(context.Background(), &v1.SetTitleRequest{SessionId: sess.GetId(), Title: "anon"})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("no identity no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}
}

// fakeSupabase is a minimal in-memory PostgREST stand-in: it understands
// eq/lt filters, select projections, order, offset, limit, deletes, and
// return=representation for the two tables session-manager uses.
type fakeSupabase struct {
	mu       sync.Mutex
	sessions []map[string]any
	messages []map[string]any
}

func newFakeSupabase() *fakeSupabase {
	return &fakeSupabase{}
}

// messageCount reports how many message rows remain for sessionID.
func (f *fakeSupabase) messageCount(sessionID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, row := range f.messages {
		if row["session_id"] == sessionID {
			n++
		}
	}
	return n
}

func (f *fakeSupabase) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case "/rest/v1/agent_sessions":
		f.handleSessions(w, r)
	case "/rest/v1/agent_messages":
		f.handleMessages(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeSupabase) handleSessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var row map[string]any
		if err := json.NewDecoder(r.Body).Decode(&row); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		row["status"] = "active"
		row["created_at"] = now
		row["last_active"] = now
		row["ended_at"] = nil
		f.sessions = append(f.sessions, row)
		writeJSON(w, []map[string]any{row})
	case http.MethodGet:
		rows := f.sessions
		rows = filterEq(rows, "id", r.URL.Query().Get("id"))
		rows = filterEq(rows, "user_id", r.URL.Query().Get("user_id"))
		rows = filterEq(rows, "status", r.URL.Query().Get("status"))
		rows = filterLt(rows, "ended_at", r.URL.Query().Get("ended_at"))
		rows = applyOrder(rows, r.URL.Query().Get("order"))
		rows = applyOffset(rows, r.URL.Query().Get("offset"))
		writeJSON(w, applyLimit(rows, r.URL.Query().Get("limit")))
	case http.MethodPatch:
		var patch map[string]any
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var updated []map[string]any
		id, _ := strings.CutPrefix(r.URL.Query().Get("id"), "eq.")
		for _, row := range f.sessions {
			if row["id"] == id {
				for k, v := range patch {
					row[k] = v
				}
				updated = append(updated, row)
			}
		}
		writeJSON(w, updated)
	case http.MethodDelete:
		var deleted []map[string]any
		id, _ := strings.CutPrefix(r.URL.Query().Get("id"), "eq.")
		kept := f.sessions[:0]
		for _, row := range f.sessions {
			if row["id"] == id {
				deleted = append(deleted, row)
			} else {
				kept = append(kept, row)
			}
		}
		f.sessions = kept
		writeJSON(w, deleted)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *fakeSupabase) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		for _, row := range rows {
			row["created_at"] = now
			f.messages = append(f.messages, row)
		}
		writeJSON(w, rows)
	case http.MethodGet:
		rows := filterEq(f.messages, "session_id", r.URL.Query().Get("session_id"))
		rows = filterLt(rows, "seq", r.URL.Query().Get("seq"))
		rows = applyOrder(rows, r.URL.Query().Get("order"))
		rows = applyOffset(rows, r.URL.Query().Get("offset"))
		rows = applyLimit(rows, r.URL.Query().Get("limit"))
		writeJSON(w, applySelect(rows, r.URL.Query().Get("select")))
	case http.MethodDelete:
		sessionID, _ := strings.CutPrefix(r.URL.Query().Get("session_id"), "eq.")
		kept := f.messages[:0]
		for _, row := range f.messages {
			if row["session_id"] != sessionID {
				kept = append(kept, row)
			}
		}
		f.messages = kept
		writeJSON(w, []map[string]any{})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// filterEq keeps rows whose column matches an "eq.<value>" filter param.
func filterEq(rows []map[string]any, col, param string) []map[string]any {
	val, ok := strings.CutPrefix(param, "eq.")
	if !ok {
		return rows
	}
	var out []map[string]any
	for _, row := range rows {
		if row[col] == val {
			out = append(out, row)
		}
	}
	return out
}

// filterLt keeps rows whose column is below an "lt.<value>" filter param,
// comparing JSON numbers numerically and anything else as strings (RFC 3339
// timestamps sort chronologically).
func filterLt(rows []map[string]any, col, param string) []map[string]any {
	val, ok := strings.CutPrefix(param, "lt.")
	if !ok {
		return rows
	}
	var num float64
	isNum := false
	if n, err := strconv.ParseFloat(val, 64); err == nil {
		num, isNum = n, true
	}
	var out []map[string]any
	for _, row := range rows {
		if isNum {
			if fv, ok := row[col].(float64); ok && fv < num {
				out = append(out, row)
			}
		} else if s, ok := row[col].(string); ok && s < val {
			out = append(out, row)
		}
	}
	return out
}

// applySelect projects only the comma-separated columns, like PostgREST.
func applySelect(rows []map[string]any, sel string) []map[string]any {
	if sel == "" {
		return rows
	}
	cols := strings.Split(sel, ",")
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		projected := map[string]any{}
		for _, c := range cols {
			if v, ok := row[c]; ok {
				projected[c] = v
			}
		}
		out = append(out, projected)
	}
	return out
}

// applyOrder sorts by "col.asc"/"col.desc", comparing JSON numbers
// numerically (seq) and anything else as strings (RFC 3339 timestamps sort
// chronologically).
func applyOrder(rows []map[string]any, order string) []map[string]any {
	col, dir, _ := strings.Cut(order, ".")
	if col == "" {
		return rows
	}
	out := append([]map[string]any(nil), rows...)
	sort.SliceStable(out, func(i, j int) bool {
		less := compareValues(out[i][col], out[j][col]) < 0
		if dir == "desc" {
			return compareValues(out[i][col], out[j][col]) > 0
		}
		return less
	})
	return out
}

func compareValues(a, b any) int {
	if fa, ok := a.(float64); ok {
		if fb, ok := b.(float64); ok {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			}
			return 0
		}
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// applyOffset drops the first n rows, like PostgREST's offset param.
func applyOffset(rows []map[string]any, offset string) []map[string]any {
	n, err := strconv.Atoi(offset)
	if err != nil || n <= 0 {
		return rows
	}
	if n >= len(rows) {
		return []map[string]any{}
	}
	return rows[n:]
}

func applyLimit(rows []map[string]any, limit string) []map[string]any {
	n, err := strconv.Atoi(limit)
	if err != nil || n >= len(rows) {
		return rows
	}
	return rows[:n]
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
