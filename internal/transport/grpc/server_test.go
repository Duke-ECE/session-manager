package grpc

import (
	"context"
	"encoding/json"
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

	return v1.NewSessionServiceClient(conn)
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

// fakeSupabase is a minimal in-memory PostgREST stand-in: it understands
// eq-filters, select projections, order, limit, and return=representation
// for the two tables session-manager uses.
type fakeSupabase struct {
	mu       sync.Mutex
	sessions []map[string]any
	messages []map[string]any
}

func newFakeSupabase() *fakeSupabase {
	return &fakeSupabase{}
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
		rows = applyOrder(rows, r.URL.Query().Get("order"))
		rows = applyLimit(rows, r.URL.Query().Get("limit"))
		writeJSON(w, applySelect(rows, r.URL.Query().Get("select")))
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

// applyOrder sorts by "col.asc"/"col.desc"; seq is the only column ordered
// on in practice.
func applyOrder(rows []map[string]any, order string) []map[string]any {
	col, dir, _ := strings.Cut(order, ".")
	if col == "" {
		return rows
	}
	out := append([]map[string]any(nil), rows...)
	sort.SliceStable(out, func(i, j int) bool {
		less := asInt(out[i][col]) < asInt(out[j][col])
		if dir == "desc" {
			return !less
		}
		return less
	})
	return out
}

func applyLimit(rows []map[string]any, limit string) []map[string]any {
	n, err := strconv.Atoi(limit)
	if err != nil || n >= len(rows) {
		return rows
	}
	return rows[:n]
}

// asInt reads a JSON-decoded number (float64) as an int for comparisons.
func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
