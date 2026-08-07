// Package test holds integration tests for session-manager. Unlike the unit
// tests next to the code (which use bufconn), these assemble the whole
// service exactly like cmd/server/main.go does — postgrest.Client →
// session.Service → transport/grpc server — and drive its public
// session.v1.SessionService API over a real TCP connection. The only fake is
// at the true external boundary: an httptest server standing in for the
// Supabase PostgREST HTTP API.
package test

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

	v1 "github.com/Duke-ECE/protos/gen/go/session/v1"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/postgrest"
	"github.com/Duke-ECE/session-manager/internal/session"
	transportgrpc "github.com/Duke-ECE/session-manager/internal/transport/grpc"
)

const testServiceToken = "integration-service-token"

// newSessionServiceClient wires the full service like cmd/server/main.go and
// returns a client connected to it over a real TCP loopback listener.
func newSessionServiceClient(t *testing.T) v1.SessionServiceClient {
	t.Helper()

	// External boundary: fake Supabase PostgREST over real HTTP.
	supabase := httptest.NewServer(newFakeSupabase())
	t.Cleanup(supabase.Close)

	// Same assembly as main(): real PostgREST client → domain service →
	// gRPC transport.
	store := postgrest.NewClient(supabase.URL, "test-service-key", nil)
	svc := session.NewService(store, testServiceToken)
	srv := transportgrpc.NewServer(svc)

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

	return v1.NewSessionServiceClient(conn)
}

func tokenCtx(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-service-token", token)
}

func mustCreateSession(t *testing.T, c v1.SessionServiceClient, userID string) *v1.Session {
	t.Helper()
	resp, err := c.CreateSession(context.Background(), &v1.CreateSessionRequest{
		UserId:   userID,
		LlmModel: "gpt-4o-mini",
	})
	if err != nil {
		t.Fatalf("CreateSession(%q): %v", userID, err)
	}
	return resp.GetSession()
}

func mustAppendTurn(t *testing.T, c v1.SessionServiceClient, sessionID string, msgs ...*v1.TurnMessage) {
	t.Helper()
	_, err := c.AppendTurn(tokenCtx(context.Background(), testServiceToken), &v1.AppendTurnRequest{
		SessionId: sessionID,
		Messages:  msgs,
	})
	if err != nil {
		t.Fatalf("AppendTurn(%q): %v", sessionID, err)
	}
}

// TestSessionLifecycle drives the full public API end-to-end: create →
// append turns → read transcript with each identity class → end (twice).
func TestSessionLifecycle(t *testing.T) {
	c := newSessionServiceClient(t)
	ctx := context.Background()

	// CreateSession: server-generated id, owner, defaults filled in.
	sess := mustCreateSession(t, c, "user-a")
	if matched, _ := regexp.MatchString(`^sess-[0-9a-f]{16}$`, sess.GetId()); !matched {
		t.Errorf("id = %q, want sess-<16 hex chars>", sess.GetId())
	}
	if sess.GetUserId() != "user-a" {
		t.Errorf("user_id = %q, want user-a", sess.GetUserId())
	}
	if sess.GetStatus() != "active" {
		t.Errorf("status = %q, want active", sess.GetStatus())
	}
	if _, err := time.Parse(time.RFC3339, sess.GetCreatedAt()); err != nil {
		t.Errorf("created_at not RFC 3339: %v", err)
	}
	if sess.GetEndedAt() != "" {
		t.Errorf("ended_at = %q, want empty", sess.GetEndedAt())
	}

	// AppendTurn without a service token is rejected.
	_, err := c.AppendTurn(ctx, &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"hi"}`}},
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("AppendTurn no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	// AppendTurn against an unknown session is NotFound, even with a token.
	_, err = c.AppendTurn(tokenCtx(ctx, testServiceToken), &v1.AppendTurnRequest{
		SessionId: "sess-nope",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"hi"}`}},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("AppendTurn unknown session: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}

	// Two valid turns: user+assistant, then a tool result.
	mustAppendTurn(t, c, sess.GetId(),
		&v1.TurnMessage{Role: "user", ContentJson: `{"text":"hi"}`},
		&v1.TurnMessage{Role: "assistant", ContentJson: `{"text":"hello"}`},
	)
	mustAppendTurn(t, c, sess.GetId(),
		&v1.TurnMessage{Role: "tool_result", ContentJson: `{"exit_code":0}`},
	)

	// The owner reads the transcript without any token: ordered by seq,
	// content round-trips verbatim.
	transcript, err := c.GetTranscript(ctx, &v1.GetTranscriptRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetTranscript as owner: %v", err)
	}
	msgs := transcript.GetMessages()
	want := []struct {
		seq     int32
		role    string
		content string
	}{
		{1, "user", `{"text":"hi"}`},
		{2, "assistant", `{"text":"hello"}`},
		{3, "tool_result", `{"exit_code":0}`},
	}
	if len(msgs) != len(want) {
		t.Fatalf("transcript = %d messages, want %d", len(msgs), len(want))
	}
	for i, w := range want {
		if msgs[i].GetSeq() != w.seq || msgs[i].GetRole() != w.role || msgs[i].GetContentJson() != w.content {
			t.Errorf("message %d = (%d, %q, %q), want (%d, %q, %q)",
				i, msgs[i].GetSeq(), msgs[i].GetRole(), msgs[i].GetContentJson(), w.seq, w.role, w.content)
		}
	}

	// A different authenticated user is denied.
	_, err = c.GetTranscript(ctx, &v1.GetTranscriptRequest{SessionId: sess.GetId(), UserId: "user-b"})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("GetTranscript as other user: code = %v, want PermissionDenied (err=%v)", status.Code(err), err)
	}

	// No user identity and no token at all is unauthenticated.
	_, err = c.GetTranscript(ctx, &v1.GetTranscriptRequest{SessionId: sess.GetId()})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("GetTranscript no identity: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	// EndSession marks it ended; a second EndSession is an idempotent no-op.
	if _, err := c.EndSession(ctx, &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	got, err := c.GetSession(ctx, &v1.GetSessionRequest{SessionId: sess.GetId(), UserId: "user-a"})
	if err != nil {
		t.Fatalf("GetSession after end: %v", err)
	}
	if got.GetSession().GetStatus() != "ended" {
		t.Errorf("status = %q, want ended", got.GetSession().GetStatus())
	}
	if _, err := time.Parse(time.RFC3339, got.GetSession().GetEndedAt()); err != nil {
		t.Errorf("ended_at not RFC 3339: %v", err)
	}
	if _, err := c.EndSession(ctx, &v1.EndSessionRequest{SessionId: sess.GetId(), UserId: "user-a"}); err != nil {
		t.Errorf("second EndSession should be idempotent: %v", err)
	}

	// Appending to an ended session is FailedPrecondition, even with a valid
	// service token: ended is terminal.
	_, err = c.AppendTurn(tokenCtx(ctx, testServiceToken), &v1.AppendTurnRequest{
		SessionId: sess.GetId(),
		UserId:    "user-a",
		Messages:  []*v1.TurnMessage{{Role: "user", ContentJson: `{"text":"late"}`}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("AppendTurn ended: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

// TestListSessionsPerUser verifies per-user filtering and most-recently-
// active-first ordering through the whole stack.
func TestListSessionsPerUser(t *testing.T) {
	c := newSessionServiceClient(t)
	ctx := context.Background()

	first := mustCreateSession(t, c, "user-a")
	second := mustCreateSession(t, c, "user-a")
	mustCreateSession(t, c, "user-b")

	// Activity on the first session makes it the most recently active.
	mustAppendTurn(t, c, first.GetId(), &v1.TurnMessage{Role: "user", ContentJson: `{"text":"ping"}`})

	resp, err := c.ListSessions(ctx, &v1.ListSessionsRequest{UserId: "user-a"})
	if err != nil {
		t.Fatalf("ListSessions(user-a): %v", err)
	}
	got := resp.GetSessions()
	if len(got) != 2 {
		t.Fatalf("user-a sessions = %d, want 2", len(got))
	}
	for _, s := range got {
		if s.GetUserId() != "user-a" {
			t.Errorf("listed session owned by %q", s.GetUserId())
		}
	}
	// last_active ties (same-second timestamps) keep insertion order, and a
	// later append only moves the first session further ahead — either way
	// the appended session sorts first.
	if got[0].GetId() != first.GetId() || got[1].GetId() != second.GetId() {
		t.Errorf("order = [%q %q], want [%q %q] (most recently active first)",
			got[0].GetId(), got[1].GetId(), first.GetId(), second.GetId())
	}

	resp, err = c.ListSessions(ctx, &v1.ListSessionsRequest{UserId: "user-b"})
	if err != nil {
		t.Fatalf("ListSessions(user-b): %v", err)
	}
	if len(resp.GetSessions()) != 1 {
		t.Fatalf("user-b sessions = %d, want 1", len(resp.GetSessions()))
	}
}

// TestSetTitle drives title writes through the whole stack: service-token
// auth, the title round-tripping via GetSession/ListSessions, last_active
// staying untouched (titling is not activity), and ended sessions staying
// settable.
func TestSetTitle(t *testing.T) {
	c := newSessionServiceClient(t)
	ctx := context.Background()

	first := mustCreateSession(t, c, "user-a")
	second := mustCreateSession(t, c, "user-a")
	if first.GetTitle() != "" || second.GetTitle() != "" {
		t.Errorf("new sessions should have empty titles, got %q / %q", first.GetTitle(), second.GetTitle())
	}

	// Make the first session the most recently active.
	mustAppendTurn(t, c, first.GetId(), &v1.TurnMessage{Role: "user", ContentJson: `{"text":"ping"}`})

	// SetTitle without a service token is rejected.
	_, err := c.SetTitle(ctx, &v1.SetTitleRequest{SessionId: second.GetId(), Title: "second chat"})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("SetTitle no token: code = %v, want Unauthenticated (err=%v)", status.Code(err), err)
	}

	// Empty titles are rejected even with a token.
	_, err = c.SetTitle(tokenCtx(ctx, testServiceToken), &v1.SetTitleRequest{SessionId: second.GetId(), Title: "  "})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("SetTitle empty title: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	// A valid set returns the updated session and persists.
	resp, err := c.SetTitle(tokenCtx(ctx, testServiceToken), &v1.SetTitleRequest{SessionId: second.GetId(), Title: "second chat"})
	if err != nil {
		t.Fatalf("SetTitle: %v", err)
	}
	if resp.GetSession().GetTitle() != "second chat" {
		t.Errorf("response title = %q, want %q", resp.GetSession().GetTitle(), "second chat")
	}

	list, err := c.ListSessions(ctx, &v1.ListSessionsRequest{UserId: "user-a"})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	got := list.GetSessions()
	if len(got) != 2 {
		t.Fatalf("sessions = %d, want 2", len(got))
	}
	// Titling the second session must not bump its last_active: the first
	// session still sorts first.
	if got[0].GetId() != first.GetId() || got[1].GetId() != second.GetId() {
		t.Errorf("order = [%q %q], want [%q %q] (SetTitle must not touch last_active)",
			got[0].GetId(), got[1].GetId(), first.GetId(), second.GetId())
	}
	if got[1].GetTitle() != "second chat" {
		t.Errorf("listed title = %q, want %q", got[1].GetTitle(), "second chat")
	}

	// Titles stay settable on ended sessions (metadata, not transcript).
	if _, err := c.EndSession(ctx, &v1.EndSessionRequest{SessionId: first.GetId(), UserId: "user-a"}); err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	resp, err = c.SetTitle(tokenCtx(ctx, testServiceToken), &v1.SetTitleRequest{SessionId: first.GetId(), Title: "first chat"})
	if err != nil {
		t.Fatalf("SetTitle on ended session: %v", err)
	}
	if resp.GetSession().GetTitle() != "first chat" || resp.GetSession().GetStatus() != "ended" {
		t.Errorf("got (title=%q, status=%q), want (first chat, ended)",
			resp.GetSession().GetTitle(), resp.GetSession().GetStatus())
	}
}

// fakeSupabase is a minimal in-memory PostgREST stand-in: it understands
// eq-filters, select projections, order, limit, and Prefer:
// return=representation for the two tables session-manager uses.
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
		// Database defaults.
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
		rows = applyOrder(rows, r.URL.Query().Get("order"))
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
