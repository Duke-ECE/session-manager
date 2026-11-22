package postgrest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Duke-ECE/session-manager/internal/session"
)

func TestMapErrorClassifiesEveryReason(t *testing.T) {
	cases := []struct {
		detail       string
		code         string
		wantNotFound bool
		wantKind     session.Kind
	}{
		{detail: "not_found", code: "P0002", wantNotFound: true},
		{detail: "permission_denied", code: "P0001", wantKind: session.KindPermissionDenied},
		{detail: "invalid_argument", code: "P0001", wantKind: session.KindInvalidArgument},
		{detail: "hash_conflict", code: "P0001", wantKind: session.KindInvalidArgument},
		{detail: "stale_lease", code: "P0001", wantKind: session.KindAborted},
		{detail: "lease_expired", code: "P0001", wantKind: session.KindAborted},
		{detail: "lease_conflict", code: "P0001", wantKind: session.KindAborted},
		{detail: "revision_conflict", code: "P0001", wantKind: session.KindAborted},
		{detail: "mutation_conflict", code: "P0001", wantKind: session.KindAborted},
		{detail: "session_ended", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "request_busy", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "request_terminal", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "request_not_active", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "request_cancelled", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "parent_conflict", code: "P0001", wantKind: session.KindFailedPrecondition},
		{detail: "config_mismatch", code: "P0001", wantKind: session.KindFailedPrecondition},
		// No machine-readable detail: fall back to the SQLSTATE.
		{code: "P0002", wantNotFound: true},
		{code: "23505", wantKind: session.KindFailedPrecondition},
		{code: "23514", wantKind: session.KindInvalidArgument},
		{code: "23503", wantKind: session.KindInvalidArgument},
		{code: "22P02", wantKind: session.KindInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.detail+tc.code, func(t *testing.T) {
			err := mapError(&APIError{StatusCode: 400, Code: tc.code, Details: tc.detail, Message: "boom"})
			if tc.wantNotFound {
				if !errors.Is(err, session.ErrNotFound) {
					t.Fatalf("mapError = %v, want ErrNotFound", err)
				}
				return
			}
			var domErr *session.Error
			if !errors.As(err, &domErr) {
				t.Fatalf("mapError = %v, want a domain error", err)
			}
			if domErr.Kind != tc.wantKind {
				t.Fatalf("mapError kind = %v, want %v", domErr.Kind, tc.wantKind)
			}
		})
	}
}

// TestDurableRPCContract checks the wire contract with the Postgres functions:
// the function name, the named arguments, and the decoding of the jsonb result.
func TestDurableRPCContract(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"session": {"id":"sess-1","user_id":"u1","status":"active","last_seq":1,"revision":1,
			            "active_request_message_id":"msg-1","lease_generation":0},
			"request_message": {"id":"msg-1","session_id":"sess-1","seq":1,"request_message_id":"msg-1",
			            "role":"user","content":[{"type":"text","text":"hi"}],"format_version":1,
			            "message_status":"complete","execution_status":"queued","client_request_id":"req-1",
			            "request_hash":"hash-1","cancellation_requested":false},
			"deduplicated": false
		}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key", srv.Client())
	res, err := client.BeginRequest(context.Background(), session.BeginRequestParams{
		SessionID:        "sess-1",
		UserID:           "u1",
		RequestMessageID: "msg-1",
		ClientRequestID:  "req-1",
		RequestHash:      "hash-1",
		Content:          []session.ContentBlock{{Type: session.BlockText, Text: "hi"}},
		FormatVersion:    1,
	})
	if err != nil {
		t.Fatalf("BeginRequest: %v", err)
	}
	if gotPath != "/rest/v1/rpc/session_begin_request" {
		t.Errorf("rpc path = %q", gotPath)
	}
	for _, key := range []string{"p_session_id", "p_user_id", "p_request_message_id", "p_client_request_id", "p_request_hash", "p_content", "p_format_version"} {
		if _, ok := gotBody[key]; !ok {
			t.Errorf("rpc body missing %q: %v", key, gotBody)
		}
	}
	if res.RequestMessage.ExecutionStatus != session.ExecutionQueued || res.Session.ActiveRequestMessageID != "msg-1" {
		t.Errorf("decoded result = %+v / %+v", res.RequestMessage, res.Session)
	}
	if res.Session.Revision != 1 {
		t.Errorf("revision = %d, want 1", res.Session.Revision)
	}
}

// TestDurableRPCPropagatesDomainErrors checks that the HTTP error body becomes a
// domain error the service layer can act on.
func TestDurableRPCPropagatesDomainErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"P0001","details":"stale_lease","message":"a current lease generation is required"}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key", srv.Client())
	_, err := client.AppendMessageBatch(context.Background(), session.AppendParams{
		SessionID: "sess-1", RequestMessageID: "msg-1",
		Guard:    session.MutationGuard{MutationID: "m", MutationHash: "h", LeaseGeneration: 1},
		Messages: []session.MessageDraft{{ID: "m1", Role: session.RoleAssistant, Content: []session.ContentBlock{{Type: session.BlockText, Text: "x"}}, FormatVersion: 1, Status: session.MessageStatusComplete}},
	})
	var domErr *session.Error
	if !errors.As(err, &domErr) || domErr.Kind != session.KindAborted {
		t.Fatalf("AppendMessageBatch error = %v, want KindAborted", err)
	}
}

// TestDeleteSessionReliesOnCascade checks the client issues a single session
// delete (children cascade in Postgres and deleting them first would trip the
// session's active-request foreign key).
func TestDeleteSessionReliesOnCascade(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"sess-1"}]`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "key", srv.Client())
	if err := client.DeleteSession(context.Background(), "sess-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if len(paths) != 1 || paths[0] != "DELETE /rest/v1/agent_sessions" {
		t.Fatalf("requests = %v, want a single DELETE of agent_sessions", paths)
	}
}
