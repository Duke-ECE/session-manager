package session_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/session"
)

func TestWorkspaceCommitOwnershipAndCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	if _, err := store.CreateSession(ctx, "sess-one", "owner", "", ""); err != nil {
		t.Fatal(err)
	}
	svc := session.NewWorkspaceService(store, "secret")
	if _, err := svc.Get(ctx, "sess-one", "other", nil); err == nil {
		t.Fatal("another user read workspace")
	}
	if _, err := svc.Get(ctx, "sess-one", "", nil); err == nil {
		t.Fatal("missing token read workspace")
	}
	ws, err := svc.Get(ctx, "sess-one", "owner", nil)
	if err != nil || ws.Revision != 0 {
		t.Fatalf("initial workspace = %+v, %v", ws, err)
	}
	p := session.CommitWorkspaceParams{
		SessionID: "sess-one", ExpectedRevision: 0,
		ObjectKey: "sessions/sess-one/revisions/one.tar.gz", SHA256: strings.Repeat("a", 64),
		SizeBytes: 123, MutationID: "upload-one",
	}
	if _, err := svc.Commit(ctx, nil, p); err == nil {
		t.Fatal("missing token committed workspace")
	}
	first, err := svc.Commit(ctx, []string{"secret"}, p)
	if err != nil || first.Workspace.Revision != 1 || first.Deduplicated {
		t.Fatalf("first commit = %+v, %v", first, err)
	}
	again, err := svc.Commit(ctx, []string{"secret"}, p)
	if err != nil || !again.Deduplicated || again.Workspace.ObjectKey != p.ObjectKey {
		t.Fatalf("retry = %+v, %v", again, err)
	}
	p.MutationID = "upload-two"
	if _, err := svc.Commit(ctx, []string{"secret"}, p); err == nil {
		t.Fatal("stale revision committed")
	}
	p.ExpectedRevision = 1
	p.ObjectKey = "sessions/sess-one/revisions/two.tar.gz"
	if _, err := svc.Commit(ctx, []string{"secret"}, p); err != nil {
		t.Fatal(err)
	}
	if err := store.EndSession(ctx, "sess-one", time.Now()); err != nil {
		t.Fatal(err)
	}
	p.ExpectedRevision = 2
	p.MutationID = "upload-three"
	if _, err := svc.Commit(ctx, []string{"secret"}, p); err == nil {
		t.Fatal("ended session committed")
	}
}
