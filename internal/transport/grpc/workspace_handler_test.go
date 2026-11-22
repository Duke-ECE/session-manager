package grpc

import (
	"context"
	"strings"
	"testing"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/infrastructure/memory"
	"github.com/Duke-ECE/session-manager/internal/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestWorkspaceHandlerRequiresTokenAndKeepsDeletionIntent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	if _, err := store.CreateSession(ctx, "sess-one", "owner", "", ""); err != nil {
		t.Fatal(err)
	}
	h := NewWorkspaceHandler(session.NewWorkspaceService(store, "secret"))
	req := &v2.CommitWorkspaceRequest{
		SessionId: "sess-one", ObjectKey: "sessions/sess-one/revisions/one.tar.gz",
		Sha256: strings.Repeat("a", 64), SizeBytes: 100, MutationId: "mutation-one",
	}
	if _, err := h.CommitWorkspace(ctx, req); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	authed := metadata.NewIncomingContext(ctx, metadata.Pairs("x-service-token", "secret"))
	if _, err := h.CommitWorkspace(authed, req); err != nil {
		t.Fatal(err)
	}
	if _, err := h.GetWorkspace(ctx, &v2.GetWorkspaceRequest{SessionId: "sess-one", UserId: "other"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("other owner: %v", err)
	}
	got, err := h.GetWorkspace(ctx, &v2.GetWorkspaceRequest{SessionId: "sess-one", UserId: "owner"})
	if err != nil || got.GetWorkspace().GetRevision() != 1 {
		t.Fatalf("owner read = %v, %v", got, err)
	}
	if err := store.DeleteSession(ctx, "sess-one"); err != nil {
		t.Fatal(err)
	}
	list, err := h.ListWorkspacesForCleanup(authed, &v2.ListWorkspacesForCleanupRequest{})
	if err != nil || len(list.GetWorkspaces()) != 1 || list.GetWorkspaces()[0].GetDeletionRequestedAt() == nil {
		t.Fatalf("deletion intent = %v, %v", list, err)
	}
	if _, err := h.FinishWorkspaceDeletion(authed, &v2.FinishWorkspaceDeletionRequest{SessionId: "sess-one"}); err != nil {
		t.Fatal(err)
	}
	list, err = h.ListWorkspacesForCleanup(authed, &v2.ListWorkspacesForCleanupRequest{})
	if err != nil || len(list.GetWorkspaces()) != 0 {
		t.Fatalf("cleanup completion = %v, %v", list, err)
	}
}
