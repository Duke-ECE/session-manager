package grpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/session-manager/internal/session"
)

type WorkspaceHandler struct {
	v2.UnimplementedWorkspaceServiceServer
	svc *session.WorkspaceService
}

func NewWorkspaceHandler(svc *session.WorkspaceService) *WorkspaceHandler {
	return &WorkspaceHandler{svc: svc}
}

func workspaceToProto(ws session.WorkspaceRevision) *v2.WorkspaceRevision {
	out := &v2.WorkspaceRevision{
		SessionId: ws.SessionID, Revision: ws.Revision, ObjectKey: ws.ObjectKey,
		Sha256: ws.SHA256, SizeBytes: ws.SizeBytes,
	}
	if ws.CommittedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, ws.CommittedAt); err == nil {
			out.CommittedAt = timestamppb.New(t)
		}
	}
	return out
}

func (h *WorkspaceHandler) GetWorkspace(ctx context.Context, req *v2.GetWorkspaceRequest) (*v2.GetWorkspaceResponse, error) {
	ws, err := h.svc.Get(ctx, req.GetSessionId(), req.GetUserId(), presentedTokens(ctx))
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.GetWorkspaceResponse{Workspace: workspaceToProto(ws)}, nil
}

func (h *WorkspaceHandler) CommitWorkspace(ctx context.Context, req *v2.CommitWorkspaceRequest) (*v2.CommitWorkspaceResponse, error) {
	res, err := h.svc.Commit(ctx, presentedTokens(ctx), session.CommitWorkspaceParams{
		SessionID: req.GetSessionId(), ExpectedRevision: req.GetExpectedRevision(),
		ObjectKey: req.GetObjectKey(), SHA256: req.GetSha256(),
		SizeBytes: req.GetSizeBytes(), MutationID: req.GetMutationId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.CommitWorkspaceResponse{Workspace: workspaceToProto(res.Workspace), Deduplicated: res.Deduplicated}, nil
}

func (h *WorkspaceHandler) ListWorkspacesForCleanup(ctx context.Context, req *v2.ListWorkspacesForCleanupRequest) (*v2.ListWorkspacesForCleanupResponse, error) {
	targets, hasMore, err := h.svc.ListForCleanup(ctx, presentedTokens(ctx), req.GetLimit(), req.GetOffset())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &v2.ListWorkspacesForCleanupResponse{HasMore: hasMore}
	for _, target := range targets {
		item := &v2.WorkspaceCleanupTarget{SessionId: target.SessionID, CurrentObjectKey: target.CurrentObjectKey}
		if target.DeletionRequestedAt != "" {
			if when, err := time.Parse(time.RFC3339Nano, target.DeletionRequestedAt); err == nil {
				item.DeletionRequestedAt = timestamppb.New(when)
			}
		}
		out.Workspaces = append(out.Workspaces, item)
	}
	return out, nil
}

func (h *WorkspaceHandler) FinishWorkspaceDeletion(ctx context.Context, req *v2.FinishWorkspaceDeletionRequest) (*v2.FinishWorkspaceDeletionResponse, error) {
	if err := h.svc.FinishDeletion(ctx, presentedTokens(ctx), req.GetSessionId()); err != nil {
		return nil, toStatus(err)
	}
	return &v2.FinishWorkspaceDeletionResponse{}, nil
}
