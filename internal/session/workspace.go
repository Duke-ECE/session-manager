package session

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// WorkspaceRevision identifies the latest committed, immutable R2 archive.
// Revision zero means the session has no saved files yet.
type WorkspaceRevision struct {
	SessionID   string `json:"session_id"`
	Revision    int64  `json:"revision"`
	ObjectKey   string `json:"object_key"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	CommittedAt string `json:"committed_at"`
}

type CommitWorkspaceParams struct {
	SessionID        string `json:"p_session_id"`
	ExpectedRevision int64  `json:"p_expected_revision"`
	ObjectKey        string `json:"p_object_key"`
	SHA256           string `json:"p_sha256"`
	SizeBytes        int64  `json:"p_size_bytes"`
	MutationID       string `json:"p_mutation_id"`
}

type CommitWorkspaceResult struct {
	Workspace    WorkspaceRevision `json:"workspace"`
	Deduplicated bool              `json:"deduplicated"`
}

type WorkspaceStore interface {
	EnsureWorkspace(context.Context, string) error
	GetWorkspace(context.Context, string) (WorkspaceRevision, error)
	CommitWorkspace(context.Context, CommitWorkspaceParams) (CommitWorkspaceResult, error)
	ListWorkspacesForCleanup(context.Context, int32, int32) ([]WorkspaceCleanupTarget, bool, error)
	FinishWorkspaceDeletion(context.Context, string) error
	GetSession(context.Context, string) (Session, error)
}

type WorkspaceCleanupTarget struct {
	SessionID           string `json:"session_id"`
	CurrentObjectKey    string `json:"object_key"`
	DeletionRequestedAt string `json:"deletion_requested_at"`
}

type WorkspaceService struct {
	store        WorkspaceStore
	serviceToken string
}

func NewWorkspaceService(store WorkspaceStore, serviceToken string) *WorkspaceService {
	return &WorkspaceService{store: store, serviceToken: serviceToken}
}

func (s *WorkspaceService) tokenOK(tokens []string) bool {
	return presentedTokenOK(s.serviceToken, tokens)
}

func (s *WorkspaceService) Get(ctx context.Context, sessionID, userID string, tokens []string) (WorkspaceRevision, error) {
	if sessionID == "" {
		return WorkspaceRevision{}, invalidArgument("session_id is required")
	}
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return WorkspaceRevision{}, err
	}
	if userID != "" {
		if sess.UserID != userID {
			return WorkspaceRevision{}, permissionDenied("session belongs to another user")
		}
	} else if !s.tokenOK(tokens) {
		return WorkspaceRevision{}, unauthenticated("owner user_id or valid x-service-token metadata is required")
	} else {
		if err := s.store.EnsureWorkspace(ctx, sessionID); err != nil {
			return WorkspaceRevision{}, err
		}
	}
	return s.store.GetWorkspace(ctx, sessionID)
}

var workspaceSHA256 = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (s *WorkspaceService) Commit(ctx context.Context, tokens []string, p CommitWorkspaceParams) (CommitWorkspaceResult, error) {
	if !s.tokenOK(tokens) {
		return CommitWorkspaceResult{}, unauthenticated("valid x-service-token metadata is required")
	}
	prefix := "sessions/" + p.SessionID + "/revisions/"
	if p.SessionID == "" || strings.Contains(p.SessionID, "/") || p.ExpectedRevision < 0 || p.MutationID == "" ||
		!strings.HasPrefix(p.ObjectKey, prefix) || !strings.HasSuffix(p.ObjectKey, ".tar.gz") ||
		strings.Contains(p.ObjectKey, "..") || !workspaceSHA256.MatchString(p.SHA256) || p.SizeBytes <= 0 || p.SizeBytes > 1<<30 {
		return CommitWorkspaceResult{}, invalidArgument("invalid workspace revision")
	}
	result, err := s.store.CommitWorkspace(ctx, p)
	if err != nil {
		return CommitWorkspaceResult{}, fmt.Errorf("commit workspace: %w", err)
	}
	return result, nil
}

func (s *WorkspaceService) ListForCleanup(ctx context.Context, tokens []string, limit, offset int32) ([]WorkspaceCleanupTarget, bool, error) {
	if !s.tokenOK(tokens) {
		return nil, false, unauthenticated("valid x-service-token metadata is required")
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if offset < 0 {
		return nil, false, invalidArgument("offset must be nonnegative")
	}
	return s.store.ListWorkspacesForCleanup(ctx, limit, offset)
}

func (s *WorkspaceService) FinishDeletion(ctx context.Context, tokens []string, sessionID string) error {
	if !s.tokenOK(tokens) {
		return unauthenticated("valid x-service-token metadata is required")
	}
	if sessionID == "" {
		return invalidArgument("session_id is required")
	}
	return s.store.FinishWorkspaceDeletion(ctx, sessionID)
}
