package postgrest

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Duke-ECE/session-manager/internal/session"
)

var _ session.WorkspaceStore = (*Client)(nil)

func (c *Client) EnsureWorkspace(ctx context.Context, sessionID string) error {
	return mapError(c.do(ctx, http.MethodPost, "/rest/v1/rpc/ensure_session_workspace", nil,
		map[string]string{"p_session_id": sessionID}, "", nil))
}

func (c *Client) GetWorkspace(ctx context.Context, sessionID string) (session.WorkspaceRevision, error) {
	query := url.Values{"session_id": {"eq." + sessionID}, "select": {"session_id,revision,object_key,sha256,size_bytes,committed_at"}, "limit": {"1"}}
	var rows []session.WorkspaceRevision
	if err := c.do(ctx, http.MethodGet, "/rest/v1/session_workspaces", query, nil, "", &rows); err != nil {
		return session.WorkspaceRevision{}, mapError(err)
	}
	if len(rows) == 0 {
		return session.WorkspaceRevision{SessionID: sessionID}, nil
	}
	return rows[0], nil
}

func (c *Client) ListWorkspacesForCleanup(ctx context.Context, limit, offset int32) ([]session.WorkspaceCleanupTarget, bool, error) {
	query := url.Values{
		"select": {"session_id,object_key,deletion_requested_at"},
		"order":  {"session_id.asc"},
		"limit":  {strconv.Itoa(int(limit + 1))},
		"offset": {strconv.Itoa(int(offset))},
	}
	var rows []session.WorkspaceCleanupTarget
	if err := c.do(ctx, http.MethodGet, "/rest/v1/session_workspaces", query, nil, "", &rows); err != nil {
		return nil, false, mapError(err)
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

func (c *Client) FinishWorkspaceDeletion(ctx context.Context, sessionID string) error {
	query := url.Values{"session_id": {"eq." + sessionID}, "deletion_requested_at": {"not.is.null"}, "select": {"session_id"}}
	var rows []struct {
		SessionID string `json:"session_id"`
	}
	if err := c.do(ctx, http.MethodDelete, "/rest/v1/session_workspaces", query, nil, "return=representation", &rows); err != nil {
		return mapError(err)
	}
	if len(rows) == 0 {
		return session.ErrNotFound
	}
	return nil
}

func (c *Client) CommitWorkspace(ctx context.Context, p session.CommitWorkspaceParams) (session.CommitWorkspaceResult, error) {
	var result session.CommitWorkspaceResult
	if err := c.do(ctx, http.MethodPost, "/rest/v1/rpc/commit_session_workspace", nil, p, "", &result); err != nil {
		return session.CommitWorkspaceResult{}, mapError(err)
	}
	return result, nil
}
