package session

import (
	"context"
	"time"
)

// DurableStore is the persistence port for the canonical-message state machine
// (session.v2). Every mutation method must be atomic: sequence allocation,
// revision advance, lease fencing, and idempotency receipts happen in one
// database transaction, never as a read-then-write sequence in the service.
// The PostgREST implementation calls the Postgres transaction functions;
// tests substitute the in-memory implementation.
type DurableStore interface {
	// CreateDurableSession inserts the session together with its immutable
	// resolved configuration. agentID is the opaque agent-template id; empty
	// means none.
	CreateDurableSession(ctx context.Context, id, userID, agentID string, cfg SessionConfig) (Session, SessionConfig, error)
	// GetSessionConfig returns the session's frozen configuration.
	GetSessionConfig(ctx context.Context, sessionID string) (SessionConfig, error)

	// BeginRequest atomically deduplicates the request, persists the initiating
	// user message as a queued root, and sets the active request reference.
	BeginRequest(ctx context.Context, p BeginRequestParams) (BeginRequestResult, error)
	// AcquireLease takes fenced ownership of the active request, moving a queued
	// request to running. A live lease held by another owner is ErrLeaseConflict.
	AcquireLease(ctx context.Context, p LeaseParams) (LeaseResult, error)
	// RenewLease extends a held lease. It does not advance the revision.
	RenewLease(ctx context.Context, p RenewLeaseParams) (Lease, error)
	// ReleaseLease clears a held lease; releasing one already taken over is a
	// no-op reported as false.
	ReleaseLease(ctx context.Context, p ReleaseLeaseParams) (bool, error)

	// AppendMessageBatch commits one idempotent, fenced batch and returns the
	// stored messages with their assigned sequence numbers.
	AppendMessageBatch(ctx context.Context, p AppendParams) (AppendResult, error)
	// FinishRequest writes the final batch, the root's terminal execution state,
	// clears the active request, and releases the matching lease, atomically.
	FinishRequest(ctx context.Context, p FinishParams) (FinishResult, error)
	// CancelRequest flags the request cancelled. A queued or lease-lapsed
	// request is terminated here; a live one keeps running until its runtime
	// finishes it with the partial output it holds.
	CancelRequest(ctx context.Context, p CancelRequestParams) (CancelResult, error)
	// PublishCheckpoint activates a checkpoint atomically (compare-and-publish).
	PublishCheckpoint(ctx context.Context, p PublishParams) (PublishResult, error)

	// GetMessageWindow returns canonical messages ordered by seq ascending: the
	// up to limit messages with seq < beforeSeq (beforeSeq = 0 means from the
	// end), and hasMore when older messages exist before the window.
	GetMessageWindow(ctx context.Context, sessionID string, beforeSeq int64, limit int32) ([]Message, bool, error)
	// GetActiveCheckpoint returns the session's active checkpoint, if any.
	GetActiveCheckpoint(ctx context.Context, sessionID string) (Checkpoint, bool, error)

	// Session lifecycle. These are the same operations the v1 contract uses;
	// one store serves both transports during migration.
	GetSession(ctx context.Context, id string) (Session, error)
	ListSessions(ctx context.Context, userID string, limit, offset int32) (sessions []Session, hasMore bool, err error)
	EndSession(ctx context.Context, id string, endedAt time.Time) error
	DeleteSession(ctx context.Context, id string) error
	SetTitle(ctx context.Context, id, title string) (Session, error)
}
