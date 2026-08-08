package session

import (
	"context"
	"log"
	"time"
)

// Janitor is the retention sweeper: every interval (and once on startup) it
// deletes ended sessions whose ended_at is older than the retention cutoff,
// including their messages. now, interval, and logf are seams for tests.
type Janitor struct {
	store     Store
	retention time.Duration
	now       func() time.Time
	interval  time.Duration
	logf      func(format string, args ...any)
}

// NewJanitor returns a retention janitor for retentionDays, or nil when
// retentionDays <= 0 (retention disabled).
func NewJanitor(store Store, retentionDays int) *Janitor {
	if retentionDays <= 0 {
		return nil
	}
	return &Janitor{
		store:     store,
		retention: time.Duration(retentionDays) * 24 * time.Hour,
		now:       time.Now,
		interval:  24 * time.Hour,
		logf:      log.Printf,
	}
}

// Run sweeps once on startup and then every interval until ctx is done.
func (j *Janitor) Run(ctx context.Context) {
	j.sweepLogged(ctx)
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			j.sweepLogged(ctx)
		}
	}
}

func (j *Janitor) sweepLogged(ctx context.Context) {
	if _, err := j.Sweep(ctx); err != nil {
		j.logf("retention sweep failed: %v", err)
	}
}

// Sweep deletes every ended session older than the cutoff (and its
// messages, via the same messages-then-session order as DeleteSession) and
// returns how many sessions were deleted.
func (j *Janitor) Sweep(ctx context.Context) (int, error) {
	cutoff := j.now().Add(-j.retention)
	ended, err := j.store.ListEndedBefore(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, sess := range ended {
		if err := j.store.DeleteSession(ctx, sess.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	j.logf("retention sweep: deleted %d ended session(s) with ended_at before %s", deleted, cutoff.UTC().Format(time.RFC3339))
	return deleted, nil
}
