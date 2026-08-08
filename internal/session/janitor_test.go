package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is an in-memory Store; only the retention paths are real, the
// rest panics if reached.
type fakeStore struct {
	mu       sync.Mutex
	sessions map[string]Session
	messages map[string][]Message
}

func newFakeStore() *fakeStore {
	return &fakeStore{sessions: map[string]Session{}, messages: map[string][]Message{}}
}

func (f *fakeStore) CreateSession(context.Context, string, string, string, string) (Session, error) {
	panic("unused")
}
func (f *fakeStore) GetSession(context.Context, string) (Session, error) { panic("unused") }
func (f *fakeStore) ListSessions(context.Context, string, int32, int32) ([]Session, bool, error) {
	panic("unused")
}
func (f *fakeStore) EndSession(context.Context, string, time.Time) error { panic("unused") }
func (f *fakeStore) SetTitle(context.Context, string, string) (Session, error) {
	panic("unused")
}
func (f *fakeStore) AppendMessages(context.Context, string, []Message) ([]Message, error) {
	panic("unused")
}
func (f *fakeStore) GetMessages(context.Context, string, int32, int32) ([]Message, bool, error) {
	panic("unused")
}

func (f *fakeStore) ListEndedBefore(_ context.Context, cutoff time.Time) ([]Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Session
	for _, sess := range f.sessions {
		if sess.Status != "ended" || sess.EndedAt == "" {
			continue
		}
		endedAt, err := time.Parse(time.RFC3339, sess.EndedAt)
		if err != nil {
			return nil, err
		}
		if endedAt.Before(cutoff) {
			out = append(out, sess)
		}
	}
	return out, nil
}

func (f *fakeStore) DeleteSession(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.sessions[id]; !ok {
		return ErrNotFound
	}
	delete(f.messages, id) // messages first, then the session row
	delete(f.sessions, id)
	return nil
}

func (f *fakeStore) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.sessions[id]
	return ok
}

func (f *fakeStore) messageCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages[id])
}

func TestNewJanitorDisabled(t *testing.T) {
	if j := NewJanitor(newFakeStore(), 0); j != nil {
		t.Errorf("NewJanitor(0 days) = %v, want nil (disabled)", j)
	}
	if j := NewJanitor(newFakeStore(), -3); j != nil {
		t.Errorf("NewJanitor(-3 days) = %v, want nil (disabled)", j)
	}
}

func TestJanitorSweep(t *testing.T) {
	store := newFakeStore()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	at := func(d time.Duration) string { return now.Add(-d).Format(time.RFC3339) }

	store.sessions["sess-old"] = Session{ID: "sess-old", UserID: "user-a", Status: "ended", EndedAt: at(31 * day)}
	store.sessions["sess-recent"] = Session{ID: "sess-recent", UserID: "user-a", Status: "ended", EndedAt: at(2 * day)}
	store.sessions["sess-active"] = Session{ID: "sess-active", UserID: "user-a", Status: "active"}
	store.messages["sess-old"] = []Message{{Seq: 1, Role: "user"}}
	store.messages["sess-recent"] = []Message{{Seq: 1, Role: "user"}}
	store.messages["sess-active"] = []Message{{Seq: 1, Role: "user"}}

	j := NewJanitor(store, 30)
	j.now = func() time.Time { return now }
	j.logf = func(string, ...any) {}

	deleted, err := j.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	if store.has("sess-old") || store.messageCount("sess-old") != 0 {
		t.Errorf("old ended session and its messages should be deleted")
	}
	for _, id := range []string{"sess-recent", "sess-active"} {
		if !store.has(id) || store.messageCount(id) != 1 {
			t.Errorf("%s should be kept (with its messages)", id)
		}
	}

	// A second sweep finds nothing left to do.
	deleted, err = j.Sweep(context.Background())
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if deleted != 0 {
		t.Errorf("second sweep deleted = %d, want 0", deleted)
	}
}

// countingStore wraps fakeStore to count ListEndedBefore calls (one per
// sweep) for the Run ticker test.
type countingStore struct {
	*fakeStore
	sweeps atomic.Int32
}

func (c *countingStore) ListEndedBefore(ctx context.Context, cutoff time.Time) ([]Session, error) {
	c.sweeps.Add(1)
	return c.fakeStore.ListEndedBefore(ctx, cutoff)
}

func TestJanitorRunSweepsOnStartupAndInterval(t *testing.T) {
	cs := &countingStore{fakeStore: newFakeStore()}
	j := NewJanitor(cs, 30)
	j.interval = 5 * time.Millisecond
	j.logf = func(string, ...any) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		j.Run(ctx)
		close(done)
	}()

	// Startup sweep plus at least one tick.
	deadline := time.After(2 * time.Second)
	for cs.sweeps.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("sweeps = %d, want >= 2 (startup + interval)", cs.sweeps.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}
