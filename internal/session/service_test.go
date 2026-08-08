package session

import (
	"context"
	"encoding/json"
	"testing"
)

// transcriptStore is a fakeStore whose read paths actually serve data (the
// janitor's fakeStore panics there); seeded directly so tests can stage rows
// a real jsonb column could never hold, like malformed content_json.
type transcriptStore struct {
	*fakeStore
}

func (f *transcriptStore) GetSession(_ context.Context, id string) (Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sess, ok := f.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return sess, nil
}

func (f *transcriptStore) GetMessages(_ context.Context, sessionID string, _, _ int32) ([]Message, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Message(nil), f.messages[sessionID]...), false, nil
}

func TestGetTranscriptRedaction(t *testing.T) {
	configFull := `{"llm":{"api_key":"sk-platform-secret","base_url":"https://llm.example.com/v1","model":"gpt-4o-mini"}}`
	configMalformed := `{not json`
	configOddShape := `{"llm":"not-an-object"}`
	systemTurn := `{"content":"you are a helpful assistant"}`

	store := &transcriptStore{fakeStore: newFakeStore()}
	store.sessions["sess-x"] = Session{ID: "sess-x", UserID: "user-a", Status: "active"}
	store.messages["sess-x"] = []Message{
		{Seq: 1, Role: "system", ContentJSON: systemTurn},
		{Seq: 2, Role: "config", ContentJSON: configFull},
		{Seq: 3, Role: "config", ContentJSON: configMalformed},
		{Seq: 4, Role: "config", ContentJSON: configOddShape},
	}
	svc := NewService(store, "tok")

	// Owner path: config api_key redacted, malformed/odd shapes pass through
	// byte-identical, seq order preserved.
	msgs, _, err := svc.GetTranscript(context.Background(), "sess-x", "user-a", nil, 0, 0)
	if err != nil {
		t.Fatalf("GetTranscript as owner: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("owner transcript = %d messages, want 4", len(msgs))
	}
	for i, want := range []int32{1, 2, 3, 4} {
		if msgs[i].Seq != want {
			t.Errorf("message %d seq = %d, want %d (ordering broken)", i, msgs[i].Seq, want)
		}
	}
	if msgs[0].ContentJSON != systemTurn {
		t.Errorf("system turn = %q, want byte-identical %q", msgs[0].ContentJSON, systemTurn)
	}
	var redacted map[string]map[string]string
	if err := json.Unmarshal([]byte(msgs[1].ContentJSON), &redacted); err != nil {
		t.Fatalf("redacted config turn is not valid JSON: %v", err)
	}
	if _, leaked := redacted["llm"]["api_key"]; leaked {
		t.Errorf("owner-path config turn still carries api_key: %s", msgs[1].ContentJSON)
	}
	if redacted["llm"]["base_url"] != "https://llm.example.com/v1" || redacted["llm"]["model"] != "gpt-4o-mini" {
		t.Errorf("redacted config llm = %v, want base_url/model intact", redacted["llm"])
	}
	if msgs[2].ContentJSON != configMalformed {
		t.Errorf("malformed config turn = %q, want byte-identical passthrough %q", msgs[2].ContentJSON, configMalformed)
	}
	if msgs[3].ContentJSON != configOddShape {
		t.Errorf("odd-shape config turn = %q, want byte-identical passthrough %q", msgs[3].ContentJSON, configOddShape)
	}

	// The store itself is untouched: redaction happens on the returned copy.
	if got := store.messages["sess-x"][1].ContentJSON; got != configFull {
		t.Errorf("stored config turn = %q, want unmodified %q", got, configFull)
	}

	// Token path (runtime hydration): the full triple must survive.
	msgs, _, err = svc.GetTranscript(context.Background(), "sess-x", "", []string{"tok"}, 0, 0)
	if err != nil {
		t.Fatalf("GetTranscript with token: %v", err)
	}
	for i, want := range []string{systemTurn, configFull, configMalformed, configOddShape} {
		if msgs[i].ContentJSON != want {
			t.Errorf("token-path message %d = %q, want byte-identical %q", i, msgs[i].ContentJSON, want)
		}
	}
}
