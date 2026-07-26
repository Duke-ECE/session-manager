# session-manager

gRPC service that owns **durable session records and transcripts** in
Supabase Postgres. It is the platform's privilege enforcement point for
session data: the browser-facing backend calls the user-scoped RPCs with the
end user's id, and the agent-runtime writes each completed turn through here
with a shared service token.

- Storage is Postgres (`agent_sessions` / `agent_messages`), accessed via the
  PostgREST REST API with the service role key — the tables have RLS enabled
  with **no policies**, so nothing else can touch them.
- The agent-runtime keeps live agents in memory; this service is the only
  source of truth for what happened in a session.

## API

Defined in the [`Duke-ECE/protos`](https://github.com/Duke-ECE/protos) repo
(`session/v1/session.proto`), consumed as the Go module
`github.com/Duke-ECE/protos`:

**`session.v1.SessionService`**:

- `CreateSession(user_id, llm_model?) → Session` — id is server-generated
  (`sess-<16 hex>`), status `active`
- `GetSession(session_id, user_id) → Session` — ownership enforced
- `ListSessions(user_id) → [Session]` — only this user's sessions
- `EndSession(session_id, user_id)` — sets `status='ended'`, `ended_at=now`
- `AppendTurn(session_id, user_id, messages)` — runtime write-through after a
  completed Chat turn; `seq` is assigned by the server (`max(seq)+1…`)
- `GetTranscript(session_id, user_id?) → [TurnMessage]` — full transcript
  ordered by seq

### Privilege model

| RPC | Rule |
|---|---|
| `CreateSession` / `GetSession` / `ListSessions` / `EndSession` | `user_id` required (`INVALID_ARGUMENT`); row owner mismatch → `PERMISSION_DENIED`; missing row → `NOT_FOUND` |
| `GetTranscript` | session owner passes with just `user_id`; any other caller needs the service token |
| `AppendTurn` | service token always required (trusted runtime callers only; `user_id` is carried for auditing) |

The service token is compared against the `x-service-token` gRPC metadata
header; a missing/wrong token is `UNAUTHENTICATED`. If `SERVICE_TOKEN` is
unset the token-gated paths fail closed.

Server reflection is enabled for `grpcurl`.

## Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `50053` | gRPC listen port |
| `SUPABASE_URL` | — | Supabase project URL (required) |
| `SUPABASE_SERVICE_ROLE_KEY` | — | service role key for PostgREST (required, never logged) |
| `SERVICE_TOKEN` | — | shared token for runtime-internal RPCs |

## Database

`supabase/migrations/0001_sessions.sql` creates `agent_sessions` and
`agent_messages` (RLS on, no policies). Apply it with the Supabase CLI or
paste it into the SQL editor.

## Local development

Requires Go 1.25. To pick up proto changes,
`go get github.com/Duke-ECE/protos@latest`.

```sh
go build ./...
go vet ./...
go test ./...

SUPABASE_URL=https://<project>.supabase.co \
SUPABASE_SERVICE_ROLE_KEY=<key> \
SERVICE_TOKEN=dev-token \
go run ./cmd/server   # serves on :50053

grpcurl -plaintext localhost:50053 list
grpcurl -plaintext localhost:50053 session.v1.SessionService/CreateSession \
  -d '{"user_id":"<uuid>","llm_model":"gpt-4o-mini"}'
grpcurl -plaintext -H 'x-service-token: dev-token' localhost:50053 \
  session.v1.SessionService/AppendTurn \
  -d '{"session_id":"sess-…","user_id":"<uuid>","messages":[{"role":"user","content_json":"{\"text\":\"hi\"}"}]}'
```

## Deploy

CI builds and pushes `ghcr.io/duke-ece/session-manager` on every push to
main, then applies `k8s.yaml` (requires the `KUBE_CONFIG` repo secret, same
as the other managed-agents repos; also expects the `supabase-service-role`
and `session-service-token` k8s secrets). Manual deploy:

```sh
kubectl apply -f k8s.yaml
```
