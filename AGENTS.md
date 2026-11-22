# AGENTS.md — session-manager

Go 1.25 gRPC service (:50053) owning durable session records and transcripts —
the platform's privilege enforcement point for session data.

**Read [Duke-ECE/standards](https://github.com/Duke-ECE/standards) first** —
its `AGENTS.md` holds the platform-wide rules (layout, testing, toolchain,
data access, secrets, CI). They apply in full here.

Repo-specific:

- Contracts: `session.v1.SessionService` (lifecycle + transcript) and
  `session.v2.SessionService` (canonical messages, request execution state,
  fenced leases, compaction checkpoints). Both are registered during migration;
  protos versions are pinned in go.mod.
- Layout (vertical slice + hexagonal): `internal/session` is the domain slice.
  `service.go` holds the v1 rules; `durable.go` / `durable_store.go` /
  `durable_service.go` hold the v2 canonical-message types, the `DurableStore`
  port, and its rules. `internal/transport/grpc` adapts both (thin handlers,
  `NewServer`, the only error→status mapping in `errors.go`);
  `internal/infrastructure/postgrest` implements both ports,
  `internal/infrastructure/memory` is the behavior-parity in-memory
  implementation. The slice imports neither transport nor infrastructure.
- Durable state transitions are **Postgres transaction functions** called
  through PostgREST `/rpc` (`supabase/migrations/*_durable_context_functions.sql`),
  so sequence allocation, revision advance, lease fencing, and idempotency
  receipts cannot race a read-max-write sequence in the service. Failures carry
  a machine-readable reason in the SQLSTATE DETAIL field (`session_fail`), which
  the Go adapter maps to domain errors — see `mapError`.
- Tests: unit tests next to code (fake PostgREST via httptest + gRPC bufconn);
  `test/` holds whole-service integration tests (real PostgREST client against a
  fake Supabase for v1, in-memory store for v2, real TCP gRPC).
  `scripts/verify-db.sh` applies every migration to a real Postgres (fresh and
  v1-upgraded) and runs `supabase/tests/durable_context.sql`, which asserts
  constraints, indexes, locks, lease fencing, revision races, RLS, and function
  privileges. Mocked PostgREST cannot prove those, so CI provisions Postgres.
- Storage: Supabase Postgres via PostgREST
  (`internal/infrastructure/postgrest`), tables `agent_sessions`,
  `agent_messages`, `agent_session_config`, `agent_context_checkpoints`,
  `agent_mutation_receipts` — RLS on, no policies, service role only. Every
  child table cascades from `agent_sessions`, so deleting a session is one
  delete of the session row.
- Privilege: user-scoped RPCs enforce ownership (`PERMISSION_DENIED`);
  `AppendTurn` and every v2 mutation/context RPC require `x-service-token`;
  `GetTranscript` / `SetTitle` / v2 `GetMessages` / v2 `CancelRequest` follow
  the owner-or-token pattern (owner `user_id` or service token). Owner-path
  reads redact the `api_key` from legacy `config` turns; the token path returns
  the full triple for runtime hydration.
- Retention: `RETENTION_DAYS` > 0 starts a janitor (`internal/session/janitor.go`,
  sweep on startup + every 24h) deleting ended sessions older than the cutoff
  and their messages; 0/unset = disabled.
- Migrations live in `supabase/migrations/` with **timestamped** versions —
  several repos share this Supabase project; counters collide.
- With `SUPABASE_URL` unset the binary serves the in-memory store (dev only,
  loud warning); production always sets the Supabase variables.
- Manual check: `grpcurl -plaintext localhost:50053 list`.
