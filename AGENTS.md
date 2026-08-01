# AGENTS.md — session-manager

Go 1.25 gRPC service (:50053) owning durable session records and transcripts —
the platform's privilege enforcement point for session data.

**Read [Duke-ECE/standards](https://github.com/Duke-ECE/standards) first** —
its `AGENTS.md` holds the platform-wide rules (layout, testing, toolchain,
data access, secrets, CI). They apply in full here.

Repo-specific:

- Contract: `session.v1.SessionService` (protos repo, pin version in go.mod).
- Layout (vertical slice + hexagonal): `internal/session` is the domain slice
  (types, `Store` port, business rules in `service.go` — ownership, id
  generation, end lifecycle, service-token policy); `internal/transport/grpc`
  adapts it to gRPC (thin handlers, `NewServer`, the only error→status
  mapping in `errors.go`); `internal/infrastructure/postgrest` implements the
  `Store` port. The slice imports neither transport nor infrastructure.
- Tests: unit tests next to code (fake PostgREST via httptest + gRPC
  bufconn); `test/` holds whole-service integration tests (real PostgREST
  client against a fake Supabase, real TCP gRPC).
- Storage: Supabase Postgres via PostgREST
  (`internal/infrastructure/postgrest`), tables
  `agent_sessions` / `agent_messages` — RLS on, no policies, service role only.
- Privilege: user-scoped RPCs enforce ownership (`PERMISSION_DENIED`);
  `AppendTurn` / token-only `GetTranscript` require `x-service-token`.
- Migrations live in `supabase/migrations/` with **timestamped** versions —
  several repos share this Supabase project; counters collide.
- Manual check: `grpcurl -plaintext localhost:50053 list`.
