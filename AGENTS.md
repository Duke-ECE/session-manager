# AGENTS.md — session-manager

Go 1.25 gRPC service (:50053) owning durable session records and transcripts —
the platform's privilege enforcement point for session data.

**Read [Duke-ECE/standards](https://github.com/Duke-ECE/standards) first** —
its `AGENTS.md` holds the platform-wide rules (layout, testing, toolchain,
data access, secrets, CI). They apply in full here.

Repo-specific:

- Contract: `session.v1.SessionService` (protos repo, pin version in go.mod).
- Storage: Supabase Postgres via PostgREST (`internal/store`), tables
  `agent_sessions` / `agent_messages` — RLS on, no policies, service role only.
- Privilege: user-scoped RPCs enforce ownership (`PERMISSION_DENIED`);
  `AppendTurn` / token-only `GetTranscript` require `x-service-token`.
- Migrations live in `supabase/migrations/` with **timestamped** versions —
  several repos share this Supabase project; counters collide.
- Manual check: `grpcurl -plaintext localhost:50053 list`.
