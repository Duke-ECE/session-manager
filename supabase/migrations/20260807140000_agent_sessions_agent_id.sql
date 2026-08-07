-- session-manager: optional agent_id linking a durable session to an agent
-- template (owned by the backend; stored here as an opaque id). Nullable, no
-- default, no FK — the agents table lives in the backend's domain. Sessions
-- created before this migration keep a null agent_id.

alter table public.agent_sessions add column if not exists agent_id text;
