-- Freeze the resolved thinking mode when a session is admitted. NULL marks
-- sessions created before this contract and preserves their legacy behavior.
alter table public.agent_sessions
  add column if not exists thinking_mode text;

alter table public.agent_sessions
  add constraint agent_sessions_thinking_mode_check
  check (thinking_mode in ('off', 'low', 'high', 'max', 'provider_default'));
