-- session-manager: durable session records and transcripts.
-- RLS is enabled with NO policies: only the service role (which bypasses
-- RLS) can read or write. All access goes through session-manager.

create table if not exists agent_sessions (
  id          text primary key,
  user_id     uuid not null,
  status      text not null default 'active',
  llm_model   text,
  created_at  timestamptz not null default now(),
  last_active timestamptz not null default now(),
  ended_at    timestamptz
);

create table if not exists agent_messages (
  id         bigint generated always as identity primary key,
  session_id text not null references agent_sessions(id) on delete cascade,
  seq        int not null,
  role       text not null,
  content    jsonb not null,
  created_at timestamptz not null default now(),
  unique (session_id, seq)
);

alter table agent_sessions enable row level security;
alter table agent_messages enable row level security;
