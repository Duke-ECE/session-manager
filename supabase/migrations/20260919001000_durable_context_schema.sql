-- Durable canonical messages, request execution state, fenced leases, and
-- context checkpoints. Existing v1 transcript rows are retained as format 0.

alter table agent_sessions
  add column if not exists last_seq bigint not null default 0,
  add column if not exists revision bigint not null default 0,
  add column if not exists active_checkpoint_id text,
  add column if not exists active_request_message_id text,
  add column if not exists lease_owner text,
  add column if not exists lease_generation bigint not null default 0,
  add column if not exists lease_expires_at timestamptz;

alter table agent_sessions
  add constraint agent_sessions_last_seq_nonnegative check (last_seq >= 0),
  add constraint agent_sessions_revision_nonnegative check (revision >= 0),
  add constraint agent_sessions_lease_generation_nonnegative check (lease_generation >= 0),
  add constraint agent_sessions_lease_shape check (
    (lease_owner is null and lease_expires_at is null)
    or (lease_owner is not null and lease_expires_at is not null)
  );

alter table agent_messages
  add column if not exists message_id text,
  add column if not exists request_message_id text,
  add column if not exists format_version integer not null default 0,
  add column if not exists message_status text not null default 'complete',
  add column if not exists reply_to_message_id text,
  add column if not exists usage jsonb,
  add column if not exists provider_metadata jsonb,
  add column if not exists client_request_id text,
  add column if not exists request_hash text,
  add column if not exists execution_status text,
  add column if not exists started_at timestamptz,
  add column if not exists finished_at timestamptz,
  add column if not exists error_code text,
  add column if not exists cancellation_requested boolean not null default false;

update agent_messages
set message_id = 'legacy-' || id::text
where message_id is null;

alter table agent_messages
  alter column message_id set not null,
  alter column seq type bigint,
  add constraint agent_messages_identity_unique unique (session_id, message_id),
  add constraint agent_messages_format_version_nonnegative check (format_version >= 0),
  add constraint agent_messages_status_valid check (
    message_status in ('complete', 'partial', 'interrupted')
  ),
  add constraint agent_messages_v2_role_valid check (
    format_version = 0 or role in ('user', 'assistant', 'tool')
  ),
  add constraint agent_messages_v2_content_valid check (
    format_version = 0 or jsonb_typeof(content) = 'array'
  ),
  add constraint agent_messages_request_status_valid check (
    execution_status is null
    or execution_status in ('queued', 'running', 'completed', 'failed', 'cancelled', 'interrupted')
  ),
  add constraint agent_messages_request_root_shape check (
    (client_request_id is null
      and request_hash is null
      and execution_status is null
      and started_at is null
      and finished_at is null
      and error_code is null
      and cancellation_requested = false)
    or
    (format_version > 0
      and role = 'user'
      and request_message_id = message_id
      and client_request_id is not null
      and request_hash is not null
      and execution_status is not null)
  ),
  add constraint agent_messages_terminal_time_shape check (
    execution_status is null
    or execution_status in ('queued', 'running')
    or finished_at is not null
  ),
  add constraint agent_messages_request_fk
    foreign key (session_id, request_message_id)
    references agent_messages(session_id, message_id)
    deferrable initially deferred,
  add constraint agent_messages_reply_fk
    foreign key (session_id, reply_to_message_id)
    references agent_messages(session_id, message_id)
    deferrable initially deferred;

update agent_sessions s
set last_seq = coalesce((
  select max(m.seq)
  from agent_messages m
  where m.session_id = s.id
), 0);

create unique index if not exists agent_messages_client_request_unique
  on agent_messages(session_id, client_request_id)
  where client_request_id is not null;

create unique index if not exists agent_messages_one_active_request
  on agent_messages(session_id)
  where execution_status in ('queued', 'running');

create index if not exists agent_messages_session_seq_desc
  on agent_messages(session_id, seq desc);

create index if not exists agent_messages_request_seq
  on agent_messages(session_id, request_message_id, seq);

create table if not exists agent_session_config (
  session_id             text primary key references agent_sessions(id) on delete cascade,
  format_version         integer not null,
  config_hash            text not null,
  system_prompt          text not null default '',
  provider               text not null,
  model                  text not null,
  base_url               text not null,
  credential_ref         text not null,
  context_window_tokens  bigint not null,
  output_limit_tokens    bigint not null,
  tools                   jsonb not null default '[]'::jsonb,
  created_at              timestamptz not null default now(),
  check (format_version > 0),
  check (context_window_tokens > 0),
  check (output_limit_tokens > 0),
  check (output_limit_tokens < context_window_tokens),
  check (jsonb_typeof(tools) = 'array')
);

create table if not exists agent_context_checkpoints (
  session_id                 text not null references agent_sessions(id) on delete cascade,
  id                         text not null,
  parent_checkpoint_id       text,
  covered_through_seq        bigint not null,
  source_head_seq            bigint not null,
  source_revision            bigint not null,
  config_hash                text not null,
  active_request_message_id  text,
  resume_after_seq           bigint not null,
  summary                    jsonb not null,
  format_version             integer not null,
  prompt_version             text not null,
  summarizer_provider        text not null,
  summarizer_model           text not null,
  estimated_tokens_before    bigint not null,
  estimated_tokens_after     bigint not null,
  generation_usage           jsonb,
  created_at                 timestamptz not null default now(),
  primary key (session_id, id),
  check (covered_through_seq >= 0),
  check (source_head_seq >= covered_through_seq),
  check (resume_after_seq >= covered_through_seq),
  check (source_revision >= 0),
  check (format_version > 0),
  check (jsonb_typeof(summary) = 'array'),
  foreign key (session_id, parent_checkpoint_id)
    references agent_context_checkpoints(session_id, id),
  foreign key (session_id, active_request_message_id)
    references agent_messages(session_id, message_id)
);

alter table agent_sessions
  add constraint agent_sessions_active_request_fk
    foreign key (id, active_request_message_id)
    references agent_messages(session_id, message_id)
    deferrable initially deferred,
  add constraint agent_sessions_active_checkpoint_fk
    foreign key (id, active_checkpoint_id)
    references agent_context_checkpoints(session_id, id)
    deferrable initially deferred;

create table if not exists agent_mutation_receipts (
  session_id     text not null references agent_sessions(id) on delete cascade,
  mutation_id    text not null,
  mutation_hash  text not null,
  result         jsonb not null,
  created_at     timestamptz not null default now(),
  primary key (session_id, mutation_id)
);

create index if not exists agent_mutation_receipts_created_at
  on agent_mutation_receipts(created_at);

create or replace function reject_agent_session_config_update()
returns trigger
language plpgsql
as $$
begin
  raise exception 'agent_session_config rows are immutable' using errcode = '55000';
end;
$$;

drop trigger if exists agent_session_config_immutable on agent_session_config;
create trigger agent_session_config_immutable
before update on agent_session_config
for each row execute function reject_agent_session_config_update();

create or replace function protect_completed_agent_message()
returns trigger
language plpgsql
as $$
begin
  if old.message_status = 'complete' and (
    new.message_id is distinct from old.message_id
    or new.session_id is distinct from old.session_id
    or new.seq is distinct from old.seq
    or new.request_message_id is distinct from old.request_message_id
    or new.role is distinct from old.role
    or new.content is distinct from old.content
    or new.format_version is distinct from old.format_version
    or new.reply_to_message_id is distinct from old.reply_to_message_id
    or new.usage is distinct from old.usage
    or new.provider_metadata is distinct from old.provider_metadata
  ) then
    raise exception 'completed agent message content is immutable' using errcode = '55000';
  end if;
  return new;
end;
$$;

drop trigger if exists agent_messages_protect_completed on agent_messages;
create trigger agent_messages_protect_completed
before update on agent_messages
for each row execute function protect_completed_agent_message();

alter table agent_session_config enable row level security;
alter table agent_context_checkpoints enable row level security;
alter table agent_mutation_receipts enable row level security;
