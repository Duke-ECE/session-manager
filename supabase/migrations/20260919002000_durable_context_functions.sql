-- Durable context transaction functions.
--
-- Every durable state transition runs inside one short Postgres transaction so
-- sequence allocation, revision advance, lease fencing, and idempotency receipts
-- cannot race through a read-max-write sequence in the service. They are called
-- through PostgREST /rpc by session-manager; the wrappers use invoker security
-- (the service role bypasses RLS) and are revoked from anon/authenticated.
--
-- Failures carry a machine-readable reason in the SQLSTATE DETAIL field so the
-- Go adapter can map them to domain errors without parsing prose. PostgREST
-- exposes it as `details` in the error body.

-- ---------------------------------------------------------------- helpers ---

-- session_fail aborts the calling function with a machine-readable reason.
create or replace function public.session_fail(p_code text, p_message text)
returns void
language plpgsql
set search_path = public, pg_temp
as $$
begin
  raise exception using
    errcode = case when p_code = 'not_found' then 'P0002' else 'P0001' end,
    message = p_message,
    detail = p_code;
end;
$$;

-- session_message_json is the stable JSON projection of one canonical message.
create or replace function public.session_message_json(m public.agent_messages)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'id', m.message_id,
    'session_id', m.session_id,
    'seq', m.seq,
    'request_message_id', m.request_message_id,
    'role', m.role,
    'content', m.content,
    'format_version', m.format_version,
    'message_status', m.message_status,
    'reply_to_message_id', m.reply_to_message_id,
    'usage', m.usage,
    'provider_metadata', m.provider_metadata,
    'client_request_id', m.client_request_id,
    'request_hash', m.request_hash,
    'execution_status', m.execution_status,
    'started_at', m.started_at,
    'finished_at', m.finished_at,
    'error_code', m.error_code,
    'cancellation_requested', m.cancellation_requested,
    'created_at', m.created_at
  );
$$;

-- session_json is the stable JSON projection of one session row, including the
-- fencing columns the runtime needs (lease generation/expiry are not conversation
-- state and never reach a browser).
create or replace function public.session_json(s public.agent_sessions)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'id', s.id,
    'user_id', s.user_id,
    'status', s.status,
    'agent_id', s.agent_id,
    'title', s.title,
    'last_seq', s.last_seq,
    'revision', s.revision,
    'active_checkpoint_id', s.active_checkpoint_id,
    'active_request_message_id', s.active_request_message_id,
    'lease_owner', s.lease_owner,
    'lease_generation', s.lease_generation,
    'lease_expires_at', s.lease_expires_at,
    'created_at', s.created_at,
    'last_active', s.last_active,
    'ended_at', s.ended_at
  );
$$;

create or replace function public.session_config_json(c public.agent_session_config)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'session_id', c.session_id,
    'format_version', c.format_version,
    'config_hash', c.config_hash,
    'system_prompt', c.system_prompt,
    'provider', c.provider,
    'model', c.model,
    'base_url', c.base_url,
    'credential_ref', c.credential_ref,
    'context_window_tokens', c.context_window_tokens,
    'output_limit_tokens', c.output_limit_tokens,
    'tools', c.tools,
    'created_at', c.created_at
  );
$$;

create or replace function public.session_checkpoint_json(cp public.agent_context_checkpoints)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object(
    'id', cp.id,
    'session_id', cp.session_id,
    'parent_checkpoint_id', cp.parent_checkpoint_id,
    'covered_through_seq', cp.covered_through_seq,
    'source_head_seq', cp.source_head_seq,
    'source_revision', cp.source_revision,
    'config_hash', cp.config_hash,
    'active_request_message_id', cp.active_request_message_id,
    'resume_after_seq', cp.resume_after_seq,
    'summary', cp.summary,
    'format_version', cp.format_version,
    'prompt_version', cp.prompt_version,
    'summarizer_provider', cp.summarizer_provider,
    'summarizer_model', cp.summarizer_model,
    'estimated_tokens_before', cp.estimated_tokens_before,
    'estimated_tokens_after', cp.estimated_tokens_after,
    'generation_usage', cp.generation_usage,
    'created_at', cp.created_at
  );
$$;

-- session_insert_messages validates a JSON array of message drafts and inserts
-- them at p_start_seq, p_start_seq+1, ... The caller owns the session row lock,
-- so sequence allocation cannot interleave.
create or replace function public.session_insert_messages(
  p_session_id         text,
  p_request_message_id text,
  p_messages           jsonb,
  p_start_seq          bigint
) returns setof public.agent_messages
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_draft  jsonb;
  v_i      bigint := 0;
  v_role   text;
  v_format integer;
  v_status text;
begin
  for v_draft in select * from jsonb_array_elements(coalesce(p_messages, '[]'::jsonb)) loop
    v_role := v_draft->>'role';
    if v_role not in ('assistant', 'tool') then
      perform session_fail('invalid_argument', format('message role %L is not appendable', coalesce(v_role, 'null')));
    end if;
    if v_draft->'content' is null or jsonb_typeof(v_draft->'content') <> 'array' then
      perform session_fail('invalid_argument', 'message content must be a JSON array of blocks');
    end if;
    v_format := coalesce((v_draft->>'format_version')::integer, 1);
    if v_format <= 0 then
      perform session_fail('invalid_argument', 'message format_version must be positive');
    end if;
    v_status := coalesce(v_draft->>'message_status', 'complete');
    if v_status not in ('complete', 'partial', 'interrupted') then
      perform session_fail('invalid_argument', format('message_status %L is invalid', v_status));
    end if;
    if coalesce(v_draft->>'id', '') = '' then
      perform session_fail('invalid_argument', 'message id is required');
    end if;

    return query
      insert into public.agent_messages (
        session_id, seq, role, content, message_id, request_message_id,
        format_version, message_status, reply_to_message_id, usage, provider_metadata
      ) values (
        p_session_id, p_start_seq + v_i, v_role, v_draft->'content', v_draft->>'id', p_request_message_id,
        v_format, v_status, nullif(v_draft->>'reply_to_message_id', ''),
        v_draft->'usage', v_draft->'provider_metadata'
      )
      returning *;
    v_i := v_i + 1;
  end loop;
end;
$$;

-- ------------------------------------------------------------- operations ---

-- session_create_session admits a session together with its immutable resolved
-- configuration, in one transaction.
create or replace function public.session_create_session(
  p_id       text,
  p_user_id  text,
  p_agent_id text,
  p_config   jsonb
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess     public.agent_sessions;
  v_cfg      public.agent_session_config;
  v_fmt      integer;
  v_hash     text;
  v_provider text;
  v_model    text;
  v_base     text;
  v_window   bigint;
  v_output   bigint;
  v_tools    jsonb;
begin
  if coalesce(p_id, '') = '' or coalesce(p_user_id, '') = '' then
    perform session_fail('invalid_argument', 'session id and user_id are required');
  end if;
  if p_config is null then
    perform session_fail('invalid_argument', 'session config is required');
  end if;
  begin
    perform p_user_id::uuid;
  exception when invalid_text_representation then
    perform session_fail('invalid_argument', 'user_id must be a UUID');
  end;

  v_fmt      := coalesce((p_config->>'format_version')::integer, 0);
  v_hash     := coalesce(p_config->>'config_hash', '');
  v_provider := coalesce(p_config->>'provider', '');
  v_model    := coalesce(p_config->>'model', '');
  v_base     := coalesce(p_config->>'base_url', '');
  v_tools    := coalesce(p_config->'tools', '[]'::jsonb);
  v_window   := coalesce((p_config->>'context_window_tokens')::bigint, 0);
  v_output   := coalesce((p_config->>'output_limit_tokens')::bigint, 0);

  if v_fmt <= 0 or v_hash = '' then
    perform session_fail('invalid_argument', 'config format_version and config_hash are required');
  end if;
  if v_model = '' or v_base = '' then
    perform session_fail('invalid_argument', 'config model and base_url are required');
  end if;
  if v_window <= 0 or v_output <= 0 or v_output >= v_window then
    perform session_fail('invalid_argument', 'config requires 0 < output_limit_tokens < context_window_tokens');
  end if;
  if jsonb_typeof(v_tools) <> 'array' then
    perform session_fail('invalid_argument', 'config tools must be a JSON array');
  end if;

  insert into public.agent_sessions (id, user_id, agent_id, status, llm_model)
  values (p_id, p_user_id::uuid, nullif(p_agent_id, ''), 'active', nullif(v_model, ''))
  returning * into v_sess;

  insert into public.agent_session_config (
    session_id, format_version, config_hash, system_prompt, provider, model, base_url,
    credential_ref, context_window_tokens, output_limit_tokens, tools
  ) values (
    p_id, v_fmt, v_hash, coalesce(p_config->>'system_prompt', ''), v_provider, v_model, v_base,
    coalesce(p_config->>'credential_ref', ''), v_window, v_output, v_tools
  ) returning * into v_cfg;

  return jsonb_build_object('session', session_json(v_sess), 'config', session_config_json(v_cfg));
end;
$$;

-- session_begin_request deduplicates the request, persists the initiating user
-- message with queued execution status, and points the session at it. It is the
-- only admission path for new work; a second nonterminal request is rejected.
create or replace function public.session_begin_request(
  p_session_id         text,
  p_user_id            text,
  p_request_message_id text,
  p_client_request_id  text,
  p_request_hash       text,
  p_content            jsonb,
  p_format_version     integer
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess     public.agent_sessions;
  v_existing public.agent_messages;
  v_root     public.agent_messages;
  v_seq      bigint;
begin
  if coalesce(p_session_id, '') = '' or coalesce(p_request_message_id, '') = ''
     or coalesce(p_client_request_id, '') = '' or coalesce(p_request_hash, '') = '' then
    perform session_fail('invalid_argument', 'session_id, request_message_id, client_request_id and request_hash are required');
  end if;
  if p_format_version is null or p_format_version <= 0 then
    perform session_fail('invalid_argument', 'format_version must be positive');
  end if;
  if p_content is null or jsonb_typeof(p_content) <> 'array' then
    perform session_fail('invalid_argument', 'content must be a JSON array of blocks');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;
  if v_sess.user_id::text <> p_user_id then
    perform session_fail('permission_denied', 'session belongs to another user');
  end if;
  if v_sess.status = 'ended' then
    perform session_fail('session_ended', 'session has ended');
  end if;

  select * into v_existing from public.agent_messages
   where session_id = p_session_id and client_request_id = p_client_request_id;
  if found then
    if v_existing.request_hash = p_request_hash then
      return jsonb_build_object(
        'session', session_json(v_sess),
        'request_message', session_message_json(v_existing),
        'deduplicated', true);
    end if;
    perform session_fail('hash_conflict', 'client_request_id was already used with different content');
  end if;

  if v_sess.active_request_message_id is not null then
    perform session_fail('request_busy', 'another request is already active on this session');
  end if;

  v_seq := v_sess.last_seq + 1;
  insert into public.agent_messages (
    session_id, seq, role, content, message_id, request_message_id,
    format_version, message_status, client_request_id, request_hash, execution_status
  ) values (
    p_session_id, v_seq, 'user', p_content, p_request_message_id, p_request_message_id,
    p_format_version, 'complete', p_client_request_id, p_request_hash, 'queued'
  ) returning * into v_root;

  update public.agent_sessions
     set last_seq = v_seq,
         revision = revision + 1,
         active_request_message_id = p_request_message_id,
         last_active = now()
   where id = p_session_id
   returning * into v_sess;

  return jsonb_build_object(
    'session', session_json(v_sess),
    'request_message', session_message_json(v_root),
    'deduplicated', false);
end;
$$;

-- session_acquire_lease takes fenced ownership of a request. A live lease held by
-- a different owner is a conflict; taking over an expired lease bumps the
-- generation so the old owner's writes are rejected.
create or replace function public.session_acquire_lease(
  p_session_id         text,
  p_request_message_id text,
  p_owner              text,
  p_ttl_seconds        integer
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess public.agent_sessions;
  v_root public.agent_messages;
  v_ttl  integer;
begin
  if coalesce(p_owner, '') = '' or coalesce(p_request_message_id, '') = '' then
    perform session_fail('invalid_argument', 'request_message_id and owner are required');
  end if;
  v_ttl := coalesce(p_ttl_seconds, 90);
  if v_ttl <= 0 or v_ttl > 3600 then
    perform session_fail('invalid_argument', 'ttl_seconds must be between 1 and 3600');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;
  if v_sess.status = 'ended' then
    perform session_fail('session_ended', 'session has ended');
  end if;
  if v_sess.active_request_message_id is distinct from p_request_message_id then
    perform session_fail('request_not_active', 'request is not the active request of this session');
  end if;

  select * into v_root from public.agent_messages
   where session_id = p_session_id and message_id = p_request_message_id;
  if not found then
    perform session_fail('not_found', 'request message not found');
  end if;
  if v_root.execution_status not in ('queued', 'running') then
    perform session_fail('request_terminal', 'request already finished');
  end if;
  if v_root.cancellation_requested then
    perform session_fail('request_cancelled', 'request was cancelled');
  end if;

  if v_sess.lease_owner is not null
     and v_sess.lease_owner <> p_owner
     and v_sess.lease_expires_at > now() then
    perform session_fail('lease_conflict', 'session is leased by another runtime');
  end if;

  update public.agent_sessions
     set lease_owner = p_owner,
         lease_generation = lease_generation + 1,
         lease_expires_at = now() + make_interval(secs => v_ttl),
         revision = revision + 1
   where id = p_session_id
   returning * into v_sess;

  update public.agent_messages
     set execution_status = 'running', started_at = coalesce(started_at, now())
   where session_id = p_session_id and message_id = p_request_message_id
     and execution_status = 'queued';

  return jsonb_build_object(
    'session', session_json(v_sess),
    'lease', jsonb_build_object(
      'owner', v_sess.lease_owner,
      'generation', v_sess.lease_generation,
      'expires_at', v_sess.lease_expires_at));
end;
$$;

-- session_renew_lease extends a held lease using database time. Renewal is not
-- semantic state, so it does not advance the session revision.
create or replace function public.session_renew_lease(
  p_session_id  text,
  p_owner       text,
  p_generation  bigint,
  p_ttl_seconds integer
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess public.agent_sessions;
  v_ttl  integer;
begin
  v_ttl := coalesce(p_ttl_seconds, 90);
  if v_ttl <= 0 or v_ttl > 3600 then
    perform session_fail('invalid_argument', 'ttl_seconds must be between 1 and 3600');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;
  if v_sess.lease_owner is distinct from p_owner or v_sess.lease_generation <> p_generation then
    perform session_fail('stale_lease', 'lease owner or generation does not match');
  end if;
  if v_sess.lease_expires_at <= now() then
    perform session_fail('lease_expired', 'lease has expired');
  end if;

  update public.agent_sessions
     set lease_expires_at = now() + make_interval(secs => v_ttl)
   where id = p_session_id
   returning * into v_sess;

  return jsonb_build_object('lease', jsonb_build_object(
    'owner', v_sess.lease_owner,
    'generation', v_sess.lease_generation,
    'expires_at', v_sess.lease_expires_at));
end;
$$;

-- session_release_lease clears a held lease. Releasing a lease that was already
-- taken over is a no-op reported as released=false.
create or replace function public.session_release_lease(
  p_session_id text,
  p_owner      text,
  p_generation bigint
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess     public.agent_sessions;
  v_released boolean := false;
begin
  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;
  if v_sess.lease_owner = p_owner and v_sess.lease_generation = p_generation then
    update public.agent_sessions
       set lease_owner = null, lease_expires_at = null
     where id = p_session_id
     returning * into v_sess;
    v_released := true;
  end if;
  return jsonb_build_object('released', v_released);
end;
$$;

-- session_append_messages commits one idempotent, fenced message batch.
create or replace function public.session_append_messages(
  p_session_id         text,
  p_request_message_id text,
  p_mutation_id        text,
  p_mutation_hash      text,
  p_expected_revision  bigint,
  p_lease_generation   bigint,
  p_messages           jsonb
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess    public.agent_sessions;
  v_root    public.agent_messages;
  v_receipt public.agent_mutation_receipts;
  v_msgs    jsonb;
  v_result  jsonb;
  v_count   bigint;
begin
  if coalesce(p_mutation_id, '') = '' or coalesce(p_mutation_hash, '') = '' then
    perform session_fail('invalid_argument', 'mutation_id and mutation_hash are required');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;

  -- The idempotency receipt is checked before payload validation: a retry that
  -- replays an already-committed mutation returns the stored result unchanged.
  select * into v_receipt from public.agent_mutation_receipts
   where session_id = p_session_id and mutation_id = p_mutation_id;
  if found then
    if v_receipt.mutation_hash = p_mutation_hash then
      return v_receipt.result || jsonb_build_object('deduplicated', true);
    end if;
    perform session_fail('mutation_conflict', 'mutation_id was reused with a different payload');
  end if;

  if p_messages is null or jsonb_typeof(p_messages) <> 'array' or jsonb_array_length(p_messages) = 0 then
    perform session_fail('invalid_argument', 'messages must be a non-empty JSON array');
  end if;

  if v_sess.status = 'ended' then
    perform session_fail('session_ended', 'session has ended');
  end if;
  if p_lease_generation is null or p_lease_generation <= 0
     or v_sess.lease_owner is null or v_sess.lease_generation <> p_lease_generation then
    perform session_fail('stale_lease', 'a current lease generation is required');
  end if;
  if v_sess.lease_expires_at <= now() then
    perform session_fail('lease_expired', 'lease has expired');
  end if;
  if v_sess.active_request_message_id is distinct from p_request_message_id then
    perform session_fail('request_not_active', 'request is not the active request of this session');
  end if;

  select * into v_root from public.agent_messages
   where session_id = p_session_id and message_id = p_request_message_id;
  if not found then
    perform session_fail('not_found', 'request message not found');
  end if;
  if v_root.execution_status not in ('queued', 'running') then
    perform session_fail('request_terminal', 'request already finished');
  end if;
  if v_root.cancellation_requested then
    perform session_fail('request_cancelled', 'request was cancelled');
  end if;
  if coalesce(p_expected_revision, 0) > 0 and v_sess.revision <> p_expected_revision then
    perform session_fail('revision_conflict',
      format('revision is %s, expected %s', v_sess.revision, p_expected_revision));
  end if;

  v_count := jsonb_array_length(p_messages);
  select coalesce(jsonb_agg(session_message_json(m) order by m.seq), '[]'::jsonb)
    into v_msgs
    from session_insert_messages(p_session_id, p_request_message_id, p_messages, v_sess.last_seq + 1) m;

  update public.agent_sessions
     set last_seq = v_sess.last_seq + v_count,
         revision = revision + 1,
         last_active = now()
   where id = p_session_id
   returning * into v_sess;

  v_result := jsonb_build_object('session', session_json(v_sess), 'messages', v_msgs);
  insert into public.agent_mutation_receipts (session_id, mutation_id, mutation_hash, result)
  values (p_session_id, p_mutation_id, p_mutation_hash, v_result);

  return v_result || jsonb_build_object('deduplicated', false);
end;
$$;

-- session_finish_request writes the final batch and the root's terminal state,
-- clears the active request, and releases the matching lease in one transaction.
create or replace function public.session_finish_request(
  p_session_id         text,
  p_request_message_id text,
  p_mutation_id        text,
  p_mutation_hash      text,
  p_expected_revision  bigint,
  p_lease_generation   bigint,
  p_status             text,
  p_messages           jsonb,
  p_error_code         text
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess    public.agent_sessions;
  v_root    public.agent_messages;
  v_receipt public.agent_mutation_receipts;
  v_msgs    jsonb := '[]'::jsonb;
  v_result  jsonb;
  v_count   bigint := 0;
begin
  if coalesce(p_mutation_id, '') = '' or coalesce(p_mutation_hash, '') = '' then
    perform session_fail('invalid_argument', 'mutation_id and mutation_hash are required');
  end if;
  if p_status is null or p_status not in ('completed', 'failed', 'cancelled', 'interrupted') then
    perform session_fail('invalid_argument', format('finish status %L is not terminal', coalesce(p_status, 'null')));
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;

  select * into v_receipt from public.agent_mutation_receipts
   where session_id = p_session_id and mutation_id = p_mutation_id;
  if found then
    if v_receipt.mutation_hash = p_mutation_hash then
      return v_receipt.result || jsonb_build_object('deduplicated', true);
    end if;
    perform session_fail('mutation_conflict', 'mutation_id was reused with a different payload');
  end if;

  if p_lease_generation is null or p_lease_generation <= 0
     or v_sess.lease_owner is null or v_sess.lease_generation <> p_lease_generation then
    perform session_fail('stale_lease', 'a current lease generation is required');
  end if;
  if v_sess.lease_expires_at <= now() then
    perform session_fail('lease_expired', 'lease has expired');
  end if;
  if v_sess.active_request_message_id is distinct from p_request_message_id then
    perform session_fail('request_not_active', 'request is not the active request of this session');
  end if;

  select * into v_root from public.agent_messages
   where session_id = p_session_id and message_id = p_request_message_id;
  if not found then
    perform session_fail('not_found', 'request message not found');
  end if;
  if v_root.execution_status not in ('queued', 'running') then
    perform session_fail('request_terminal', 'request already finished');
  end if;
  if coalesce(p_expected_revision, 0) > 0 and v_sess.revision <> p_expected_revision then
    perform session_fail('revision_conflict',
      format('revision is %s, expected %s', v_sess.revision, p_expected_revision));
  end if;

  if p_messages is not null and jsonb_typeof(p_messages) = 'array' and jsonb_array_length(p_messages) > 0 then
    v_count := jsonb_array_length(p_messages);
    select coalesce(jsonb_agg(session_message_json(m) order by m.seq), '[]'::jsonb)
      into v_msgs
      from session_insert_messages(p_session_id, p_request_message_id, p_messages, v_sess.last_seq + 1) m;
  end if;

  update public.agent_messages
     set execution_status = p_status,
         finished_at = now(),
         error_code = nullif(p_error_code, '')
   where session_id = p_session_id and message_id = p_request_message_id
   returning * into v_root;

  update public.agent_sessions
     set last_seq = v_sess.last_seq + v_count,
         revision = revision + 1,
         active_request_message_id = null,
         lease_owner = null,
         lease_expires_at = null,
         last_active = now()
   where id = p_session_id
   returning * into v_sess;

  v_result := jsonb_build_object(
    'session', session_json(v_sess),
    'request_message', session_message_json(v_root),
    'messages', v_msgs);
  insert into public.agent_mutation_receipts (session_id, mutation_id, mutation_hash, result)
  values (p_session_id, p_mutation_id, p_mutation_hash, v_result);

  return v_result || jsonb_build_object('deduplicated', false);
end;
$$;

-- session_cancel_request is the owner-initiated cancellation path. It flags the
-- active request immediately. A queued request, or a running one whose lease has
-- lapsed, is terminated here (interrupted: the outcome is unknown); a live
-- runtime keeps the lease and finishes the request itself with its partial output.
create or replace function public.session_cancel_request(
  p_session_id         text,
  p_user_id            text,
  p_request_message_id text
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess   public.agent_sessions;
  v_root   public.agent_messages;
  v_target text;
begin
  if coalesce(p_user_id, '') = '' then
    perform session_fail('invalid_argument', 'user_id is required');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;
  if v_sess.user_id::text <> p_user_id then
    perform session_fail('permission_denied', 'session belongs to another user');
  end if;

  v_target := coalesce(nullif(p_request_message_id, ''), v_sess.active_request_message_id);
  if v_target is null then
    perform session_fail('not_found', 'session has no active request to cancel');
  end if;

  select * into v_root from public.agent_messages
   where session_id = p_session_id and message_id = v_target;
  if not found then
    perform session_fail('not_found', 'request message not found');
  end if;

  if v_root.execution_status not in ('queued', 'running') then
    return jsonb_build_object('request_message', session_message_json(v_root), 'deduplicated', true);
  end if;

  update public.agent_messages
     set cancellation_requested = true
   where session_id = p_session_id and message_id = v_target
   returning * into v_root;

  if v_root.execution_status = 'queued'
     or v_sess.lease_owner is null
     or v_sess.lease_expires_at <= now() then
    update public.agent_messages
       set execution_status = case when v_root.execution_status = 'running' then 'interrupted' else 'cancelled' end,
           finished_at = now()
     where session_id = p_session_id and message_id = v_target
     returning * into v_root;
    update public.agent_sessions
       set active_request_message_id = null,
           lease_owner = null,
           lease_expires_at = null,
           revision = revision + 1,
           last_active = now()
     where id = p_session_id
     returning * into v_sess;
  else
    update public.agent_sessions
       set revision = revision + 1
     where id = p_session_id
     returning * into v_sess;
  end if;

  return jsonb_build_object('request_message', session_message_json(v_root), 'deduplicated', false);
end;
$$;

-- session_publish_checkpoint activates a compaction checkpoint atomically. It
-- refuses to publish for a request that already finished or was cancelled, so
-- compaction can never revive inference after cancellation.
create or replace function public.session_publish_checkpoint(
  p_session_id        text,
  p_mutation_id       text,
  p_mutation_hash     text,
  p_expected_revision bigint,
  p_lease_generation  bigint,
  p_checkpoint        jsonb
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_sess    public.agent_sessions;
  v_root    public.agent_messages;
  v_receipt public.agent_mutation_receipts;
  v_cfg     public.agent_session_config;
  v_cp      public.agent_context_checkpoints;
  v_result  jsonb;
  v_id      text;
  v_parent  text;
  v_covered bigint;
  v_head    bigint;
  v_rev     bigint;
  v_active  text;
  v_resume  bigint;
  v_summary jsonb;
  v_fmt     integer;
begin
  if coalesce(p_mutation_id, '') = '' or coalesce(p_mutation_hash, '') = '' then
    perform session_fail('invalid_argument', 'mutation_id and mutation_hash are required');
  end if;
  if p_checkpoint is null then
    perform session_fail('invalid_argument', 'checkpoint is required');
  end if;

  select * into v_sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform session_fail('not_found', 'session not found');
  end if;

  -- Receipt first: a replayed publication returns the stored result before the
  -- (now unnecessary) payload is validated again.
  select * into v_receipt from public.agent_mutation_receipts
   where session_id = p_session_id and mutation_id = p_mutation_id;
  if found then
    if v_receipt.mutation_hash = p_mutation_hash then
      return v_receipt.result || jsonb_build_object('deduplicated', true);
    end if;
    perform session_fail('mutation_conflict', 'mutation_id was reused with a different payload');
  end if;

  v_id      := p_checkpoint->>'id';
  v_parent  := nullif(p_checkpoint->>'parent_checkpoint_id', '');
  v_covered := coalesce((p_checkpoint->>'covered_through_seq')::bigint, -1);
  v_head    := coalesce((p_checkpoint->>'source_head_seq')::bigint, -1);
  v_rev     := coalesce((p_checkpoint->>'source_revision')::bigint, -1);
  v_active  := nullif(p_checkpoint->>'active_request_message_id', '');
  v_resume  := coalesce((p_checkpoint->>'resume_after_seq')::bigint, -1);
  v_summary := p_checkpoint->'summary';
  v_fmt     := coalesce((p_checkpoint->>'format_version')::integer, 0);

  if coalesce(v_id, '') = '' then
    perform session_fail('invalid_argument', 'checkpoint id is required');
  end if;
  if v_fmt <= 0 then
    perform session_fail('invalid_argument', 'checkpoint format_version must be positive');
  end if;
  if v_summary is null or jsonb_typeof(v_summary) <> 'array' or jsonb_array_length(v_summary) = 0 then
    perform session_fail('invalid_argument', 'checkpoint summary must be a non-empty JSON array');
  end if;

  if p_lease_generation is null or p_lease_generation <= 0
     or v_sess.lease_owner is null or v_sess.lease_generation <> p_lease_generation then
    perform session_fail('stale_lease', 'a current lease generation is required');
  end if;
  if v_sess.lease_expires_at <= now() then
    perform session_fail('lease_expired', 'lease has expired');
  end if;

  if v_sess.active_request_message_id is null then
    perform session_fail('request_not_active', 'session has no active request');
  end if;
  select * into v_root from public.agent_messages
   where session_id = p_session_id and message_id = v_sess.active_request_message_id;
  if not found then
    perform session_fail('not_found', 'request message not found');
  end if;
  if v_root.execution_status <> 'running' or v_root.cancellation_requested then
    perform session_fail('request_not_active', 'active request is not running');
  end if;
  if v_active is distinct from v_sess.active_request_message_id then
    perform session_fail('request_not_active', 'checkpoint active_request_message_id does not match the active request');
  end if;

  if v_sess.active_checkpoint_id is distinct from v_parent then
    perform session_fail('parent_conflict', 'checkpoint parent is not the active checkpoint');
  end if;
  if v_covered < 0 or v_head < v_covered or v_resume < v_covered then
    perform session_fail('invalid_argument', 'checkpoint sequence bounds are invalid');
  end if;
  if v_head > v_sess.last_seq then
    perform session_fail('invalid_argument', 'source_head_seq is beyond the allocated session sequence');
  end if;

  select * into v_cfg from public.agent_session_config where session_id = p_session_id;
  if not found then
    perform session_fail('not_found', 'session configuration not found');
  end if;
  if v_cfg.config_hash <> coalesce(p_checkpoint->>'config_hash', '') then
    perform session_fail('config_mismatch', 'checkpoint config_hash does not match the frozen session configuration');
  end if;

  if coalesce(p_expected_revision, 0) > 0 and v_sess.revision <> p_expected_revision then
    perform session_fail('revision_conflict',
      format('revision is %s, expected %s', v_sess.revision, p_expected_revision));
  end if;

  insert into public.agent_context_checkpoints (
    session_id, id, parent_checkpoint_id, covered_through_seq, source_head_seq, source_revision,
    config_hash, active_request_message_id, resume_after_seq, summary, format_version,
    prompt_version, summarizer_provider, summarizer_model,
    estimated_tokens_before, estimated_tokens_after, generation_usage
  ) values (
    p_session_id, v_id, v_parent, v_covered, v_head, v_rev,
    v_cfg.config_hash, v_active, v_resume, v_summary, v_fmt,
    coalesce(p_checkpoint->>'prompt_version', ''), coalesce(p_checkpoint->>'summarizer_provider', ''),
    coalesce(p_checkpoint->>'summarizer_model', ''),
    coalesce((p_checkpoint->>'estimated_tokens_before')::bigint, 0),
    coalesce((p_checkpoint->>'estimated_tokens_after')::bigint, 0),
    p_checkpoint->'generation_usage'
  ) returning * into v_cp;

  update public.agent_sessions
     set active_checkpoint_id = v_id, revision = revision + 1
   where id = p_session_id
   returning * into v_sess;

  v_result := jsonb_build_object('session', session_json(v_sess), 'checkpoint', session_checkpoint_json(v_cp));
  insert into public.agent_mutation_receipts (session_id, mutation_id, mutation_hash, result)
  values (p_session_id, p_mutation_id, p_mutation_hash, v_result);

  return v_result || jsonb_build_object('deduplicated', false);
end;
$$;

-- ---------------------------------------------------------------- grants ----
-- These are service-only operations: revoke the implicit PUBLIC execute grant
-- (and the Supabase API roles) and hand execute to service_role alone.
do $$
declare
  v_sig  regprocedure;
  v_role text;
begin
  for v_sig in
    select p.oid::regprocedure
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
     where n.nspname = 'public' and p.proname like 'session\_%'
  loop
    execute format('revoke all on function %s from public', v_sig);
    foreach v_role in array array['anon', 'authenticated', 'service_role'] loop
      if exists (select 1 from pg_roles where rolname = v_role) then
        execute format('revoke all on function %s from %I', v_sig, v_role);
      end if;
    end loop;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
      execute format('grant execute on function %s to service_role', v_sig);
    end if;
  end loop;
end;
$$;
