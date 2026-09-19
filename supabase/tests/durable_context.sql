-- Durable context verification suite.
--
-- Run against a scratch database with all migrations applied. Every durable
-- transition is exercised through the same /rpc entry points session-manager
-- uses, and constraint/index/RLS/privilege behavior is asserted directly. The
-- whole suite runs in one transaction and rolls back, so it can be re-run.
--
--   psql -v ON_ERROR_STOP=1 -d <scratch> -f supabase/tests/durable_context.sql
--
-- scripts/verify-db.sh creates the scratch database and applies migrations.

\set ON_ERROR_STOP on

begin;

-- Foreign keys are deferred in production so a root may reference itself within
-- the admitting statement; the suite flips them to immediate so violation
-- assertions fire at the offending statement instead of at commit.
set constraints all immediate;

-- ------------------------------------------------------------- test helpers ---

create function pg_temp.expect(cond boolean, msg text) returns void
language plpgsql as $$
begin
  if cond is not true then
    raise exception 'FAIL: %', msg;
  end if;
end;
$$;

-- expect_fail runs `stmt` and asserts it aborts. A non-null want_detail must
-- match session_fail's machine-readable DETAIL exactly; NULL accepts any
-- failure (constraint, trigger, or domain reason).
create function pg_temp.expect_fail(stmt text, want_detail text) returns void
language plpgsql as $$
declare
  v_detail text;
  v_state  text;
begin
  begin
    execute stmt;
  exception when others then
    get stacked diagnostics v_detail = pg_exception_detail, v_state = returned_sqlstate;
    -- want_detail = NULL means "any failure"; a named reason must match
    -- session_fail's machine-readable DETAIL.
    if want_detail is not null and nullif(v_detail, '') is distinct from want_detail then
      raise exception 'FAIL: expected reason [%], got [%] (sqlstate %, %)',
        want_detail, v_detail, v_state, sqlerrm;
    end if;
    return;
  end;
  raise exception 'FAIL: expected failure [%] but the statement succeeded', want_detail;
end;
$$;

create function pg_temp.cfg() returns jsonb language sql immutable as $$
  select '{
    "format_version": 1,
    "config_hash": "cfg-hash-1",
    "system_prompt": "you are a test assistant",
    "provider": "openai",
    "model": "gpt-4o-mini",
    "base_url": "https://llm.example.com/v1",
    "credential_ref": "cred-ref-1",
    "context_window_tokens": 128000,
    "output_limit_tokens": 4096,
    "tools": [{"name": "bash", "version": "1"}]
  }'::jsonb;
$$;

create function pg_temp.u1() returns text language sql immutable as $$ select '11111111-1111-1111-1111-111111111111'::text $$;
create function pg_temp.u2() returns text language sql immutable as $$ select '22222222-2222-2222-2222-222222222222'::text $$;

-- ------------------------------------------------- 1. schema and constraints ---

do $$
declare
  v_pol int;
begin
  -- RLS on for every service-owned table, and no policies: the service role
  -- (which bypasses RLS) is the only way in.
  select count(*) into v_pol
    from pg_policies
   where schemaname = 'public'
     and tablename in ('agent_sessions', 'agent_messages', 'agent_session_config',
                       'agent_context_checkpoints', 'agent_mutation_receipts');
  perform pg_temp.expect(v_pol = 0, 'no anon/authenticated policies on service tables');

  perform pg_temp.expect(
    (select bool_and(relrowsecurity) from pg_class
      where relname in ('agent_sessions', 'agent_messages', 'agent_session_config',
                        'agent_context_checkpoints', 'agent_mutation_receipts')),
    'row level security enabled on every service table');

  -- The functions are service-only.
  perform pg_temp.expect(
    not has_function_privilege('public', 'public.session_begin_request(text,text,text,text,text,jsonb,integer)', 'execute'),
    'session_begin_request not executable by public');
  perform pg_temp.expect(
    not exists (
      select 1 from pg_roles r
       where r.rolname in ('anon', 'authenticated')
         and has_function_privilege(r.rolname, 'public.session_append_messages(text,text,text,text,bigint,bigint,jsonb)', 'execute')),
    'session_append_messages not executable by anon/authenticated');
  perform pg_temp.expect(
    coalesce((select has_function_privilege('service_role', 'public.session_append_messages(text,text,text,text,bigint,bigint,jsonb)', 'execute')), true),
    'session_append_messages executable by service_role');

  -- Required indexes for latest-window reads, request history, deduplication,
  -- and one nonterminal request per session.
  perform pg_temp.expect(
    (select count(*) from pg_indexes where schemaname = 'public'
      and indexname in ('agent_messages_session_seq_desc', 'agent_messages_request_seq',
                        'agent_messages_client_request_unique', 'agent_messages_one_active_request')) = 4,
    'canonical message indexes exist');
end;
$$;

-- --------------------------------------------------- 2. create and admission ---

do $$
declare
  v_res   jsonb;
  v_sess  jsonb;
  v_beg   jsonb;
  v_last  bigint;
  v_rev   bigint;
begin
  perform pg_temp.expect_fail(
    format('select session_create_session(''sess-bad'', ''not-a-uuid'', null, %L::jsonb)', pg_temp.cfg()),
    'invalid_argument');

  v_res := session_create_session('sess-t1', pg_temp.u1(), 'agent-1', pg_temp.cfg());
  v_sess := v_res->'session';
  perform pg_temp.expect(v_sess->>'id' = 'sess-t1', 'session created with its id');
  perform pg_temp.expect((v_sess->>'last_seq')::bigint = 0, 'fresh session last_seq is 0');
  perform pg_temp.expect((v_sess->>'revision')::bigint = 0, 'fresh session revision is 0');
  perform pg_temp.expect(v_res->'config'->>'config_hash' = 'cfg-hash-1', 'frozen config stored');

  -- The frozen configuration is immutable.
  perform pg_temp.expect_fail(
    $q$update agent_session_config set model = 'other' where session_id = 'sess-t1'$q$,
    null);

  -- A request from a different user is rejected.
  perform pg_temp.expect_fail(
    $q$select session_begin_request('sess-t1', '22222222-2222-2222-2222-222222222222',
        'msg-r1', 'req-1', 'hash-1', '[{"type":"text","text":"hi"}]'::jsonb, 1)$q$,
    'permission_denied');

  v_beg := session_begin_request('sess-t1', pg_temp.u1(), 'msg-r1', 'req-1', 'hash-1',
                                 '[{"type":"text","text":"hi"}]'::jsonb, 1);
  perform pg_temp.expect((v_beg->>'deduplicated')::boolean is false, 'first request is not a duplicate');
  perform pg_temp.expect(v_beg->'request_message'->>'execution_status' = 'queued', 'new root is queued');
  perform pg_temp.expect(v_beg->'request_message'->>'request_message_id' = 'msg-r1', 'root references itself');
  perform pg_temp.expect((v_beg->'request_message'->>'seq')::bigint = 1, 'root takes seq 1');
  perform pg_temp.expect(v_beg->'session'->>'active_request_message_id' = 'msg-r1', 'active request set');
  perform pg_temp.expect((v_beg->'session'->>'revision')::bigint = 1, 'admission advances revision');

  -- Same id + same hash returns the existing root without allocating a sequence.
  v_beg := session_begin_request('sess-t1', pg_temp.u1(), 'msg-r1-ignored', 'req-1', 'hash-1',
                                 '[{"type":"text","text":"hi"}]'::jsonb, 1);
  perform pg_temp.expect((v_beg->>'deduplicated')::boolean, 'replayed request is deduplicated');
  perform pg_temp.expect(v_beg->'request_message'->>'id' = 'msg-r1', 'replay returns the original root');
  select last_seq, revision into v_last, v_rev from agent_sessions where id = 'sess-t1';
  perform pg_temp.expect(v_last = 1, 'replay does not allocate a sequence');
  perform pg_temp.expect(v_rev = 1, 'replay does not advance revision');

  -- Same id + different content is a conflict.
  perform pg_temp.expect_fail(
    $q$select session_begin_request('sess-t1', '11111111-1111-1111-1111-111111111111',
        'msg-r1b', 'req-1', 'hash-different', '[{"type":"text","text":"changed"}]'::jsonb, 1)$q$,
    'hash_conflict');

  -- A second concurrent request is rejected while one is active.
  perform pg_temp.expect_fail(
    $q$select session_begin_request('sess-t1', '11111111-1111-1111-1111-111111111111',
        'msg-r2', 'req-2', 'hash-2', '[{"type":"text","text":"again"}]'::jsonb, 1)$q$,
    'request_busy');
end;
$$;

-- --------------------------------------------------------- 3. lease fencing ---

do $$
declare
  v_acq   jsonb;
  v_gen   bigint;
  v_lease jsonb;
begin
  v_acq := session_acquire_lease('sess-t1', 'msg-r1', 'runtime-a', 90);
  v_gen := (v_acq->'lease'->>'generation')::bigint;
  perform pg_temp.expect(v_gen = 1, 'first lease generation is 1');
  perform pg_temp.expect(v_acq->'session'->>'active_request_message_id' = 'msg-r1', 'lease keeps the active request');
  perform pg_temp.expect(
    (select execution_status from agent_messages where session_id = 'sess-t1' and message_id = 'msg-r1') = 'running',
    'acquiring a lease moves the request to running');
  perform pg_temp.expect(
    (select started_at is not null from agent_messages where session_id = 'sess-t1' and message_id = 'msg-r1'),
    'acquiring a lease stamps started_at');

  -- Another runtime cannot take a live lease.
  perform pg_temp.expect_fail(
    $q$select session_acquire_lease('sess-t1', 'msg-r1', 'runtime-b', 90)$q$,
    'lease_conflict');

  -- Renewal returns a fresh expiry and does not advance semantic revision.
  v_lease := session_renew_lease('sess-t1', 'runtime-a', v_gen, 90);
  perform pg_temp.expect(v_lease->'lease'->>'owner' = 'runtime-a', 'renewal keeps the owner');
  perform pg_temp.expect(
    (select revision from agent_sessions where id = 'sess-t1') = 2,
    'renewal does not advance revision');

  -- A wrong generation is stale.
  perform pg_temp.expect_fail(
    format('select session_renew_lease(''sess-t1'', ''runtime-a'', %s, 90)', v_gen + 5),
    'stale_lease');
  -- Releasing a lease another owner holds is a documented no-op, not an error.
  perform pg_temp.expect(
    (session_release_lease('sess-t1', 'runtime-b', 1)->>'released')::boolean is false,
    'releasing another owner''s lease is a no-op');
end;
$$;

-- ------------------------------------------- 4. incremental persistence + CAS ---

do $$
declare
  v_res jsonb;
  v_msgs jsonb;
  v_bad_mutation jsonb;
  v_gen bigint;
  v_rev bigint;
begin
  select lease_generation, revision into v_gen, v_rev from agent_sessions where id = 'sess-t1';

  v_res := session_append_messages(
    'sess-t1', 'msg-r1', 'mut-1', 'mut-hash-1', v_rev, v_gen,
    '[{"id":"msg-a1","role":"assistant","format_version":1,"message_status":"complete",
       "content":[{"type":"text","text":"running pwd"},{"type":"tool_call","id":"call_1","name":"bash","arguments_json":"{\"command\":\"pwd\"}"}]}]'::jsonb);
  v_msgs := v_res->'messages';
  perform pg_temp.expect((v_res->>'deduplicated')::boolean is false, 'append commits');
  perform pg_temp.expect(jsonb_array_length(v_msgs) = 1, 'one message inserted');
  perform pg_temp.expect((v_msgs->0->>'seq')::bigint = 2, 'append takes the next sequence');
  perform pg_temp.expect(v_msgs->0->>'role' = 'assistant', 'assistant message stored');
  perform pg_temp.expect(v_msgs->0->>'request_message_id' = 'msg-r1', 'message references the request root');
  perform pg_temp.expect((v_res->'session'->>'last_seq')::bigint = 2, 'last_seq advanced');

  -- Idempotent replay: same mutation id and hash returns the stored result.
  v_res := session_append_messages(
    'sess-t1', 'msg-r1', 'mut-1', 'mut-hash-1', v_rev, v_gen, '[]'::jsonb);
  -- NOTE: the replay must not require the messages payload to be non-empty once
  -- the receipt exists, so the stored result is returned unchanged.
  perform pg_temp.expect((v_res->>'deduplicated')::boolean, 'replay is deduplicated');
  perform pg_temp.expect((v_res->'session'->>'last_seq')::bigint = 2, 'replay does not allocate sequences');
  perform pg_temp.expect(
    (select count(*) from agent_messages where session_id = 'sess-t1') = 2,
    'replay does not duplicate messages');

  -- Same mutation id with a different payload is a conflict.
  perform pg_temp.expect_fail(
    format($q$select session_append_messages('sess-t1','msg-r1','mut-1','mut-hash-other',%s,%s,
        '[{"id":"msg-x","role":"tool","content":[{"type":"tool_result","tool_call_id":"call_1","status":"success","content":[{"type":"text","text":"x"}]}]}]'::jsonb)$q$, v_rev, v_gen),
    'mutation_conflict');

  -- A stale lease generation cannot write.
  perform pg_temp.expect_fail(
    format($q$select session_append_messages('sess-t1','msg-r1','mut-2','mut-hash-2',0,%s,
        '[{"id":"msg-a2","role":"assistant","content":[{"type":"text","text":"x"}]}]'::jsonb)$q$, v_gen + 9),
    'stale_lease');

  -- A stale expected revision cannot write.
  perform pg_temp.expect_fail(
    format($q$select session_append_messages('sess-t1','msg-r1','mut-3','mut-hash-3',%s,%s,
        '[{"id":"msg-a3","role":"assistant","content":[{"type":"text","text":"x"}]}]'::jsonb)$q$, v_rev + 100, v_gen),
    'revision_conflict');

  -- Tool results link to their call by id.
  v_res := session_append_messages(
    'sess-t1', 'msg-r1', 'mut-4', 'mut-hash-4', 0, v_gen,
    '[{"id":"msg-t1","role":"tool","format_version":1,"message_status":"complete",
       "reply_to_message_id":"msg-a1",
       "content":[{"type":"tool_result","tool_call_id":"call_1","status":"success","content":[{"type":"text","text":"/workspace"}]}]}]'::jsonb);
  perform pg_temp.expect(v_res->'messages'->0->>'reply_to_message_id' = 'msg-a1', 'tool result keeps its causal link');
  perform pg_temp.expect((v_res->'messages'->0->>'seq')::bigint = 3, 'tool result takes seq 3');
end;
$$;

-- ---------------------------------------------------- 5. finish and terminality ---

do $$
declare
  v_gen bigint;
  v_fin jsonb;
begin
  select lease_generation into v_gen from agent_sessions where id = 'sess-t1';

  v_fin := session_finish_request(
    'sess-t1', 'msg-r1', 'mut-5', 'mut-hash-5', 0, v_gen, 'completed',
    '[{"id":"msg-a2","role":"assistant","format_version":1,"message_status":"complete",
       "content":[{"type":"text","text":"done"}]}]'::jsonb, null);
  perform pg_temp.expect(v_fin->'request_message'->>'execution_status' = 'completed', 'root completes');
  perform pg_temp.expect(v_fin->'request_message'->>'finished_at' is not null, 'finish stamps finished_at');
  perform pg_temp.expect(v_fin->'session'->>'active_request_message_id' is null, 'finish clears the active request');
  perform pg_temp.expect(v_fin->'session'->>'lease_owner' is null, 'finish releases the lease');
  perform pg_temp.expect((v_fin->'session'->>'last_seq')::bigint = 4, 'final output takes the next sequence');

  -- Terminal requests reject further appends.
  perform pg_temp.expect_fail(
    $q$select session_append_messages('sess-t1','msg-r1','mut-6','mut-hash-6',0,0,
        '[{"id":"msg-late","role":"assistant","content":[{"type":"text","text":"late"}]}]'::jsonb)$q$,
    'stale_lease');

  -- Completed content is immutable.
  perform pg_temp.expect_fail(
    $q$update agent_messages set content = '[{"type":"text","text":"tampered"}]'::jsonb
        where session_id = 'sess-t1' and message_id = 'msg-a1'$q$,
    null);

  -- Retry after failure is a new request root; the client can reopen the session.
  perform session_begin_request('sess-t1', pg_temp.u1(), 'msg-r2', 'req-2', 'hash-2',
                                '[{"type":"text","text":"retry"}]'::jsonb, 1);
  perform pg_temp.expect(
    (select active_request_message_id from agent_sessions where id = 'sess-t1') = 'msg-r2',
    'a finished session admits a new request');
end;
$$;

-- --------------------------------------------------------------- 6. cancellation ---

do $$
declare
  v_gen bigint;
  v_can jsonb;
begin
  -- Cancel a queued request: it has no executor, so it terminates immediately.
  v_can := session_cancel_request('sess-t1', pg_temp.u1(), 'msg-r2');
  perform pg_temp.expect(v_can->'request_message'->>'execution_status' = 'cancelled', 'queued cancel terminates');
  perform pg_temp.expect((v_can->'request_message'->>'cancellation_requested')::boolean, 'cancel flag recorded');
  perform pg_temp.expect(
    (select active_request_message_id is null from agent_sessions where id = 'sess-t1'),
    'queued cancel clears the active request');

  -- Cancelling with the wrong owner is denied.
  perform pg_temp.expect_fail(
    $q$select session_cancel_request('sess-t1', '22222222-2222-2222-2222-222222222222', null)$q$,
    'permission_denied');

  -- A running request with a live lease is only flagged; the runtime finishes it.
  perform session_begin_request('sess-t1', pg_temp.u1(), 'msg-r3', 'req-3', 'hash-3',
                                '[{"type":"text","text":"long task"}]'::jsonb, 1);
  select lease_generation into v_gen from agent_sessions where id = 'sess-t1';
  perform session_acquire_lease('sess-t1', 'msg-r3', 'runtime-a', 90);
  select lease_generation into v_gen from agent_sessions where id = 'sess-t1';

  v_can := session_cancel_request('sess-t1', pg_temp.u1(), null);
  perform pg_temp.expect((v_can->'request_message'->>'cancellation_requested')::boolean, 'cancel flags the live request');
  perform pg_temp.expect(v_can->'request_message'->>'execution_status' = 'running', 'live request keeps running until the runtime finishes');

  -- Once cancellation is requested, further appends are refused.
  perform pg_temp.expect_fail(
    format($q$select session_append_messages('sess-t1','msg-r3','mut-7','mut-hash-7',0,%s,
        '[{"id":"msg-c1","role":"assistant","content":[{"type":"text","text":"more"}]}]'::jsonb)$q$, v_gen),
    'request_cancelled');

  -- And a cancelled request can never publish a compaction checkpoint.
  perform pg_temp.expect_fail(
    format($q$select session_publish_checkpoint('sess-t1','mut-8','mut-hash-8',0,%s,
      '{"id":"cp-x","covered_through_seq":1,"source_head_seq":4,"source_revision":1,
        "config_hash":"cfg-hash-1","active_request_message_id":"msg-r3","resume_after_seq":4,
        "summary":[{"type":"text","text":"s"}],"format_version":1}'::jsonb)$q$, v_gen),
    'request_not_active');

  -- The runtime finishes the cancelled request with its partial output.
  perform session_finish_request('sess-t1', 'msg-r3', 'mut-9', 'mut-hash-9', 0, v_gen, 'cancelled',
    '[{"id":"msg-partial","role":"assistant","format_version":1,"message_status":"partial",
       "content":[{"type":"text","text":"half a sen"}]}]'::jsonb, 'cancelled_by_user');
  perform pg_temp.expect(
    (select message_status from agent_messages where session_id = 'sess-t1' and message_id = 'msg-partial') = 'partial',
    'partial output is retained with a partial status');
  perform pg_temp.expect(
    (select error_code from agent_messages where session_id = 'sess-t1' and message_id = 'msg-r3') = 'cancelled_by_user',
    'cancellation reason recorded on the root');
end;
$$;

-- ------------------------------------------------------- 7. lease expiry takeover ---

do $$
declare
  v_acq  jsonb;
  v_gen  bigint;
  v_prev bigint;
begin
  perform session_begin_request('sess-t1', pg_temp.u1(), 'msg-r4', 'req-4', 'hash-4',
                                '[{"type":"text","text":"takeover"}]'::jsonb, 1);
  perform session_acquire_lease('sess-t1', 'msg-r4', 'runtime-a', 90);
  select lease_generation into v_prev from agent_sessions where id = 'sess-t1';

  -- Simulate expiry (database time is authoritative).
  update agent_sessions set lease_expires_at = now() - interval '1 second' where id = 'sess-t1';

  perform pg_temp.expect_fail(
    $q$select session_append_messages('sess-t1','msg-r4','mut-10','mut-hash-10',0,
        (select lease_generation from agent_sessions where id='sess-t1'),
        '[{"id":"msg-z","role":"assistant","content":[{"type":"text","text":"z"}]}]'::jsonb)$q$,
    'lease_expired');

  v_acq := session_acquire_lease('sess-t1', 'msg-r4', 'runtime-b', 90);
  v_gen := (v_acq->'lease'->>'generation')::bigint;
  perform pg_temp.expect(v_gen = v_prev + 1, 'takeover bumps the lease generation');
  perform pg_temp.expect(v_acq->'lease'->>'owner' = 'runtime-b', 'takeover changes the owner');

  -- The old owner's generation is fenced out.
  perform pg_temp.expect_fail(
    format($q$select session_append_messages('sess-t1','msg-r4','mut-11','mut-hash-11',0,%s,
        '[{"type":"text","text":"z"}]'::jsonb)$q$, v_gen - 1),
    'stale_lease');
end;
$$;

-- --------------------------------------------------- 8. checkpoint publication ---

do $$
declare
  v_gen bigint;
  v_pub jsonb;
  v_cp  jsonb;
begin
  select lease_generation into v_gen from agent_sessions where id = 'sess-t1';

  v_pub := session_publish_checkpoint('sess-t1', 'mut-12', 'mut-hash-12', 0, v_gen,
    '{"id":"cp-1","covered_through_seq":4,"source_head_seq":7,"source_revision":6,
      "config_hash":"cfg-hash-1","active_request_message_id":"msg-r4","resume_after_seq":7,
      "summary":[{"type":"text","text":"the user asked for takeover"}],"format_version":1,
      "prompt_version":"v1","summarizer_provider":"openai","summarizer_model":"gpt-4o-mini",
      "estimated_tokens_before":9000,"estimated_tokens_after":1200,
      "generation_usage":{"input_tokens":9000,"output_tokens":300}}'::jsonb);
  v_cp := v_pub->'checkpoint';
  perform pg_temp.expect(v_cp->>'id' = 'cp-1', 'checkpoint persisted');
  perform pg_temp.expect(v_pub->'session'->>'active_checkpoint_id' = 'cp-1', 'active checkpoint pointer moved');
  perform pg_temp.expect(v_cp->'summary'->0->>'text' = 'the user asked for takeover', 'summary blocks preserved');

  -- A checkpoint whose parent is not the active checkpoint is rejected.
  perform pg_temp.expect_fail(
    format($q$select session_publish_checkpoint('sess-t1','mut-13','mut-hash-13',0,%s,
      '{"id":"cp-2","parent_checkpoint_id":"cp-bogus","covered_through_seq":7,"source_head_seq":7,
        "source_revision":7,"config_hash":"cfg-hash-1","active_request_message_id":"msg-r4",
        "resume_after_seq":7,"summary":[{"type":"text","text":"s"}],"format_version":1}'::jsonb)$q$, v_gen),
    'parent_conflict');

  -- A config hash that does not match the frozen session config is rejected.
  perform pg_temp.expect_fail(
    format($q$select session_publish_checkpoint('sess-t1','mut-14','mut-hash-14',0,%s,
      '{"id":"cp-3","parent_checkpoint_id":"cp-1","covered_through_seq":7,"source_head_seq":7,
        "source_revision":7,"config_hash":"wrong-hash","active_request_message_id":"msg-r4",
        "resume_after_seq":7,"summary":[{"type":"text","text":"s"}],"format_version":1}'::jsonb)$q$, v_gen),
    'config_mismatch');

  -- Replaying the same mutation returns the stored result.
  v_pub := session_publish_checkpoint('sess-t1', 'mut-12', 'mut-hash-12', 0, v_gen, '{}'::jsonb);
  perform pg_temp.expect((v_pub->>'deduplicated')::boolean, 'checkpoint replay is deduplicated');
  perform pg_temp.expect(
    (select count(*) from agent_context_checkpoints where session_id = 'sess-t1') = 1,
    'checkpoint replay does not duplicate rows');
end;
$$;

-- ------------------------------------------------------------ 9. end lifecycle ---

do $$
begin
  update agent_sessions set status = 'ended', ended_at = now() where id = 'sess-t1';
  perform pg_temp.expect_fail(
    $q$select session_begin_request('sess-t1','11111111-1111-1111-1111-111111111111',
        'msg-r5','req-5','hash-5','[{"type":"text","text":"after end"}]'::jsonb,1)$q$,
    'session_ended');
  -- History stays readable after end: root, assistant, tool, final, cancelled,
  -- partial and takeover messages are all retained.
  perform pg_temp.expect(
    (select count(*) from agent_messages where session_id = 'sess-t1') = 8,
    'transcript is retained after the session ends');
end;
$$;

-- ------------------------------------------------- 10. root-only request shape ---

do $$
begin
  -- Request lifecycle fields are legal only on a self-referencing user root.
  perform pg_temp.expect_fail(
    $q$insert into agent_messages (session_id, seq, role, content, message_id, request_message_id,
         format_version, message_status, client_request_id, request_hash, execution_status)
       values ('sess-t1', 100, 'assistant', '[]'::jsonb, 'msg-bad', 'msg-r1', 1, 'complete',
               'req-bad', 'hash-bad', 'running')$q$,
    null);

  -- A request root in a different session is rejected by the composite FK.
  perform pg_temp.expect_fail(
    $q$insert into agent_messages (session_id, seq, role, content, message_id, request_message_id,
         format_version, message_status)
       values ('sess-t1', 101, 'assistant', '[]'::jsonb, 'msg-bad2', 'msg-in-other-session', 1, 'complete')$q$,
    null);

  -- Only one nonterminal request root may exist per session.
  perform pg_temp.expect_fail(
    $q$insert into agent_messages (session_id, seq, role, content, message_id, request_message_id,
         format_version, message_status, client_request_id, request_hash, execution_status)
       values ('sess-t1', 102, 'user', '[]'::jsonb, 'msg-bad3', 'msg-bad3', 1, 'complete',
               'req-bad3', 'hash-bad3', 'running')$q$,
    null);
end;
$$;

select 'durable_context: all assertions passed' as result;

rollback;
