-- Verify workspace revision CAS, idempotency, purge intent and privileges.
do $$
declare
  result jsonb;
  detail text;
begin
  insert into public.agent_sessions (id, user_id, status)
  values ('sess-workspace-test', '33333333-3333-3333-3333-333333333333', 'active');

  perform public.ensure_session_workspace('sess-workspace-test');
  if (select revision from public.session_workspaces where session_id = 'sess-workspace-test') <> 0 then
    raise exception 'workspace reservation did not start at revision zero';
  end if;

  result := public.commit_session_workspace('sess-workspace-test', 0,
    'sessions/sess-workspace-test/revisions/one.tar.gz', repeat('a', 64), 42, 'mutation-one');
  if (result #>> '{workspace,revision}')::bigint <> 1 or (result ->> 'deduplicated')::boolean then
    raise exception 'initial workspace commit failed: %', result;
  end if;

  result := public.commit_session_workspace('sess-workspace-test', 0,
    'sessions/sess-workspace-test/revisions/one.tar.gz', repeat('a', 64), 42, 'mutation-one');
  if not (result ->> 'deduplicated')::boolean then
    raise exception 'identical retry was not deduplicated';
  end if;

  begin
    perform public.commit_session_workspace('sess-workspace-test', 0,
      'sessions/sess-workspace-test/revisions/two.tar.gz', repeat('b', 64), 43, 'mutation-two');
    raise exception 'stale workspace revision was accepted';
  exception when sqlstate 'P0001' then
    get stacked diagnostics detail = PG_EXCEPTION_DETAIL;
    if detail <> 'aborted' then
      raise;
    end if;
  end;

  delete from public.agent_sessions where id = 'sess-workspace-test';
  if not exists (select 1 from public.session_workspaces
                 where session_id = 'sess-workspace-test' and deletion_requested_at is not null) then
    raise exception 'purge did not preserve workspace deletion intent';
  end if;

  if has_table_privilege('anon', 'public.session_workspaces', 'SELECT') or
     has_table_privilege('authenticated', 'public.session_workspaces', 'SELECT') or
     has_function_privilege('anon', 'public.commit_session_workspace(text,bigint,text,text,bigint,text)', 'EXECUTE') then
    raise exception 'workspace privileges are too broad';
  end if;
end $$;
