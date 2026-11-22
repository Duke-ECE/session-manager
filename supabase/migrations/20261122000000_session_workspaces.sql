-- Session-owned archive pointers. Archive bytes stay in R2; only committed
-- metadata is stored here. No foreign key so a deletion intent can outlive a
-- purged conversation until object cleanup succeeds.
create table public.session_workspaces (
  session_id text primary key,
  user_id uuid not null,
  revision bigint not null default 0 check (revision >= 0),
  object_key text not null default '',
  sha256 text not null default '',
  size_bytes bigint not null default 0 check (size_bytes >= 0 and size_bytes <= 1073741824),
  mutation_id text not null default '',
  committed_at timestamptz,
  deletion_requested_at timestamptz
);

create index session_workspaces_deletion_idx on public.session_workspaces (deletion_requested_at)
  where deletion_requested_at is not null;

alter table public.session_workspaces enable row level security;
revoke all on public.session_workspaces from anon, authenticated;
grant select, insert, update, delete on public.session_workspaces to service_role;

-- Reserve a cleanup row before a sandbox receives any signed URL. Even a
-- failed first upload can then be removed after the session is purged.
create function public.ensure_session_workspace(p_session_id text)
returns void
language plpgsql
set search_path = public, pg_temp
as $$
declare
  sess public.agent_sessions%rowtype;
begin
  select * into sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform public.session_fail('not_found', 'session not found');
  end if;
  if sess.status <> 'active' then
    perform public.session_fail('failed_precondition', 'session is ended');
  end if;
  insert into public.session_workspaces (session_id, user_id)
    values (p_session_id, sess.user_id)
    on conflict (session_id) do nothing;
end;
$$;

revoke all on function public.ensure_session_workspace(text) from public, anon, authenticated;
grant execute on function public.ensure_session_workspace(text) to service_role;

-- Lock the parent session first so two initial commits cannot create distinct
-- revision-one rows. Compare-and-swap and idempotency happen in one transaction.
create function public.commit_session_workspace(
  p_session_id text, p_expected_revision bigint, p_object_key text,
  p_sha256 text, p_size_bytes bigint, p_mutation_id text
) returns jsonb
language plpgsql
set search_path = public, pg_temp
as $$
declare
  sess public.agent_sessions%rowtype;
  ws public.session_workspaces%rowtype;
  was_deduplicated boolean := false;
begin
  select * into sess from public.agent_sessions where id = p_session_id for update;
  if not found then
    perform public.session_fail('not_found', 'session not found');
  end if;

  insert into public.session_workspaces (session_id, user_id)
    values (p_session_id, sess.user_id)
    on conflict (session_id) do nothing;
  select * into ws from public.session_workspaces where session_id = p_session_id for update;

  if ws.mutation_id = p_mutation_id and p_mutation_id <> '' then
    if ws.revision <> p_expected_revision + 1 or ws.object_key <> p_object_key
       or ws.sha256 <> p_sha256 or ws.size_bytes <> p_size_bytes then
      perform public.session_fail('failed_precondition', 'mutation id reused with different content');
    end if;
    was_deduplicated := true;
  else
    if sess.status <> 'active' or ws.deletion_requested_at is not null then
      perform public.session_fail('failed_precondition', 'session is ended or deleting');
    end if;
    if ws.revision <> p_expected_revision then
      perform public.session_fail('aborted', 'workspace revision changed');
    end if;
    update public.session_workspaces set
      revision = ws.revision + 1,
      object_key = p_object_key,
      sha256 = p_sha256,
      size_bytes = p_size_bytes,
      mutation_id = p_mutation_id,
      committed_at = now()
    where session_id = p_session_id
    returning * into ws;
  end if;

  return jsonb_build_object('workspace', jsonb_build_object(
    'session_id', ws.session_id, 'revision', ws.revision,
    'object_key', ws.object_key, 'sha256', ws.sha256,
    'size_bytes', ws.size_bytes, 'committed_at', ws.committed_at
  ), 'deduplicated', was_deduplicated);
end;
$$;

revoke all on function public.commit_session_workspace(text, bigint, text, text, bigint, text) from public, anon, authenticated;
grant execute on function public.commit_session_workspace(text, bigint, text, text, bigint, text) to service_role;

-- Purging a session (including retention sweep) keeps a durable cleanup
-- tombstone. The manager waits longer than the signed URL lifetime before
-- deleting R2 objects, so an in-flight PUT cannot arrive after cleanup.
create function public.request_workspace_deletion() returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  update public.session_workspaces
     set deletion_requested_at = coalesce(deletion_requested_at, now())
   where session_id = old.id;
  return old;
end;
$$;

create trigger agent_sessions_workspace_deletion
before delete on public.agent_sessions
for each row execute function public.request_workspace_deletion();

revoke all on function public.request_workspace_deletion() from public, anon, authenticated;
