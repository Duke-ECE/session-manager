#!/bin/sh
# verify-db.sh — prove the durable-context migrations and transaction functions
# against a real Postgres.
#
# Mocked PostgREST cannot prove constraints, row locks, lease fencing, revision
# races, RLS, or function privileges, so this applies every migration to a real
# server and runs supabase/tests/durable_context.sql against it. Two paths are
# covered: a fresh database, and the v1 schema upgraded with retained rows.
#
#   DATABASE_URL=postgres://user:pass@host:5432/postgres scripts/verify-db.sh
#
# DATABASE_URL must point at an existing database on the server (it is used for
# the CREATE DATABASE statements); the scratch databases sm_verify_fresh and
# sm_verify_upgrade are dropped and recreated. Needs psql.
set -eu
cd "$(dirname "$0")/.."

ADMIN_URL="${DATABASE_URL:-postgres://postgres@127.0.0.1:5432/postgres}"
SERVER_URL="${ADMIN_URL%/*}"
FRESH=sm_verify_fresh
UPGRADE=sm_verify_upgrade
SUITE=supabase/tests/durable_context.sql

# NOTICEs from `drop database if exists` and `drop trigger if exists` are noise.
PGOPTIONS="-c client_min_messages=warning"
export PGOPTIONS

admin()   { psql -q -v ON_ERROR_STOP=1 -d "$ADMIN_URL" "$@"; }
migrate() { psql -q -v ON_ERROR_STOP=1 -d "$SERVER_URL/$1" -f "$2"; }
run_sql() { psql -q -v ON_ERROR_STOP=1 -d "$SERVER_URL/$1"; }

reset_db() {
  admin -c "drop database if exists $1;" -c "create database $1;"
}

# apply_range <db> <first migration basename> <last migration basename>, inclusive.
apply_range() {
  started=no
  for f in supabase/migrations/*.sql; do
    b=$(basename "$f")
    [ "$b" = "$2" ] && started=yes
    [ "$started" = yes ] || continue
    migrate "$1" "$f"
    [ "$b" = "$3" ] && break
  done
}

# Supabase provides these roles; a bare Postgres does not. The migration and the
# suite both branch on their existence, so create them to exercise the real path.
admin <<'SQL'
do $$
declare r text;
begin
  foreach r in array array['anon', 'authenticated', 'service_role'] loop
    if not exists (select 1 from pg_roles where rolname = r) then
      execute format('create role %I nologin', r);
    end if;
  end loop;
  if exists (select 1 from pg_roles where rolname = 'service_role') then
    execute 'alter role service_role bypassrls';
  end if;
end $$;
SQL

echo "== fresh database: $FRESH =="
reset_db "$FRESH"
for f in supabase/migrations/*.sql; do
  migrate "$FRESH" "$f"
done
migrate "$FRESH" "$SUITE"

echo "== upgraded database: $UPGRADE (v1 schema with retained rows) =="
reset_db "$UPGRADE"
apply_range "$UPGRADE" 20260726000001_sessions.sql 20260807140000_agent_sessions_agent_id.sql
run_sql "$UPGRADE" <<'SQL'
insert into agent_sessions (id, user_id, status, llm_model, title, agent_id) values
 ('sess-legacy-1', '11111111-1111-1111-1111-111111111111', 'active', 'gpt-4o-mini',
  'Legacy chat', 'a0000000-0000-0000-0000-000000000001'),
 ('sess-legacy-2', '22222222-2222-2222-2222-222222222222', 'ended', null, null, null);
insert into agent_messages (session_id, seq, role, content) values
 ('sess-legacy-1', 1, 'system', '{"content":"you are helpful"}'),
 ('sess-legacy-1', 2, 'config', '{"llm":{"api_key":"sk-legacy","base_url":"https://x/v1","model":"m"}}'),
 ('sess-legacy-1', 3, 'user', '{"content":"hi"}'),
 ('sess-legacy-1', 4, 'assistant', '{"content":"hello"}'),
 ('sess-legacy-2', 1, 'user', '{"content":"bye"}');
SQL
apply_range "$UPGRADE" 20260919001000_durable_context_schema.sql 20260919002000_durable_context_functions.sql

# The upgrade must preserve every retained row and backfill the new columns.
run_sql "$UPGRADE" <<'SQL'
do $$
declare v_bad int;
begin
  select count(*) into v_bad
    from agent_messages
   where session_id like 'sess-legacy-%'
     and (message_id is null or format_version <> 0 or message_status <> 'complete');
  if v_bad > 0 then
    raise exception 'legacy rows were not preserved as format 0: % bad rows', v_bad;
  end if;
  if (select last_seq from agent_sessions where id = 'sess-legacy-1') <> 4 then
    raise exception 'last_seq was not backfilled for retained rows';
  end if;
  if (select count(*) from agent_messages where session_id like 'sess-legacy-%') <> 5 then
    raise exception 'retained rows were lost during the upgrade';
  end if;
end $$;
SQL

migrate "$UPGRADE" "$SUITE"
echo "verify-db: all checks passed"
