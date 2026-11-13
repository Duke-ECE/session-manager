-- Add the immutable thinking choice to the v2 configuration snapshot.
alter table public.agent_session_config
  add column if not exists thinking_mode text not null default ''
  check (thinking_mode in ('', 'off', 'low', 'high', 'max', 'provider_default'));

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
    'thinking_mode', c.thinking_mode,
    'credential_ref', c.credential_ref,
    'context_window_tokens', c.context_window_tokens,
    'output_limit_tokens', c.output_limit_tokens,
    'tools', c.tools,
    'created_at', c.created_at
  );
$$;

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
  v_thinking text;
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
  v_thinking := coalesce(p_config->>'thinking_mode', '');
  v_tools    := coalesce(p_config->'tools', '[]'::jsonb);
  v_window   := coalesce((p_config->>'context_window_tokens')::bigint, 0);
  v_output   := coalesce((p_config->>'output_limit_tokens')::bigint, 0);

  if v_fmt <= 0 or v_hash = '' then
    perform session_fail('invalid_argument', 'config format_version and config_hash are required');
  end if;
  if v_thinking not in ('', 'off', 'low', 'high', 'max', 'provider_default') then
    perform session_fail('invalid_argument', 'invalid thinking_mode');
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
    credential_ref, context_window_tokens, output_limit_tokens, tools, thinking_mode
  ) values (
    p_id, v_fmt, v_hash, coalesce(p_config->>'system_prompt', ''), v_provider, v_model, v_base,
    coalesce(p_config->>'credential_ref', ''), v_window, v_output, v_tools, v_thinking
  ) returning * into v_cfg;

  return jsonb_build_object('session', session_json(v_sess), 'config', session_config_json(v_cfg));
end;
$$;
