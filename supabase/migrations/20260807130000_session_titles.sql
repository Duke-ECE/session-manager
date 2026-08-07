-- session-manager: optional display title for a session (e.g. an
-- LLM-generated summary of the first turn), set via session.v1.SetTitle.
-- Nullable; sessions created before this migration keep a null title.

alter table agent_sessions add column title text;
