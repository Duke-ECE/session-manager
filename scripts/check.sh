#!/bin/sh
# Mechanical rule checks for Duke-ECE Go services — the parts of the
# standard a script can verify. Run by `make check` and CI. Fast, boring,
# no dependencies beyond git + gofmt.
set -u
cd "$(dirname "$0")/.."

fail=0
bad() { echo "check: FAIL: $1"; fail=1; }

# 1. go.mod's go directive stays on the 1.25 line (Docker builds with
#    golang:1.25-alpine and GOTOOLCHAIN=local).
go_v=$(awk '/^go / {print $2}' go.mod)
case "$go_v" in
  1.25|1.25.*) ;;
  *) bad "go.mod directive is '$go_v' — must be on the 1.25 line" ;;
esac

# 2. gofmt clean.
out=$(gofmt -l . 2>/dev/null)
[ -z "$out" ] || bad "gofmt: $out"

# 3. No .env files committed — env lives in k8s.yaml + README.
if git ls-files | grep -E '(^|/)\.env$' >/dev/null; then
  bad ".env file committed: $(git ls-files | grep -E '(^|/)\.env$')"
fi

# 4. Supabase migration filenames are timestamped (YYYYMMDDHHMMSS_name.sql)
#    — plain counters collide across repos sharing the project.
for f in supabase/migrations/*.sql; do
  [ -e "$f" ] || continue
  base=$(basename "$f")
  echo "$base" | grep -E '^[0-9]{14}_.+\.sql$' >/dev/null \
    || bad "migration '$base' is not timestamped (YYYYMMDDHHMMSS_name.sql)"
done

# 5. No obvious secret literals in committed manifests.
if git ls-files '*.yaml' '*.yml' | xargs grep -lE 'sk-[a-zA-Z0-9_-]{20,}|sb_secret_[A-Za-z0-9_-]{10,}' 2>/dev/null | grep -q .; then
  bad "possible secret literal in: $(git ls-files '*.yaml' '*.yml' | xargs grep -lE 'sk-[a-zA-Z0-9_-]{20,}|sb_secret_[A-Za-z0-9_-]{10,}' 2>/dev/null)"
fi

[ "$fail" -eq 0 ] && echo "check: all rules pass"
exit "$fail"
