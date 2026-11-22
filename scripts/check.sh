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

# 5. No obvious secret literals in any committed file — manifests, source, and
#    fixtures alike. A key pasted into a Go or test file is the realistic
#    accident; the deliberately fake placeholders in tests are shorter than these
#    patterns, so they do not trip it.
secret_pattern='sk-[a-zA-Z0-9_-]{20,}|sb_secret_[A-Za-z0-9_-]{10,}|AIza[0-9A-Za-z_-]{30,}|ghp_[A-Za-z0-9]{30,}'
secrets=$(git ls-files -z | xargs -0 grep -lE "$secret_pattern" 2>/dev/null || true)
if [ -n "$secrets" ]; then
  bad "possible secret literal in: $secrets"
fi

[ "$fail" -eq 0 ] && echo "check: all rules pass"
exit "$fail"
