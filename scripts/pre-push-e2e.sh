#!/usr/bin/env bash
# Smart-skip pre-push hook for the e2e suite.
#
# Runs `make test-e2e` only when files relevant to e2e changed between the
# upstream branch and HEAD. If only docs/frontend/spec changed, e2e is skipped.
#
# The hook also skips entirely when the test stack isn't up — no point waiting
# 60s just to fail healthcheck. A clear message tells the user to start it.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Determine the diff range. `git push` provides remote refs on stdin in the
# format "<local_ref> <local_sha> <remote_ref> <remote_sha>". The pre-commit
# framework instead passes "origin <branch>" as $1 $2. We default to comparing
# against origin/main for robustness.
DIFF_RANGE="origin/main...HEAD"
if [ -n "${PRE_COMMIT_REMOTE_BRANCH:-}" ]; then
  DIFF_RANGE="origin/${PRE_COMMIT_REMOTE_BRANCH#refs/heads/}...HEAD"
fi

# Glob of files that warrant running e2e
RELEVANT_PATTERN='^(services/(api|ingestion|normalization|enrichment)/|shared/|tests/e2e/(services|helpers)/|infra/(docker-compose\.yml|docker-compose\.test\.yml|\.env\.test)$)'

CHANGED_FILES="$(git diff --name-only "$DIFF_RANGE" 2>/dev/null || git diff --name-only HEAD~1...HEAD)"
RELEVANT_CHANGES="$(echo "$CHANGED_FILES" | grep -E "$RELEVANT_PATTERN" || true)"

if [ -z "$RELEVANT_CHANGES" ]; then
  echo "[pre-push e2e] No backend/test files changed — skipping e2e suite."
  exit 0
fi

# Cheap reachability check — test stack must be up
if ! curl -fsS --max-time 2 http://localhost:8010/health > /dev/null 2>&1; then
  echo ""
  echo "[pre-push e2e] Test stack api /health (port 8010) is unreachable."
  echo "               Start it with: make test-stack-up"
  echo "               To bypass this hook: git push --no-verify"
  echo ""
  exit 1
fi

echo "[pre-push e2e] Backend/test files changed — running e2e suite..."
echo "[pre-push e2e] Changed (relevant):"
echo "$RELEVANT_CHANGES" | sed 's/^/                /'
echo ""

# Run the e2e suite. ~2 min full run; warm runs much faster.
exec make test-e2e
