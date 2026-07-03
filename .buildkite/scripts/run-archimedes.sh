#!/bin/bash
# Runs an archimedes workflow session.
# Called by .buildkite/pipelines/agentic-workflow.yml.
#
# All variables (WORKFLOW, ISSUE_URL, PR_URL, BUILDKITE_RETRY_COUNT,
# ARCHIMEDES_SESSION_DIR) are set as shell environment variables before this
# script runs — no Buildkite YAML interpolation headaches here.

set -euo pipefail

# ── Validate inputs ────────────────────────────────────────────────────────────
case "${WORKFLOW:-}" in
  test-analysis)
    [[ -n "${ISSUE_URL:-}" ]] || { echo "ISSUE_URL is required for test-analysis" >&2; exit 1; }
    ;;
  pull-request-fix)
    [[ -n "${PR_URL:-}" ]] || { echo "PR_URL is required for pull-request-fix" >&2; exit 1; }
    ;;
  pull-request-creation)
    [[ -n "${ISSUE_URL:-}" ]] || { echo "ISSUE_URL is required for pull-request-creation" >&2; exit 1; }
    ;;
  *)
    echo "WORKFLOW is not set or not recognised: '${WORKFLOW:-}'" >&2
    echo "Trigger this pipeline with WORKFLOW set to one of:" >&2
    echo "  test-analysis | pull-request-fix | pull-request-creation" >&2
    echo "From the Buildkite UI: set WORKFLOW in the 'Environment Variables' field of the New Build dialog." >&2
    echo "From the API: include WORKFLOW in the env block of the build request." >&2
    exit 1
    ;;
esac

# ── Session persistence (spot-instance preemption recovery) ───────────────────
SESSION_DIR="${ARCHIMEDES_SESSION_DIR:-/tmp/archimedes-sessions}"
mkdir -p "$SESSION_DIR"

_upload_session() {
  local latest
  latest=$(ls -t "$SESSION_DIR"/*.jsonl 2>/dev/null | head -1) || true
  if [[ -n "$latest" ]]; then
    local dest="$SESSION_DIR/archimedes-session.jsonl"
    # The newest .jsonl may already be the canonical artifact name (e.g. on a
    # resumed retry). Only copy when they differ to avoid a cp self-copy error.
    [[ "$latest" -ef "$dest" ]] || cp "$latest" "$dest"
    buildkite-agent artifact upload "$dest" 2>/dev/null || true
    echo "Session artifact uploaded ($(wc -c < "$dest") bytes)"
  fi
}
trap '_upload_session' EXIT
trap 'echo "SIGTERM received — uploading session before exit"; _upload_session; exit 47' SIGTERM

if [[ "${BUILDKITE_RETRY_COUNT:-0}" -gt 0 ]]; then
  echo "--- Retry #${BUILDKITE_RETRY_COUNT} — downloading previous session"
  buildkite-agent artifact download "archimedes-session.jsonl" "$SESSION_DIR/" 2>/dev/null \
    && echo "Session downloaded ($(wc -c < "$SESSION_DIR/archimedes-session.jsonl") bytes) — will resume" \
    || echo "No previous session artifact found — starting fresh"
fi

# ── Opening annotation ─────────────────────────────────────────────────────────
REF="${ISSUE_URL:-${PR_URL:-}}"
buildkite-agent annotate \
  "### 🤖 archimedes starting

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}
Session is initialising…" \
  --context "archimedes-progress" --style "info"

# ── Run archimedes (in-process nono sandbox) ─────────────────────────────────────
# archimedes sandboxes ITSELF via the bundled nono-ts SDK when ARCHIMEDES_SANDBOX=1
# (Landlock on Linux). Applying the sandbox in-process — after node has started
# and its libraries are mapped — avoids the `nono run` CLI's supervised re-exec,
# which re-resolved archimedes's `#!/usr/bin/env node` interpreter under the
# sandbox and died with exit 127 before any agent code ran. The filesystem
# allowlist, Vault-handle scrubbing, and fail-closed behaviour live in the
# archimedes distro (packages/archimedes/src/sandbox.js + nono-archimedes.json).
# Network is left open in-process; host-level egress control, if ever required,
# belongs at the VM/firewall layer.
export ARCHIMEDES_SANDBOX=1

ARCHIMEDES_EXIT=0
case "${WORKFLOW}" in
  test-analysis)
    archimedes analyze --issue-url "${ISSUE_URL}" || ARCHIMEDES_EXIT=$? ;;
  pull-request-fix)
    archimedes fix-pr  --pr-url    "${PR_URL}"    || ARCHIMEDES_EXIT=$? ;;
  pull-request-creation)
    archimedes create  --issue-url "${ISSUE_URL}" || ARCHIMEDES_EXIT=$? ;;
esac

# ── Final annotation ───────────────────────────────────────────────────────────
if [[ $ARCHIMEDES_EXIT -eq 0 ]]; then
  buildkite-agent annotate \
    "### ✅ archimedes completed

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}" \
    --context "archimedes-progress" --style "success"
else
  buildkite-agent annotate \
    "### ❌ archimedes failed (exit ${ARCHIMEDES_EXIT})

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}
See the job log for details." \
    --context "archimedes-progress" --style "error"
fi

exit $ARCHIMEDES_EXIT
