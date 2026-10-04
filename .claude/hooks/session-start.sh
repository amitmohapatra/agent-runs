#!/bin/bash
# Claude Code on the web: install what this repo's CLAUDE.md requires before the session
# starts - gstack (global, team mode), Matt Pocock's skills plugin, and the Python env.
# Idempotent: every step is skipped when its result is already there, and the env sync
# never removes a package (--inexact), so a fuller local install is left intact. Local sessions are
# left alone (their tools are installed once, by hand, as CLAUDE.md says).
set -euo pipefail
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || exit 0

GSTACK="$HOME/.claude/skills/gstack"
if [ ! -d "$GSTACK/bin" ]; then
  git clone -q --depth 1 https://github.com/garrytan/gstack.git "$GSTACK"
fi
# setup links each gstack skill into ~/.claude/skills; /review is one of them
if [ ! -e "$HOME/.claude/skills/review" ]; then
  (cd "$GSTACK" && ./setup --team </dev/null >/tmp/gstack-setup.log 2>&1) \
    || echo "gstack setup failed; see /tmp/gstack-setup.log" >&2
fi

if ! claude plugin list 2>/dev/null | grep -q "mattpocock-skills@claude-plugins-official"; then
  claude plugin marketplace add anthropics/claude-plugins-official >/dev/null 2>&1 || true
  claude plugin install mattpocock-skills@claude-plugins-official -y >/dev/null \
    || echo "mattpocock-skills plugin install failed" >&2
fi

cd "$CLAUDE_PROJECT_DIR"
uv sync --all-extras --inexact -q || echo "uv sync --all-extras failed (a sibling checkout this repo builds against may be missing)" >&2
