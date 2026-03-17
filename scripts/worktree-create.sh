#!/usr/bin/env bash
set -euo pipefail

# Create a new git worktree with a conventionally-named branch.
#
# Usage:
#   scripts/worktree-create.sh <type> <short-description>
#
# Examples:
#   scripts/worktree-create.sh feat jetstream-consumers
#   scripts/worktree-create.sh fix mqtt-reconnection
#   scripts/worktree-create.sh docs adr-p2p-delegation
#
# Creates:
#   .claude/worktrees/<short-description>/   (worktree directory)
#   branch: <type>/<short-description>       (based on latest main)

VALID_TYPES="feat|fix|docs|style|refactor|perf|test|chore|ci|build"

usage() {
    echo "Usage: $0 <type> <short-description>"
    echo ""
    echo "Types: ${VALID_TYPES}"
    echo ""
    echo "Examples:"
    echo "  $0 feat jetstream-consumers"
    echo "  $0 fix mqtt-reconnection"
    echo "  $0 docs adr-p2p-delegation"
    exit 1
}

# --- Validation ---

if [[ $# -ne 2 ]]; then
    usage
fi

TYPE="$1"
DESC="$2"

if ! echo "$TYPE" | grep -qE "^(${VALID_TYPES})$"; then
    echo "Error: Invalid type '${TYPE}'"
    echo "Valid types: ${VALID_TYPES}"
    exit 1
fi

if ! echo "$DESC" | grep -qE '^[a-z0-9][a-z0-9-]*[a-z0-9]$'; then
    echo "Error: Description must be lowercase alphanumeric with hyphens (e.g., 'my-feature')"
    exit 1
fi

# --- Resolve repo root ---

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
    # May be in a worktree — find the main working tree
    REPO_ROOT="$(git rev-parse --git-common-dir 2>/dev/null | xargs dirname)"
fi

BRANCH="${TYPE}/${DESC}"
WORKTREE_DIR="${REPO_ROOT}/.claude/worktrees/${DESC}"

# --- Pre-flight checks ---

if git show-ref --verify --quiet "refs/heads/${BRANCH}" 2>/dev/null; then
    echo "Error: Branch '${BRANCH}' already exists"
    echo "  To reuse it:  git worktree add ${WORKTREE_DIR} ${BRANCH}"
    echo "  To delete it: git branch -d ${BRANCH}"
    exit 1
fi

if [[ -d "$WORKTREE_DIR" ]]; then
    echo "Error: Worktree directory already exists: ${WORKTREE_DIR}"
    echo "  Clean up with: scripts/worktree-cleanup.sh ${DESC}"
    exit 1
fi

# --- Create ---

echo "Updating main..."
git fetch origin main --quiet

echo "Creating worktree..."
mkdir -p "${REPO_ROOT}/.claude/worktrees"
git worktree add -b "${BRANCH}" "${WORKTREE_DIR}" origin/main --quiet

# --- Post-setup: copy env files and install deps ---

echo "Setting up worktree environment..."

# Copy .env files from main repo if they exist
for env_file in .env .env.local; do
    if [[ -f "${REPO_ROOT}/${env_file}" ]]; then
        cp "${REPO_ROOT}/${env_file}" "${WORKTREE_DIR}/${env_file}"
        echo "  Copied ${env_file}"
    fi
done

# Install dependencies if package files exist
if [[ -f "${WORKTREE_DIR}/frontend/package.json" ]]; then
    echo "  Installing frontend dependencies..."
    (cd "${WORKTREE_DIR}/frontend" && npm ci --silent 2>/dev/null) || echo "  Warning: npm ci failed (run manually)"
fi

if [[ -f "${WORKTREE_DIR}/aggregator/requirements.txt" ]]; then
    echo "  Installing Python dependencies..."
    (cd "${WORKTREE_DIR}/aggregator" && pip install -r requirements.txt -q 2>/dev/null) || echo "  Warning: pip install failed (run manually)"
fi

if [[ -f "${WORKTREE_DIR}/e2e/package.json" ]]; then
    echo "  Installing e2e dependencies..."
    (cd "${WORKTREE_DIR}/e2e" && npm ci --silent 2>/dev/null) || echo "  Warning: npm ci failed (run manually)"
fi

echo ""
echo "Worktree created:"
echo "  Branch:    ${BRANCH}"
echo "  Directory: ${WORKTREE_DIR}"
echo ""
echo "Next steps:"
echo "  cd ${WORKTREE_DIR}"
echo "  # make changes, commit, push"
echo "  git push -u origin ${BRANCH}"
echo "  gh pr create --base main"
echo ""
echo "When done:"
echo "  scripts/worktree-cleanup.sh ${DESC}"
