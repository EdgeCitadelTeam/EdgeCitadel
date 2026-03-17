#!/usr/bin/env bash
set -euo pipefail

# Resume work on an existing branch by recreating its worktree.
#
# Use this when a PR needs changes after the worktree was already removed.
# The branch still exists (locally or on remote) — this script recreates
# the worktree from it, installs dependencies, and copies env files.
#
# Usage:
#   scripts/worktree-resume.sh <branch-name>
#   scripts/worktree-resume.sh <pr-number>
#
# Examples:
#   scripts/worktree-resume.sh feat/jetstream-consumers
#   scripts/worktree-resume.sh 42    # checks out the branch for PR #42
#
# Creates:
#   .claude/worktrees/<short-description>/   (worktree directory)

usage() {
    echo "Usage: $0 <branch-name|pr-number>"
    echo ""
    echo "Resume work on an existing branch by recreating its worktree."
    echo "Use after a worktree was cleaned up but the PR still needs changes."
    echo ""
    echo "Examples:"
    echo "  $0 feat/jetstream-consumers"
    echo "  $0 42    # resume branch from PR #42"
    exit 1
}

if [[ $# -ne 1 ]]; then
    usage
fi

ARG="$1"

# --- Resolve repo root ---

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
    REPO_ROOT="$(git rev-parse --git-common-dir 2>/dev/null | xargs dirname)"
fi

WORKTREE_BASE="${REPO_ROOT}/.claude/worktrees"

# --- Resolve branch name ---

if [[ "$ARG" =~ ^[0-9]+$ ]]; then
    # PR number — look up the branch via gh CLI
    if ! command -v gh &>/dev/null; then
        echo "Error: gh CLI is required to resolve PR numbers"
        echo "  Install: https://cli.github.com/"
        exit 1
    fi
    BRANCH="$(gh pr view "$ARG" --json headRefName -q .headRefName 2>/dev/null || true)"
    if [[ -z "$BRANCH" ]]; then
        echo "Error: Could not find branch for PR #${ARG}"
        exit 1
    fi
    echo "PR #${ARG} → branch: ${BRANCH}"
else
    BRANCH="$ARG"
fi

# --- Derive worktree name from branch ---
# Branch format: type/short-description → worktree name: short-description

if [[ "$BRANCH" == */* ]]; then
    WORKTREE_NAME="${BRANCH#*/}"
else
    WORKTREE_NAME="$BRANCH"
fi

WORKTREE_DIR="${WORKTREE_BASE}/${WORKTREE_NAME}"

# --- Pre-flight checks ---

if [[ -d "$WORKTREE_DIR" ]]; then
    echo "Worktree already exists at ${WORKTREE_DIR}"
    echo "  cd ${WORKTREE_DIR}"
    exit 0
fi

# Prune stale worktree references first
git worktree prune 2>/dev/null || true

# --- Fetch and resolve branch ---

echo "Fetching latest from origin..."
git fetch origin --quiet

# Check if branch exists locally
LOCAL_EXISTS=false
if git show-ref --verify --quiet "refs/heads/${BRANCH}" 2>/dev/null; then
    LOCAL_EXISTS=true
fi

# Check if branch exists on remote
REMOTE_EXISTS=false
if git show-ref --verify --quiet "refs/remotes/origin/${BRANCH}" 2>/dev/null; then
    REMOTE_EXISTS=true
fi

if ! $LOCAL_EXISTS && ! $REMOTE_EXISTS; then
    echo "Error: Branch '${BRANCH}' not found locally or on origin"
    echo ""
    echo "Available branches:"
    git branch -a --list "*${WORKTREE_NAME}*" 2>/dev/null || true
    exit 1
fi

# --- Create worktree ---

mkdir -p "$WORKTREE_BASE"

if $LOCAL_EXISTS; then
    echo "Creating worktree from local branch '${BRANCH}'..."
    git worktree add "$WORKTREE_DIR" "$BRANCH" --quiet
elif $REMOTE_EXISTS; then
    echo "Creating worktree from remote branch 'origin/${BRANCH}'..."
    git worktree add "$WORKTREE_DIR" -b "$BRANCH" "origin/${BRANCH}" --quiet
fi

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
echo "Worktree resumed:"
echo "  Branch:    ${BRANCH}"
echo "  Directory: ${WORKTREE_DIR}"
echo ""
echo "Next steps:"
echo "  cd ${WORKTREE_DIR}"
echo "  # address review feedback, commit, push"
echo "  git push origin ${BRANCH}"
echo ""
echo "When done:"
echo "  scripts/worktree-cleanup.sh ${WORKTREE_NAME}"
