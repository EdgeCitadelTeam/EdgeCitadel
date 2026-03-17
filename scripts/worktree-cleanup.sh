#!/usr/bin/env bash
set -euo pipefail

# Clean up a worktree and its branch after merge.
#
# Usage:
#   scripts/worktree-cleanup.sh <name>           # clean up specific worktree
#   scripts/worktree-cleanup.sh --all             # clean up ALL worktrees
#   scripts/worktree-cleanup.sh --list            # list worktrees and status
#
# Examples:
#   scripts/worktree-cleanup.sh jetstream-consumers
#   scripts/worktree-cleanup.sh --all

# --- Resolve repo root ---

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$REPO_ROOT" ]]; then
    REPO_ROOT="$(git rev-parse --git-common-dir 2>/dev/null | xargs dirname)"
fi

WORKTREE_BASE="${REPO_ROOT}/.claude/worktrees"

usage() {
    echo "Usage: $0 <name> | --all | --list"
    echo ""
    echo "Commands:"
    echo "  <name>    Clean up worktree at .claude/worktrees/<name>/ and its branch"
    echo "  --all     Clean up all worktrees under .claude/worktrees/"
    echo "  --list    List all worktrees with branch and merge status"
    exit 1
}

# Get the branch checked out in a worktree directory
get_worktree_branch() {
    local dir="$1"
    git worktree list --porcelain | awk -v dir="$dir" '
        /^worktree / { wt=$2 }
        /^branch /   { if (wt == dir) { sub(/^branch refs\/heads\//, ""); print } }
    '
}

# Check if a branch has been merged into main
is_merged() {
    local branch="$1"
    git fetch origin main --quiet 2>/dev/null || true
    git merge-base --is-ancestor "$branch" origin/main 2>/dev/null
}

# Clean up a single worktree by name
cleanup_one() {
    local name="$1"
    local worktree_dir="${WORKTREE_BASE}/${name}"

    if [[ ! -d "$worktree_dir" ]]; then
        echo "Warning: No worktree found at ${worktree_dir}"
        return 1
    fi

    local branch
    branch="$(get_worktree_branch "$worktree_dir")"

    if [[ -z "$branch" ]]; then
        echo "Warning: Could not determine branch for ${worktree_dir}"
        echo "  Removing worktree directory only..."
        git worktree remove "$worktree_dir" --force 2>/dev/null || rm -rf "$worktree_dir"
        git worktree prune
        return 0
    fi

    # Check for uncommitted changes
    if git -C "$worktree_dir" status --porcelain 2>/dev/null | grep -q .; then
        echo "Error: Worktree '${name}' has uncommitted changes on branch '${branch}'"
        echo "  cd ${worktree_dir}"
        echo "  # commit or discard changes first"
        return 1
    fi

    # Check if merged
    if is_merged "$branch"; then
        echo "Branch '${branch}' is merged into main."
    else
        echo "Warning: Branch '${branch}' is NOT merged into main."
        read -r -p "  Delete anyway? [y/N] " confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            echo "  Skipped."
            return 0
        fi
    fi

    # Remove worktree
    echo "Removing worktree: ${worktree_dir}"
    git worktree remove "$worktree_dir" --force 2>/dev/null || rm -rf "$worktree_dir"
    git worktree prune

    # Delete local branch
    echo "Deleting local branch: ${branch}"
    git branch -D "$branch" 2>/dev/null || true

    # Delete remote branch
    if git ls-remote --exit-code --heads origin "$branch" &>/dev/null; then
        echo "Deleting remote branch: origin/${branch}"
        git push origin --delete "$branch" 2>/dev/null || true
    fi

    echo "Cleaned up: ${name} (${branch})"
    echo ""
}

# List all worktrees with status
list_worktrees() {
    if [[ ! -d "$WORKTREE_BASE" ]] || [[ -z "$(ls -A "$WORKTREE_BASE" 2>/dev/null)" ]]; then
        echo "No worktrees found under ${WORKTREE_BASE}"
        return 0
    fi

    git fetch origin main --quiet 2>/dev/null || true

    printf "%-25s %-35s %-10s %-10s\n" "WORKTREE" "BRANCH" "MERGED" "CLEAN"
    printf "%-25s %-35s %-10s %-10s\n" "--------" "------" "------" "-----"

    for dir in "${WORKTREE_BASE}"/*/; do
        [[ -d "$dir" ]] || continue
        local name
        name="$(basename "$dir")"
        local branch
        branch="$(get_worktree_branch "$dir")"

        local merged="no"
        if [[ -n "$branch" ]] && is_merged "$branch"; then
            merged="yes"
        fi

        local clean="yes"
        if git -C "$dir" status --porcelain 2>/dev/null | grep -q .; then
            clean="no"
        fi

        printf "%-25s %-35s %-10s %-10s\n" "$name" "${branch:-unknown}" "$merged" "$clean"
    done
}

# --- Main ---

if [[ $# -ne 1 ]]; then
    usage
fi

case "$1" in
    --list)
        list_worktrees
        ;;
    --all)
        if [[ ! -d "$WORKTREE_BASE" ]] || [[ -z "$(ls -A "$WORKTREE_BASE" 2>/dev/null)" ]]; then
            echo "No worktrees to clean up."
            exit 0
        fi
        for dir in "${WORKTREE_BASE}"/*/; do
            [[ -d "$dir" ]] || continue
            name="$(basename "$dir")"
            cleanup_one "$name" || true
        done
        echo "Done. Run 'git worktree list' to verify."
        ;;
    --help|-h)
        usage
        ;;
    *)
        cleanup_one "$1"
        echo "Done. Run 'git worktree list' to verify."
        ;;
esac
