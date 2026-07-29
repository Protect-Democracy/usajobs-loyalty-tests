#!/usr/bin/env bash
# Retry helpers for flaky GitHub API calls, shared by the daily workflows.
#
# Why this exists: GitHub's GraphQL API intermittently returns a 5xx *after*
# having performed the mutation. On 2026-07-29, run 30462686894 got
# "HTTP 502 Bad Gateway" from `gh pr create` — PR #520 had in fact been
# created, but the step exited 1, the whole run was marked failed, and the
# failure watcher opened a bogus issue and emailed about it. Every data step
# in that run had passed.
#
# So a plain `cmd || retry` is not enough: retrying a mutation that already
# succeeded either fails again or creates a duplicate. Each helper below
# re-checks real state from the API before every attempt, which makes a
# lying error harmless in both directions.
#
# Source it, don't execute it:
#     source .github/scripts/gh_api_retry.sh
#
# Requires GH_TOKEN in the environment.

_ATTEMPTS="${GH_RETRY_ATTEMPTS:-5}"

# Sleep for attempt * 10 seconds — short enough not to stall the job, long
# enough to ride out the API blips we've actually seen.
_backoff() {
  local attempt="$1"
  sleep $((attempt * 10))
}

# ensure_pr <head-branch> <title> <body>
#
# Guarantees an open PR from <head-branch> into main, or fails loudly.
# Safe to call when a PR already exists.
ensure_pr() {
  local head="$1" title="$2" body="$3"
  local attempt count

  for attempt in $(seq 1 "$_ATTEMPTS"); do
    # Re-check first: covers both "a previous attempt actually worked" and
    # "yesterday's PR is still open".
    count=$(gh pr list --base main --head "$head" --state open --json number --jq length 2>/dev/null || echo "")
    if [ -n "$count" ] && [ "$count" -ne 0 ]; then
      echo "PR from $head into main is open."
      return 0
    fi

    if gh pr create --base main --head "$head" --title "$title" --body "$body"; then
      return 0
    fi

    echo "gh pr create failed (attempt $attempt/$_ATTEMPTS) — re-checking state, then retrying"
    _backoff "$attempt"
  done

  echo "::error::Could not open a PR from $head after $_ATTEMPTS attempts"
  return 1
}

# merge_pr <pr-number> [extra gh pr merge args...]
#
# Merges the PR, treating "already merged" as success. Extra args are passed
# through so callers can pick --merge/--admin/--auto as they already did.
merge_pr() {
  local number="$1"; shift
  local attempt state

  for attempt in $(seq 1 "$_ATTEMPTS"); do
    state=$(gh pr view "$number" --json state --jq .state 2>/dev/null || echo "")
    case "$state" in
      MERGED)
        echo "PR #$number is merged."
        return 0
        ;;
      CLOSED)
        echo "::warning::PR #$number is closed without merging — not retrying."
        return 0
        ;;
    esac

    if gh pr merge "$number" "$@"; then
      return 0
    fi

    echo "gh pr merge failed (attempt $attempt/$_ATTEMPTS) — re-checking state, then retrying"
    _backoff "$attempt"
  done

  echo "::error::Could not merge PR #$number after $_ATTEMPTS attempts"
  return 1
}

# gh_retry <command...>
#
# Plain bounded retry for calls that are safe to repeat (workflow dispatch,
# reads). No state check — only use where a duplicate call is harmless.
gh_retry() {
  local attempt
  for attempt in $(seq 1 "$_ATTEMPTS"); do
    if "$@"; then
      return 0
    fi
    echo "command failed (attempt $attempt/$_ATTEMPTS): $*"
    _backoff "$attempt"
  done
  echo "::error::Failed after $_ATTEMPTS attempts: $*"
  return 1
}
