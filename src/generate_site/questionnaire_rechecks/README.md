# Questionnaire re-checks

`../raw_questionnaires/` holds each questionnaire as it looked the **first** time the
pipeline scraped it. Those files are never overwritten, so they're the record of what
a posting originally asked.

Agencies do edit questionnaires after posting. In late September 2026 many replaced the
Merit Hiring Plan essay question

> How would you help advance the President's Executive Orders and policy priorities in
> this role? ...

with

> In this role, you will be expected to professionally and efficiently implement
> executive direction. Provide an example where you implemented leadership direction on
> a strategy or policy decision that differed from your own recommendation.

This directory records what questionnaires say now, without touching the originals.

## Daily refresh

`../refresh_open_questionnaires.py` runs daily in GitHub Actions
(`.github/workflows/daily-questionnaire-refresh.yml`) and re-fetches the questionnaire
of every currently-open posting:

- `current_status.csv`: one row per questionnaire of a currently-open posting, with its
  current flags: `has_loyalty_q` (the old wording), `has_new_wording`, and
  `has_question_5` (OPM's "This position supports [agency]'s mission and current
  priorities..." question). The file is rewritten each run, but a row only changes when
  that questionnaire's text or fetch result changes, so the git history shows real
  changes only.
  - `fetch_status` is `ok`, `failed` (flags carried over from the last known text), or
    `skipped_blocked_domain`. The last applies to portals such as `jobs.faa.gov` that
    block automated access; their flags come from the latest stored copy, which the
    local re-check below keeps current.
  - `text_file` is where the current text lives: a dated folder here, or
    `raw_questionnaires/` if it hasn't changed since first scraped.
- `changes_log.csv`: append-only, one row per questionnaire per day it changed, with
  its flags before and after.
- `<YYYY-MM-DD>/`: the text of every questionnaire that changed that day, using the
  same file names as `raw_questionnaires/` so the two can be diffed.
- `open_postings.csv`: one row per currently-open posting (agency, title, open and close
  dates, USAJOBS link) with `questionnaire_found` and whether any of its questionnaires
  has the old question, the new wording, or Question 5. Built from `current_status.csv`.
- `last_run.json`: date and counts for the latest run.

USAStaffing questionnaires are fetched from
`apply.usastaffing.gov/public/api/viewquestionnaire/<id>`. That's the JSON the public
ViewQuestionnaire page loads to draw itself, rendered to text. Monster questionnaires
come from the Monster preview page.

The site's tracker (`../generate_website_json.py`) reports each posting's latest
re-checked status, and the workflow rebuilds it after the refresh.
`../check_tracker_matches_email.py` then checks that every open posting's tracker status
agrees with `open_postings.csv`, and the workflow opens an "Email and tracker do not
match" issue if any don't.

## Blocked portals (local re-check)

`../recheck_blocked_portals.py` fetches the questionnaires the daily refresh skips
(`skipped_blocked_domain`, e.g. `jobs.faa.gov`) through a real Chrome window over the
DevTools Protocol. It runs on a laptop, not in GitHub Actions. It saves any copy whose
text changed to `<YYYY-MM-DD>/`, and the next daily refresh uses it.

- `blocked_portal_checks.csv`: append-only, one row per questionnaire per local run,
  with its flags, so each has a date it was last verified even when nothing changed.

## One-off re-check, 2026-09-28

Before the daily refresh existed, the open postings first scraped with the old
question were re-checked by hand. `recheck_log.csv` records that run: one row per
posting, with `method` (`usastaffing_api`, `browser`, `browser_cdp`, or
`monster_requests`), `result`, and `recheck_file`. The texts are in `2026-09-28/`.
