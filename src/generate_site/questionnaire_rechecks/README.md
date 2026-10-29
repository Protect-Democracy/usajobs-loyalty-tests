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
  - `fetch_status` is `ok`, `failed` (flags carried over from the last known text),
    `skipped_blocked_domain`, or `posting_not_listed`. `skipped_blocked_domain` applies
    to portals such as `jobs.faa.gov` that block automated access; their flags come from
    the latest stored copy, which the re-check below keeps current.
    `posting_not_listed` is a questionnaire of a posting kept open although USAJobs
    didn't list it today (below); it isn't re-fetched.
  - `checked_date` is the day the flags were last confirmed against the questionnaire
    itself: today for a fetched one, the last re-check for a blocked portal.
  - `text_file` is where the current text lives: a dated folder here, or
    `raw_questionnaires/` if it hasn't changed since first scraped.
- `changes_log.csv`: append-only, one row per questionnaire per day it changed, with
  its flags before and after.
- `<YYYY-MM-DD>/`: the text of every questionnaire that changed that day, using the
  same file names as `raw_questionnaires/` so the two can be diffed.
- `open_postings.csv`: one row per currently-open posting (agency, title, open and close
  dates, USAJOBS link) with `questionnaire_found` and whether any of its questionnaires
  has the old question, the new wording, or Question 5. Built from `current_status.csv`.
  `listed_today` is `False` for a posting kept although USAJobs didn't list it today
  (`not_listed_since` says from when), and `checked_date` is the oldest `checked_date`
  of its questionnaires: its flags are only as fresh as that. `reopened_date` is set for a
  posting that closed and was reopened under the same control number: the open date its
  USAJobs page shows, at least two days after `original_close_date` (the close date it was
  first collected with). `position_open_date` stays the first open date. `page_open_date`
  is the open date read from the page, for postings past their first close date (blank if
  not read).
- `last_run.json`: date and counts for the latest run, and `warnings`: problems with
  today's data. The workflow opens an "Open postings data warnings" issue for them, and
  the daily email shows them at the top.

## Postings missing from the listing

USAJobs search sometimes leaves out open postings for a day. On 2026-10-23 it left out
124 postings that were listed the day before and the day after, 112 of them at the FAA,
and the daily email undercounted. So a posting that was open in the last run, isn't in
today's listing, and whose close date hasn't passed is looked up on its own USAJobs
page:

- the page says "This job announcement has closed": dropped (`closed_early` in
  `link_changes_log.csv`). Agencies close postings early, e.g. when a set number of
  applications has come in.
- the page shows the posting without saying that: kept as open, with its last
  re-checked flags (`kept_page_open`). Its questionnaires on blocked portals
  (jobs.faa.gov) stay `skipped_blocked_domain`, so that evening's local re-check still
  reads them. (On 2026-10-23 it didn't: the 22 FAA postings missing from the listing
  weren't on that evening's list, so 2026-10-24 reported their 2026-10-22 text even
  though the listing had them again.)
- the page can't be read: kept (`kept_page_unreadable`) for up to three days out of the
  listing, then dropped (`dropped_page_unreadable`).

Replaying 2026-10-23 this way: of the 204 postings missing from that day's listing
before their close date, 103 had closed early and 101 (all FAA) were open, 20 of them
with the Loyalty Q (page lookups on 2026-10-26).

A day with 25 or more kept postings gets a warning in `last_run.json`.

USAStaffing questionnaires are fetched from
`apply.usastaffing.gov/public/api/viewquestionnaire/<id>`. That's the JSON the public
ViewQuestionnaire page loads to draw itself, rendered to text. Monster questionnaires
come from the Monster preview page.

The site's tracker (`../generate_website_json.py`) reports each posting's latest
re-checked status, and the workflow rebuilds it after the refresh.
`../check_tracker_matches_email.py` then checks that every open posting's tracker status
agrees with `open_postings.csv`, and the workflow opens an "Email and tracker do not
match" issue if any don't.

## Blocked portals (jobs.faa.gov)

The daily refresh can't fetch some portals with plain requests (`skipped_blocked_domain`,
e.g. `jobs.faa.gov`). Its bot check turns away headless browsers, from a home IP or
GitHub's, but lets a real Chrome window in (tested 2026-10-27). So the daily workflow
runs the refresh with `--check-blocked-portals` on a virtual display (`xvfb-run`).
Before it decides each questionnaire's answers, it re-checks every one on today's list,
new and reopened postings included, through `../recheck_blocked_portals.py`. That
script saves any copy whose text changed to `<YYYY-MM-DD>/`, and the refresh uses it
right away. Before 2026-10-28 the only re-check was the same script run on a laptop in
the evening (the loyalty-q-reports FAA job). It was turned off on 2026-10-29: when both
ran, each saved copies the other then counted as failures, because jobs.faa.gov lists a
posting's locations in a different order on each load and the script won't overwrite a
copy saved the same day.

If the re-check fails, the refresh still runs: those questionnaires keep their last
flags, `last_run.json` records the failure (`blocked_portal_check`), and their
`checked_date` shows how old they are. When the newest check is older than yesterday,
`last_run.json` gets a warning (and so an issue and a line at the top of the email). On
2026-10-24 the email reported 22 FAA postings with the Loyalty Q from the 2026-10-22
check; that evening's check found all 22 had switched to the new wording.

Until 2026-10-27 most FAA questionnaires were never checked at all. The scraper can't get
past jobs.faa.gov's bot check, took that as a broken link, and added the link to
`../questionnaire_known_bad.txt` (4,900 FAA lines), and the refresh dropped blacklisted
links. That hid 136 of the 182 open FAA postings' questionnaires; one had the Loyalty Q.
The refresh now keeps blacklisted links on blocked portals and the Chrome re-check reads
them, and the scraper no longer blacklists them. A blocked-portal questionnaire that has
never been read (the re-check failed and there's no saved copy) gets its own warning, since
its posting counts as not having the question.

## Questionnaires not linked from the posting

Some USAStaffing postings don't link their questionnaire and don't show a "Preview job
questionnaire" button, and USAJobs marks some of them as not using USAStaffing. When a
posting still has no link, the refresh tries every 7-9 digit number in its announcement
number (e.g. `DE-13101174-27-ALK`) and keeps a guess only if the questionnaire's own control
number or announcement number is the posting's. It does this only for a posting whose
Apply button goes to USAStaffing: on 2026-10-27 the guess matched questionnaires for 476
open postings, but 394 of them had no Apply button, so their applicants may never see the
questionnaire. One was a Navy police officer "public notice flyer" (882412600) that takes
applications by email; its questionnaire has the Loyalty Q.
That leaves 82 postings, 1 with the Loyalty Q. Others (e.g. ICE Health Service Corps)
have no number to go on; a one-off scan of questionnaire IDs that day found 380 more, none
with the Loyalty Q (198 of them have no Apply button either).

USAStaffing numbers questionnaires in order, about 1,500-2,000 a day, and each one's
public JSON names its posting's control number. `../scan_usastaffing_ids.py` runs daily
after the refresh and appends each new ID to `usastaffing_ids.csv` (`qid`,
`control_number`, `close_date`; rows closed more than 90 days are dropped): it re-tries
the last 2,000 IDs it doesn't have and goes on until 500 in a row don't exist, about
3,000 requests. The next day's refresh adds those questionnaires to the open postings
they belong to that apply through USAStaffing, and verifies each against its control
number like any other inferred link. The table was seeded from scans of IDs
12,370,000-13,104,000 on 2026-10-27/28.

USAStaffing refuses clients that ask too fast: a home IP was blocked (403) after about
300,000 requests at 28 a second, and on 2026-10-28 a GitHub runner after about 1,700 at 4
workers, when the scan ran before the refresh (the refresh and the jobs.faa.gov check
weren't affected). So it now runs after them, with 2 workers.
`usastaffing_ids_last_scan.json` records whether it finished; if the last one didn't (or
is older than yesterday), `last_run.json` warns.

- `blocked_portal_checks.csv`: append-only, one row per questionnaire per re-check,
  with its flags, so each has a date it was last verified even when nothing changed.

## One-off re-check, 2026-09-28

Before the daily refresh existed, the open postings first scraped with the old
question were re-checked by hand. `recheck_log.csv` records that run: one row per
posting, with `method` (`usastaffing_api`, `browser`, `browser_cdp`, or
`monster_requests`), `result`, and `recheck_file`. The texts are in `2026-09-28/`.
