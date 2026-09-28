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

This directory records later re-checks of questionnaires we'd already scraped, without
touching the originals:

- `<YYYY-MM-DD>/` holds the text as fetched on that date, with the same file names as
  `raw_questionnaires/` (e.g. `usastaffing_13048279.txt`), so the two can be diffed.
- `recheck_log.csv` is append-only, one row per posting per re-check:
  - `checked_date`
  - `usajobs_control_number`
  - `questionnaire_url`
  - `method`: `usastaffing_api` is the JSON from
    `apply.usastaffing.gov/public/api/viewquestionnaire/<id>`, rendered to text.
    `browser` is the Playwright scrape. `monster_requests` is the Monster preview page.
  - `result`: `still old wording`, `switched to new wording`, `neither wording`, or
    `rescrape failed`.
  - `recheck_file`: the path under this directory, or empty if the fetch failed.

A later re-check adds a new dated folder and new log rows; it never edits old ones.
For the most recent status of a posting, take its latest row by `checked_date`.
