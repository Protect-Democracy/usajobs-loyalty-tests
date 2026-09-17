#!/usr/bin/env python3
"""Manual backfill for questionnaires on portals that block automated scraping.

Some agencies host their own USAStaffing-style questionnaire portal (e.g. the
FAA at jobs.faa.gov) instead of using apply.usastaffing.gov or Monster
Government directly. extract_questionnaires.py's discovery fallback (see
questionnaire_utils.discover_questionnaire_url_from_usajobs_posting) can find
these links fine over plain HTTP — but the sites themselves block a
launched/headless browser at the network level (confirmed: jobs.faa.gov's
TCP handshake hangs for a Playwright-launched Chromium from a dev laptop, a
CI runner, and a residential network alike, but loads instantly in a normal,
hand-driven browser). This is not something the daily GitHub Actions pipeline
can ever get through — there is no human there to solve it.

The workaround is CDP-attach: drive a real Chrome window you start and log
into by hand, over the DevTools Protocol, instead of Playwright launching its
own browser. The automation lives outside the browser process, so there is no
"launched by software" signal for the site to key on. See
~/.claude/skills/scraping-cloudflare-cdp for the general technique (same idea
Cloudflare-walled sites need).

This script is NOT part of the daily automated pipeline and never will be —
it must be run locally, by a human, periodically, as new agency-branded
portals accumulate a backlog. It is safe to re-run any time: it only scrapes
questionnaires that don't already have a file in raw_questionnaires/.

Usage:
    1. Start a debug Chrome and load the target site once by hand:
       "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
           --remote-debugging-port=9222 --user-data-dir="$HOME/chrome-debug"
       (visit e.g. https://jobs.faa.gov in that window once)

    2. cd src/generate_site
       python3 backfill_blocked_portal_questionnaires.py

    This will:
      - Run discovery (fetch_usajobs_html fallback) across every job that
        currently has no questionnaire link at all, and append any newly
        found links to questionnaire_links.csv.
      - Scrape the ones on a known-blocked domain (BLOCKED_DOMAINS below)
        through the CDP-attached Chrome from step 1.
      - Scrape everything else (USAStaffing/Monster recoveries from the same
        discovery pass) the normal headless way — no CDP needed for those.

    Add a new agency portal to BLOCKED_DOMAINS (and to
    questionnaire_utils._AGENCY_QUESTIONNAIRE_URL_PATTERNS, so it's
    discoverable at all) once you've confirmed the same launched-browser
    block applies to it.
"""
import argparse
import json
import time
from pathlib import Path

import pandas as pd

from extract_questionnaires import extract_questionnaire_links_from_job, scrape_questionnaire_worker
from questionnaire_utils import get_questionnaire_filepath

DATA_DIR = Path('../../data')

# Domains confirmed to block a launched/headless browser at the network level.
# Links on these domains need --no-headless (CDP-attach); everything else can
# scrape normally.
BLOCKED_DOMAINS = ('jobs.faa.gov',)


def find_jobs_missing_a_link(only_live: bool = True) -> pd.DataFrame:
    """Every job with no row in questionnaire_links.csv, optionally live-only."""
    all_jobs_df = pd.read_csv('all_jobs_clean.csv', low_memory=False)
    links_df = pd.read_csv('questionnaire_links.csv')
    linked_ids = set(links_df['usajobs_control_number'])

    missing = all_jobs_df[~all_jobs_df['usajobs_control_number'].isin(linked_ids)].copy()
    if only_live:
        missing['position_close_date'] = pd.to_datetime(
            missing['position_close_date'], format='mixed', errors='coerce'
        )
        now = pd.Timestamp.now()
        missing = missing[missing['position_close_date'].isna() | (missing['position_close_date'] >= now)]
    return missing


def discover_links(missing_df: pd.DataFrame) -> dict:
    """Run the discovery fallback for each job; returns {control_number: url}."""
    ids = set(missing_df['usajobs_control_number'].astype(int))
    dfs = []
    for parquet_file in sorted(DATA_DIR.glob('*_jobs_*.parquet')):
        d = pd.read_parquet(parquet_file)
        match = d[d['usajobsControlNumber'].isin(ids)]
        if len(match):
            dfs.append(match)
    if not dfs:
        return {}
    combined = pd.concat(dfs).drop_duplicates(subset='usajobsControlNumber')

    found = {}
    for i, (_, row) in enumerate(combined.iterrows()):
        try:
            links = extract_questionnaire_links_from_job(row, fetch_usajobs_html=True)[0]
        except Exception:
            continue
        if links:
            found[int(row['usajobsControlNumber'])] = links[0]
        if (i + 1) % 200 == 0:
            print(f"  discovery: {i + 1}/{len(combined)}, found so far: {len(found)}", flush=True)
    return found


def persist_new_links(found: dict) -> None:
    """Append rows to questionnaire_links.csv for any (url, control_number) pair not already there."""
    if not found:
        return
    ids = [int(k) for k in found]
    dfs = []
    for parquet_file in sorted(DATA_DIR.glob('*_jobs_*.parquet')):
        d = pd.read_parquet(parquet_file)
        match = d[d['usajobsControlNumber'].isin(ids)].copy()
        if len(match):
            match['__source_file'] = parquet_file.name
            dfs.append(match)
    combined = pd.concat(dfs).drop_duplicates(subset='usajobsControlNumber').set_index('usajobsControlNumber')

    existing_df = pd.read_csv('questionnaire_links.csv')
    existing_pairs = set(zip(existing_df['questionnaire_url'], existing_df['usajobs_control_number']))

    rows = []
    for cid_str, url in found.items():
        cid = int(cid_str)
        if (url, cid) in existing_pairs or cid not in combined.index:
            continue
        row = combined.loc[cid]
        rows.append({
            'questionnaire_url': url,
            'usajobs_control_number': cid,
            'position_title': row.get('positionTitle'),
            'announcement_number': row.get('announcementNumber'),
            'hiring_agency': row.get('hiringAgencyName'),
            'occupation_series': row.get('occupationSeries'),
            'occupation_name': row.get('occupationName'),
            'position_open_date': row.get('positionOpenDate'),
            'position_close_date': row.get('positionCloseDate'),
            'position_location': row.get('positionLocation'),
            'grade_code': row.get('payScale'),
            'position_schedule': row.get('positionSchedule'),
            'service_type': row.get('serviceType'),
            'extracted_from_file': row.get('__source_file'),
            'extracted_date': pd.Timestamp.now().isoformat(),
            'inferred_from_announcement': False,
            'inferred_from_posting_html': True,
        })

    if rows:
        pd.DataFrame(rows).to_csv('questionnaire_links.csv', mode='a', header=False, index=False)
    print(f"Appended {len(rows)} new (url, job) rows to questionnaire_links.csv")


def scrape(urls: set, headless: bool, politeness_delay: float = 1.5) -> tuple:
    links_df = pd.read_csv('questionnaire_links.csv')
    to_scrape = links_df[links_df['questionnaire_url'].isin(urls)].copy()
    to_scrape = to_scrape[~to_scrape['questionnaire_url'].apply(lambda u: get_questionnaire_filepath(u).exists())]
    to_scrape = to_scrape.drop_duplicates(subset='questionnaire_url')

    success, failed = 0, 0
    for i, (_, row) in enumerate(to_scrape.iterrows()):
        args = (row.to_dict(), './raw_questionnaires', i + 1, len(to_scrape), headless, None)
        try:
            _, ok = scrape_questionnaire_worker(args)
        except Exception as e:
            print(f"  ERROR on {row['questionnaire_url']}: {e}")
            ok = False
        success += int(ok)
        failed += int(not ok)
        if not headless:
            time.sleep(politeness_delay)
    return success, failed, len(to_scrape)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--all-jobs', action='store_true',
                         help='Scan every job with no link, not just live ones (slower)')
    parser.add_argument('--skip-cdp', action='store_true',
                         help="Skip the CDP-attached scrape of BLOCKED_DOMAINS links (discovery + "
                              'non-blocked scraping only — use when no debug Chrome is running)')
    args = parser.parse_args()

    print('Finding jobs with no questionnaire link...')
    missing = find_jobs_missing_a_link(only_live=not args.all_jobs)
    print(f"{len(missing):,} jobs to check")

    print('Running discovery...')
    found = discover_links(missing)
    print(f"Discovered {len(found)} links")

    persist_new_links(found)

    blocked_urls = {u for u in found.values() if any(d in u for d in BLOCKED_DOMAINS)}
    other_urls = {u for u in found.values() if u not in blocked_urls}

    print(f"\nScraping {len(other_urls)} non-blocked links (headless)...")
    success, failed, total = scrape(other_urls, headless=True)
    print(f"  {success}/{total} succeeded, {failed} failed")

    if args.skip_cdp:
        print(f"\nSkipping {len(blocked_urls)} blocked-domain links (--skip-cdp).")
        return

    print(f"\nScraping {len(blocked_urls)} blocked-domain links via CDP-attached Chrome "
          "(make sure it's running — see module docstring)...")
    success, failed, total = scrape(blocked_urls, headless=False)
    print(f"  {success}/{total} succeeded, {failed} failed")


if __name__ == '__main__':
    main()
