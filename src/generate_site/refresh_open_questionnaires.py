#!/usr/bin/env python3
"""Daily re-pull of the questionnaire for every currently-open posting.

raw_questionnaires/ holds each questionnaire as first scraped and is never
overwritten. Agencies edit questionnaires after posting (in late September
2026 many swapped the Loyalty Q for new wording), so this re-fetches every
open posting's questionnaire and records what it says now, under
questionnaire_rechecks/:

  current_status.csv  One row per questionnaire of a currently-open posting.
                      Rewritten each run, but a row only changes when that
                      questionnaire's text or fetch result changes.
  changes_log.csv     Append-only. A row whenever a questionnaire's text
                      changes, or when it's first refreshed and its flags
                      differ from the last stored copy.
  <YYYY-MM-DD>/       The text of each questionnaire that changed that day.
  open_postings.csv   One row per currently-open posting: agency, title, dates, link,
                      and whether it has the Loyalty Q, the new wording, or Question 5.
  last_run.json       Date and counts for the latest run.

USAStaffing questionnaires come from the JSON the public ViewQuestionnaire page
itself loads (/public/api/viewquestionnaire/<id>); Monster ones from the
Monster preview page. Other portals (e.g. jobs.faa.gov) block automated access
and are recorded as skipped, keeping the flags of their last stored copy.

Usage:
    cd src/generate_site
    python refresh_open_questionnaires.py [--limit N] [--out-dir DIR] [--workers 5]
"""
import argparse
import hashlib
import html
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from questionnaire_utils import (
    BROWSER_HEADERS,
    RAW_QUESTIONNAIRES_DIR,
    fetch_usastaffing_questionnaire,
    get_questionnaire_filename,
    load_known_bad_urls,
    transform_monster_url,
)

LOYALTY_Q = re.compile(
    r"How would you help advance the President(?:'|’)s Executive Orders and policy priorities in this role",
    re.IGNORECASE,
)
NEW_WORDING = re.compile(r"professionally and efficiently implement executive direction", re.IGNORECASE)
# OPM's template: "This position supports [agency]'s mission and current priorities, including
# [1-3 priorities]. Describe how your skills and experience would help the agency advance those
# priorities in this role." Match the fixed parts, since the brackets get filled in per agency.
QUESTION_5 = re.compile(r"mission and current priorities|help the agency advance those priorities", re.IGNORECASE)

STATUS_COLUMNS = [
    'questionnaire_url', 'usajobs_control_numbers', 'fetch_status',
    'has_loyalty_q', 'has_new_wording', 'has_question_5',
    'text_sha1', 'text_file', 'last_changed_date',
]
LOG_COLUMNS = [
    'date', 'questionnaire_url', 'change',
    'had_loyalty_q', 'has_loyalty_q', 'had_new_wording', 'has_new_wording',
    'had_question_5', 'has_question_5', 'text_file',
]
# If fewer than this share of fetches succeed, something is wrong (e.g. we're
# being blocked) and the previous state is kept rather than overwritten.
MIN_OK_SHARE = 0.5


def flags(text):
    # Monster pages can spell the apostrophe as an HTML entity ("President&rsquo;s"),
    # which the patterns would otherwise miss.
    text = html.unescape(text)
    return {
        'has_loyalty_q': bool(LOYALTY_Q.search(text)),
        'has_new_wording': bool(NEW_WORDING.search(text)),
        'has_question_5': bool(QUESTION_5.search(text)),
    }


def open_postings_and_links(today):
    """Every posting still open on `today` (one row each), and its
    (usajobs_control_number, questionnaire_url) links."""
    jobs = pd.read_csv('all_jobs_clean.csv', low_memory=False, usecols=[
        'usajobs_control_number', 'hiring_agency', 'position_title', 'position_open_date', 'position_close_date'])
    jobs['position_open_date'] = pd.to_datetime(jobs['position_open_date'], format='mixed', errors='coerce')
    jobs['position_close_date'] = pd.to_datetime(jobs['position_close_date'], format='mixed', errors='coerce')
    close = jobs['position_close_date']
    jobs = jobs[close.isna() | (close >= today)].drop_duplicates('usajobs_control_number')
    links = pd.read_csv('questionnaire_links.csv', usecols=['usajobs_control_number', 'questionnaire_url'])
    links = links[links['usajobs_control_number'].isin(jobs['usajobs_control_number'])]
    links = links[~links['questionnaire_url'].isin(load_known_bad_urls())]
    return jobs, links.drop_duplicates()


def write_open_postings(jobs, status, path):
    """One row per open posting with its current flags. A posting counts as having a
    question if any of its questionnaires does; postings with no questionnaire link get
    questionnaire_found = False and blank flags."""
    per_q = status.assign(usajobs_control_number=status['usajobs_control_numbers'].str.split(';')).explode(
        'usajobs_control_number')
    per_q['usajobs_control_number'] = per_q['usajobs_control_number'].astype('int64')
    flag_cols = ['has_loyalty_q', 'has_new_wording', 'has_question_5']
    per_job = per_q.groupby('usajobs_control_number')[flag_cols].agg(lambda col: (col == 'True').any())
    out = jobs.merge(per_job, left_on='usajobs_control_number', right_index=True, how='left')
    out['questionnaire_found'] = out['has_loyalty_q'].notna()
    for col in flag_cols:
        out[col] = out[col].map({True: 'True', False: 'False'}).fillna('')
    out['usajobs_control_number'] = out['usajobs_control_number'].astype('int64')
    out['usajobs_link'] = 'https://www.usajobs.gov/job/' + out['usajobs_control_number'].astype(str)
    out['position_open_date'] = out['position_open_date'].dt.strftime('%Y-%m-%d')
    out['position_close_date'] = out['position_close_date'].dt.strftime('%Y-%m-%d')
    out = out.sort_values('usajobs_control_number')[[
        'usajobs_control_number', 'hiring_agency', 'position_title', 'position_open_date', 'position_close_date',
        'questionnaire_found', *flag_cols, 'usajobs_link']]
    out.to_csv(path, index=False)
    return out


def fetch_monster(url):
    for attempt in range(3):
        try:
            resp = requests.get(transform_monster_url(url), headers=BROWSER_HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if resp.status_code == 200:
            # Same cleanup extract_questionnaires.scrape_questionnaire applies to Monster pages.
            text = re.sub(r'<script[^>]*>.*?</script>', '', resp.text, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
            text = ' '.join(re.sub(r'<[^>]+>', ' ', text).split())
            return text if len(text) >= 500 else None
        time.sleep(2 * (attempt + 1))
    return None


def fetch(url):
    """Returns (fetch_status, text)."""
    if 'apply.usastaffing.gov/ViewQuestionnaire/' in url:
        text, _ = fetch_usastaffing_questionnaire(url)
    elif 'monstergovt.com' in url:
        text = fetch_monster(url)
    else:
        return 'skipped_blocked_domain', None
    return ('ok', text) if text is not None else ('failed', None)


def latest_stored_copy(url, out_dir):
    """Path of the most recent stored text for this questionnaire: a dated re-check, else raw_questionnaires."""
    name = get_questionnaire_filename(url)
    for day_dir in sorted((d for d in out_dir.iterdir() if d.is_dir()), reverse=True):
        if (day_dir / name).exists():
            return day_dir / name
    raw = RAW_QUESTIONNAIRES_DIR / name
    return raw if raw.exists() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--out-dir', default='questionnaire_rechecks')
    parser.add_argument('--limit', type=int, help='Only refresh this many questionnaires (for testing)')
    parser.add_argument('--workers', type=int, default=5)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date()
    status_path = out_dir / 'current_status.csv'
    log_path = out_dir / 'changes_log.csv'

    jobs, links = open_postings_and_links(pd.Timestamp(today))
    n_open = len(jobs)
    by_url = links.groupby('questionnaire_url')['usajobs_control_number'].apply(
        lambda ids: ';'.join(str(int(i)) for i in sorted(ids)))
    urls = sorted(by_url.index)
    if args.limit:
        urls = urls[:args.limit]
    print(f'{n_open:,} open postings; refreshing {len(urls):,} questionnaires', flush=True)

    previous = {}
    if status_path.exists():
        previous = pd.read_csv(status_path, dtype=str, keep_default_na=False).set_index('questionnaire_url').to_dict('index')

    with ThreadPoolExecutor(args.workers) as pool:
        fetched = list(pool.map(fetch, urls))

    attempted = [s for s, _ in fetched if s != 'skipped_blocked_domain']
    ok_share = attempted.count('ok') / len(attempted) if attempted else 1.0
    if ok_share < MIN_OK_SHARE:
        print(f'Only {ok_share:.0%} of fetches succeeded; keeping previous state.', file=sys.stderr)
        sys.exit(1)

    day_dir = out_dir / today.isoformat()
    rows, log_rows = [], []
    for url, (fetch_status, text) in zip(urls, fetched):
        prev = previous.get(url)
        stored = None
        if prev and prev['text_sha1']:
            before = {k: prev[k] == 'True' for k in ('has_loyalty_q', 'has_new_wording', 'has_question_5')}
        else:
            stored = latest_stored_copy(url, out_dir)
            before = flags(stored.read_text(encoding='utf-8', errors='ignore')) if stored else None

        row = {'questionnaire_url': url, 'usajobs_control_numbers': by_url[url], 'fetch_status': fetch_status}
        if text is None:
            # Not fetched today: keep the last known flags and text.
            carried = before or {'has_loyalty_q': False, 'has_new_wording': False, 'has_question_5': False}
            row.update({k: str(v) for k, v in carried.items()})
            row.update({
                'text_sha1': prev['text_sha1'] if prev else '',
                'text_file': prev['text_file'] if prev else (str(stored) if stored else ''),
                'last_changed_date': prev['last_changed_date'] if prev else '',
            })
            rows.append(row)
            continue

        now = flags(text)
        sha = hashlib.sha1(text.encode('utf-8')).hexdigest()
        if prev and prev['text_sha1']:
            changed = sha != prev['text_sha1']
        else:
            # First refresh of this questionnaire: record it only if it no longer matches its stored copy.
            changed = before is None or now != before

        if changed:
            day_dir.mkdir(exist_ok=True)
            text_file = day_dir / get_questionnaire_filename(url)
            if text_file.exists() and text_file.read_text(encoding='utf-8') != text:
                # Never overwrite a copy saved earlier the same day.
                text_file = text_file.with_name(f'{text_file.stem}_{datetime.now(timezone.utc):%H%M%S}.txt')
            text_file.write_text(text, encoding='utf-8')
            text_file, last_changed = str(text_file), today.isoformat()
            log_rows.append({
                'date': today.isoformat(), 'questionnaire_url': url,
                'change': 'changed' if before is not None else 'first_seen',
                **{f'had_{k[4:]}': (before or {}).get(k, '') for k in now},
                **now, 'text_file': text_file,
            })
        else:
            text_file = prev['text_file'] if prev else str(stored)
            last_changed = prev['last_changed_date'] if prev else ''
        row.update({k: str(v) for k, v in now.items()})
        row.update({'text_sha1': sha, 'text_file': text_file, 'last_changed_date': last_changed})
        rows.append(row)

    status = pd.DataFrame(rows, columns=STATUS_COLUMNS)
    postings = write_open_postings(jobs, status, out_dir / 'open_postings.csv')
    status.to_csv(status_path, index=False)
    if log_rows:
        pd.DataFrame(log_rows, columns=LOG_COLUMNS).to_csv(log_path, mode='a', index=False,
                                                           header=not log_path.exists())

    summary = {
        'date': today.isoformat(),
        'open_postings': n_open,
        'questionnaires': len(status),
        'fetched_ok': int((status['fetch_status'] == 'ok').sum()),
        'failed': int((status['fetch_status'] == 'failed').sum()),
        'skipped_blocked_domain': int((status['fetch_status'] == 'skipped_blocked_domain').sum()),
        'changed_today': len(log_rows),
        'with_loyalty_q': int((status['has_loyalty_q'] == 'True').sum()),
        'with_new_wording': int((status['has_new_wording'] == 'True').sum()),
        'with_question_5': int((status['has_question_5'] == 'True').sum()),
        'open_postings_with_loyalty_q': int((postings['has_loyalty_q'] == 'True').sum()),
        'open_postings_with_question_5': int((postings['has_question_5'] == 'True').sum()),
    }
    (out_dir / 'last_run.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
