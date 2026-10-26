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
  link_changes_log.csv  Append-only. A row whenever an open posting's questionnaire
                      links change (link_added, link_removed) or a posting stops being
                      listed before its close date (no_longer_listed), and what was done
                      about it (kept_page_open, kept_page_unreadable, closed_early,
                      dropped_page_unreadable).

"Open" means USAJobs listed the posting in today's collection
(../../data/current_listing.csv, written by collect_current_data.py), or it was
open in the last run, its close date hasn't passed, and its own USAJobs page
doesn't say it closed. USAJobs search sometimes leaves out open postings for a
day (2026-10-23: 124 of them, 112 at the FAA), so a posting missing from the
listing is looked up on its page (usajobs_page_status) before it's dropped. Such
a posting stays in open_postings.csv with listed_today = False, and its
questionnaires keep their last re-checked flags (fetch_status
posting_not_listed in current_status.csv).

Every questionnaire row, and every open posting, has a checked_date: the day its
flags were last confirmed against the questionnaire itself. That is today for a
fetched questionnaire; for a blocked portal it is the last local re-check
(blocked_portal_checks.csv), which can be days old if that job didn't run.
last_run.json counts what was carried over and lists warnings, which the daily
workflow turns into an issue and the daily email shows at the top. Each open
posting's questionnaires are found from scratch every day, the same way a new
posting's are (questionnaire_utils.questionnaire_links_for_posting): links in its
text today, else a guess from its announcement number, else a link on its rendered
USAJobs page. Inferred USAStaffing questionnaires count only if their own posting or
announcement number matches.

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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from job_fields import load_questionnaire_links  # noqa: E402
from questionnaire_utils import (  # noqa: E402
    questionnaire_links_for_posting,
    BROWSER_HEADERS,
    RAW_QUESTIONNAIRES_DIR,
    fetch_usastaffing_questionnaire_json,
    get_questionnaire_filename,
    load_known_bad_urls,
    questionnaire_json_to_text,
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

# Looser phrases for each tracked question. A questionnaire that hits one of these but not
# the exact wording above may use a reworded version, so its full question text is saved as
# possible_variant for a person to judge. (Checked 2026-09-30: every questionnaire with the
# exact Executive Orders wording hits all of the first four; no open questionnaire hit any
# phrase without the exact wording.)
VARIANT_PHRASES = [
    ('Executive Orders question', LOYALTY_Q, re.compile(
        r"Executive\s+Orders?\W+and\W+policy\s+priorities"
        r"|policy\s+priorities\s+in\s+this\s+role"
        r"|advance\s+the\s+President"
        r"|President\W{0,3}s\s+Executive\s+Orders?"
        r"|(?:President|Administration)\W{0,3}s\s+(?:policy\s+)?(?:priorities|agenda)", re.IGNORECASE)),
    ('Question 5', QUESTION_5, re.compile(
        r"current\s+priorities,?\s+including"
        r"|agency[-\s]wide\s+priorities"
        r"|advance\s+(?:those|its|the\s+agency\W{0,3}s)\s+priorities"
        r"|mission\s+and\s+(?:current\s+)?priorities", re.IGNORECASE)),
]

STATUS_COLUMNS = [
    'questionnaire_url', 'usajobs_control_numbers', 'fetch_status',
    'has_loyalty_q', 'has_new_wording', 'has_question_5',
    'text_sha1', 'text_file', 'last_changed_date', 'possible_variant', 'checked_date',
]
LOG_COLUMNS = [
    'date', 'questionnaire_url', 'change',
    'had_loyalty_q', 'has_loyalty_q', 'had_new_wording', 'has_new_wording',
    'had_question_5', 'has_question_5', 'text_file',
]
# If fewer than this share of fetches succeed, something is wrong (e.g. we're
# being blocked) and the previous state is kept rather than overwritten.
MIN_OK_SHARE = 0.5


def possible_variants(text):
    """Full text of any question that looks like a tracked question but doesn't match its
    exact wording, as 'Executive Orders question: <text>' (several joined by ' || ')."""
    text = html.unescape(text)
    found = []
    for label, exact, loose in VARIANT_PHRASES:
        if exact.search(text):
            continue
        m = loose.search(text)
        if not m:
            continue
        # The whole question: its line if the text has one question per line, otherwise
        # from the previous question's end to this one's question mark.
        line_start = text.rfind('\n', 0, m.start()) + 1
        line_end = text.find('\n', m.end())
        line_end = len(text) if line_end == -1 else line_end
        if line_end - line_start <= 1200:
            snippet = text[line_start:line_end]
        else:
            start = max(text.rfind('?', 0, m.start()) + 1, m.start() - 600)
            end = text.find('?', m.end())
            end = min(len(text), (end + 1 if end != -1 else m.end() + 400), m.end() + 600)
            snippet = text[start:end]
        found.append(f'{label}: ' + ' '.join(snippet.split()))
    return ' || '.join(found)


def flags(text):
    # Monster pages can spell the apostrophe as an HTML entity ("President&rsquo;s"),
    # which the patterns would otherwise miss.
    text = html.unescape(text)
    return {
        'has_loyalty_q': bool(LOYALTY_Q.search(text)),
        'has_new_wording': bool(NEW_WORDING.search(text)),
        'has_question_5': bool(QUESTION_5.search(text)),
    }


CURRENT_LISTING = Path('../../data/current_listing.csv')
LINK_LOG_COLUMNS = ['date', 'usajobs_control_number', 'change', 'questionnaire_url', 'position_close_date']

USAJOBS_JOB_PAGE = 'https://www.usajobs.gov/job/{}'
# A posting missing from the listing whose page can't be read is kept this many days, then dropped.
MAX_UNREADABLE_DAYS = 3
# Warnings in last_run.json (and so an issue and a banner in the email) when at least this
# many open postings are missing from the listing, or this many questionnaires failed to fetch.
WARN_NOT_LISTED = 25
WARN_FAILED = 100


def usajobs_page_status(cn, session=None):
    """'closed' if the posting's own USAJobs page says it has closed, 'open' if the page
    shows the posting without saying so, else 'unreadable'. (2026-10-26: 12 Department of
    Transportation postings missing from the listing said "This job announcement has
    closed", closed early on 10/21-22; 2 open postings' pages had "This job is open to"
    and no such line.)"""
    try:
        resp = (session or requests).get(USAJOBS_JOB_PAGE.format(cn), headers=BROWSER_HEADERS, timeout=30)
    except requests.RequestException:
        return 'unreadable'
    if resp.status_code != 200:
        return 'unreadable'
    text = html.unescape(re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', resp.text)))
    if 'This job announcement has closed' in text:
        return 'closed'
    if 'This job is open to' in text:
        return 'open'
    return 'unreadable'


def unlisted_postings(today, previous_postings, listed_ids, page_status=usajobs_page_status, workers=5):
    """Postings that were open in the last run, aren't in today's listing, and whose close
    date hasn't passed. Each is looked up on its own USAJobs page:
      closed      -> dropped ('closed_early')
      open        -> kept ('kept_page_open')
      unreadable  -> kept ('kept_page_unreadable') until it has been out of the listing
                     MAX_UNREADABLE_DAYS days, then dropped ('dropped_page_unreadable').
    Returns (the kept rows of previous_postings, with listed_today 'False' and
    not_listed_since set; {control number: outcome} for every posting looked up)."""
    today_s = today.isoformat()
    prev = previous_postings.copy()
    if prev.empty:
        return prev, {}
    for col in ('listed_today', 'not_listed_since'):
        if col not in prev:
            prev[col] = ''
    cn = prev['usajobs_control_number'].astype('int64')
    candidates = prev[~cn.isin(listed_ids) & (prev['position_close_date'].fillna('') >= today_s)]
    if candidates.empty:
        return candidates, {}
    with ThreadPoolExecutor(workers) as pool:
        statuses = list(pool.map(page_status, candidates['usajobs_control_number'].astype('int64')))
    kept, outcomes = [], {}
    for (_, row), st in zip(candidates.iterrows(), statuses):
        since = row['not_listed_since'] or today_s
        days = (today - datetime.fromisoformat(since).date()).days
        n = int(row['usajobs_control_number'])
        if st == 'closed':
            outcomes[n] = 'closed_early'
        elif st == 'unreadable' and days >= MAX_UNREADABLE_DAYS:
            outcomes[n] = 'dropped_page_unreadable'
        else:
            outcomes[n] = f'kept_page_{st}'
            kept.append(row.to_dict() | {'listed_today': 'False', 'not_listed_since': since})
    return pd.DataFrame(kept, columns=list(prev.columns)), outcomes


def carry_status_rows(status, previous_status, kept_ids):
    """Today's status plus the questionnaires of postings kept although USAJobs didn't
    list them (unlisted_postings). Those aren't re-fetched: their last row is kept with
    fetch_status 'posting_not_listed', or, when a listed posting shares the
    questionnaire, the kept posting's number is added to today's row."""
    if not kept_ids or previous_status.empty:
        return status
    kept = {str(i) for i in kept_ids}
    status = status.copy()
    by_url = dict(zip(status['questionnaire_url'], status.index))
    extra = []
    for _, prev in previous_status.iterrows():
        ids = [i for i in str(prev['usajobs_control_numbers']).split(';') if i in kept]
        if not ids:
            continue
        if prev['questionnaire_url'] in by_url:
            i = by_url[prev['questionnaire_url']]
            have = status.at[i, 'usajobs_control_numbers'].split(';')
            status.at[i, 'usajobs_control_numbers'] = ';'.join(sorted(set(have) | set(ids), key=int))
        else:
            extra.append(prev.to_dict() | {'usajobs_control_numbers': ';'.join(ids),
                                           'fetch_status': 'posting_not_listed'})
    if extra:
        status = pd.concat([status, pd.DataFrame(extra).reindex(columns=status.columns, fill_value='')],
                           ignore_index=True)
    return status


def blocked_portal_check_dates(out_dir):
    """{questionnaire_url: date of its last successful local re-check} from blocked_portal_checks.csv."""
    path = out_dir / 'blocked_portal_checks.csv'
    if not path.exists():
        return {}
    log = pd.read_csv(path, dtype=str, keep_default_na=False)
    ok = log[log['status'] == 'ok']
    return ok.assign(day=ok['date'].str[:10]).groupby('questionnaire_url')['day'].max().to_dict()


def open_postings_and_links(today):
    """Every posting USAJobs lists today (one row each), and its
    (usajobs_control_number, questionnaire_url) links.

    Fails if today's listing is missing or from another day: the collector writes it
    only after a complete collection, and an old one would show pulled postings as open."""
    listing = pd.read_csv(CURRENT_LISTING, dtype=str, keep_default_na=False)
    listed_dates = set(listing['listed_date'])
    if listed_dates != {today.date().isoformat()}:
        raise SystemExit(f'{CURRENT_LISTING} is from {sorted(listed_dates)}, not {today.date()}; '
                         'the daily data update must complete first')
    listing['usajobs_control_number'] = listing['usajobs_control_number'].astype('int64')

    jobs = pd.read_csv('all_jobs_clean.csv', low_memory=False, usecols=[
        'usajobs_control_number', 'hiring_agency', 'position_title', 'position_open_date', 'position_close_date'])
    jobs['position_open_date'] = pd.to_datetime(jobs['position_open_date'], format='mixed', errors='coerce')
    jobs['position_close_date'] = pd.to_datetime(jobs['position_close_date'], format='mixed', errors='coerce')
    jobs = jobs.drop_duplicates('usajobs_control_number')
    listed = set(listing['usajobs_control_number'])
    # Listed postings outside all_jobs_clean.csv (e.g. opened before its cutoff date)
    # have no agency or title to report; count them so the gap is visible.
    not_in_job_data = len(listed - set(jobs['usajobs_control_number']))
    jobs = jobs[jobs['usajobs_control_number'].isin(listed)].copy()
    # The stored close date is the one first collected; agencies extend and shorten
    # postings, so use today's from the listing.
    close_today = pd.to_datetime(listing.set_index('usajobs_control_number')['position_close_date'],
                                 format='mixed', errors='coerce')
    jobs['position_close_date'] = jobs['usajobs_control_number'].map(close_today).fillna(jobs['position_close_date'])

    # Each posting's questionnaire links, found from scratch from today's posting the
    # same way a new posting's are (questionnaire_links_for_posting): links in its
    # text, else a guess from its announcement number, else a link on its rendered
    # USAJobs page. Nothing carries over from earlier runs, so links an agency added,
    # removed, or replaced show up as changes.
    listing = listing[listing['usajobs_control_number'].isin(set(jobs['usajobs_control_number']))]
    session = requests.Session()

    def find(row):
        links, by_announcement, by_page = questionnaire_links_for_posting(
            load_questionnaire_links(row.questionnaire_links), row.mentions_questionnaire == 'True',
            row.uses_usastaffing == 'True', row.announcement_number, row.position_uri, session=session)
        return [(row.usajobs_control_number, url, by_announcement or by_page, row.announcement_number) for url in links]

    with ThreadPoolExecutor(5) as pool:
        found = [link for links in pool.map(find, listing.itertuples(index=False)) for link in links]
    links = pd.DataFrame(found, columns=['usajobs_control_number', 'questionnaire_url', 'inferred', 'announcement_number'])
    print(f'{len(links):,} questionnaire links for {links["usajobs_control_number"].nunique():,} listed postings '
          f'({int(links["inferred"].sum()):,} inferred)', flush=True)

    # Postings whose text links a questionnaire (questionnaire_link_in_posting).
    has_direct = set(links.loc[~links['inferred'], 'usajobs_control_number'])

    # questionnaire_known_bad.txt holds links the scraper once judged broken, but
    # some are live questionnaires for the right posting (2 open postings with the
    # old question were hidden that way on 2026-09-29). Blacklisted USAStaffing
    # links are kept and verified in fetch() against the questionnaire's own
    # control number; blacklisted links on other sites can't be verified and are
    # dropped as before.
    blacklisted = links['questionnaire_url'].isin(load_known_bad_urls())
    usastaffing = links['questionnaire_url'].str.contains('apply.usastaffing.gov/ViewQuestionnaire/', regex=False)
    links = links[~blacklisted | usastaffing].assign(blacklisted=blacklisted)
    return jobs, links.drop_duplicates(['usajobs_control_number', 'questionnaire_url']), not_in_job_data, has_direct


def link_changes(today, previous_status, previous_postings, status, jobs, unlisted_outcomes=None):
    """Rows for link_changes_log.csv: compares today's open postings and their
    questionnaires with the previous run's. A posting kept although USAJobs didn't
    list it (unlisted_postings) is logged the day it drops out of the listing, with
    what was done about it, and again the day it's dropped."""
    unlisted_outcomes = unlisted_outcomes or {}
    if 'listed_today' in previous_postings:
        was_listed = previous_postings[previous_postings['listed_today'] != 'False']
        was_kept = set(previous_postings.loc[previous_postings['listed_today'] == 'False',
                                             'usajobs_control_number'].astype('int64'))
    else:
        was_listed, was_kept = previous_postings, set()
    def pairs(st):
        if st.empty:
            return set()
        ex = st.assign(cn=st['usajobs_control_numbers'].str.split(';')).explode('cn')
        return set(zip(ex['cn'].astype('int64'), ex['questionnaire_url']))

    before, now = pairs(previous_status), pairs(status)
    open_before = set(was_listed['usajobs_control_number'].astype('int64'))
    open_now = set(jobs['usajobs_control_number'])
    close = dict(zip(previous_postings['usajobs_control_number'].astype('int64'),
                     previous_postings['position_close_date']))
    close.update(zip(jobs['usajobs_control_number'], jobs['position_close_date'].dt.strftime('%Y-%m-%d')))
    rows = []
    for cn, url in sorted(before - now):
        if cn in open_now:
            rows.append((cn, 'link_removed', url))
    for cn, url in sorted(now - before):
        if cn in open_before:
            rows.append((cn, 'link_added', url))
    for cn in sorted(open_before - open_now):
        if close.get(cn, '') and close[cn] >= today.isoformat():
            rows.append((cn, 'no_longer_listed', ''))
            if cn in unlisted_outcomes:
                rows.append((cn, unlisted_outcomes[cn], ''))
    for cn in sorted(was_kept - open_now):
        # kept yesterday although unlisted; logged again only when dropped
        if unlisted_outcomes.get(cn, '').startswith(('closed', 'dropped')):
            rows.append((cn, unlisted_outcomes[cn], ''))
    return [{'date': today.isoformat(), 'usajobs_control_number': cn, 'change': change,
             'questionnaire_url': url, 'position_close_date': close.get(cn, '')} for cn, change, url in rows]


def write_open_postings(jobs, status, path, link_in_posting_ids, kept_unlisted=None):
    """One row per open posting with its current flags. A posting counts as having a
    question if any of its questionnaires does; postings with no questionnaire link get
    questionnaire_found = False and blank flags. questionnaire_link_in_posting is False
    when the posting's text has no questionnaire link and we inferred it (from the
    announcement number or the posting's rendered USAJobs page)."""
    per_q = status.assign(usajobs_control_number=status['usajobs_control_numbers'].str.split(';')).explode(
        'usajobs_control_number')
    per_q['usajobs_control_number'] = per_q['usajobs_control_number'].astype('int64')
    flag_cols = ['has_loyalty_q', 'has_new_wording', 'has_question_5']
    per_job = per_q.groupby('usajobs_control_number')[flag_cols].agg(lambda col: (col == 'True').any())
    per_job['possible_variant'] = per_q.groupby('usajobs_control_number')['possible_variant'].agg(
        lambda col: ' || '.join(sorted({v for v in col if v})))
    # The oldest check among a posting's questionnaires: its flags are only as fresh as that.
    per_job['checked_date'] = per_q.groupby('usajobs_control_number')['checked_date'].agg(
        lambda col: min((v for v in col if v), default=''))
    out = jobs.merge(per_job, left_on='usajobs_control_number', right_index=True, how='left')
    out['possible_variant'] = out['possible_variant'].fillna('')
    out['questionnaire_found'] = out['has_loyalty_q'].notna()
    out['questionnaire_link_in_posting'] = out['usajobs_control_number'].isin(link_in_posting_ids).map(
        {True: 'True', False: 'False'}).where(out['questionnaire_found'], '')
    for col in flag_cols:
        out[col] = out[col].map({True: 'True', False: 'False'}).fillna('')
    out['usajobs_control_number'] = out['usajobs_control_number'].astype('int64')
    out['usajobs_link'] = 'https://www.usajobs.gov/job/' + out['usajobs_control_number'].astype(str)
    out['position_open_date'] = out['position_open_date'].dt.strftime('%Y-%m-%d')
    out['position_close_date'] = out['position_close_date'].dt.strftime('%Y-%m-%d')
    out['checked_date'] = out['checked_date'].fillna('')
    kept = kept_unlisted if kept_unlisted is not None else pd.DataFrame(columns=['usajobs_control_number'])
    since = dict(zip(kept['usajobs_control_number'].astype('int64'), kept.get('not_listed_since', [])))
    out['listed_today'] = (~out['usajobs_control_number'].isin(since)).map({True: 'True', False: 'False'})
    out['not_listed_since'] = out['usajobs_control_number'].map(since).fillna('')
    out = out.sort_values('usajobs_control_number')[[
        'usajobs_control_number', 'hiring_agency', 'position_title', 'position_open_date', 'position_close_date',
        'questionnaire_found', *flag_cols, 'usajobs_link', 'possible_variant', 'questionnaire_link_in_posting',
        'listed_today', 'not_listed_since', 'checked_date']]
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


def fetch(url, linked_ids=(), verify=False, announcements=()):
    """Returns (fetch_status, text). A USAStaffing questionnaire that needs verifying
    (blacklisted, or inferred rather than linked in the posting) counts only if it
    belongs to one of the postings linking to it: its own control number is one of
    theirs, or its announcement number is (the same announcement re-posted).
    Otherwise the status is 'not_this_posting' (or 'invalid' if USAStaffing rejects
    the ID)."""
    if 'apply.usastaffing.gov/ViewQuestionnaire/' in url:
        raw, invalid = fetch_usastaffing_questionnaire_json(url)
        if verify and invalid:
            return 'invalid', None
        if verify and raw is not None:
            own = json.loads(raw)
            if (str(own.get('controlNumber')) not in linked_ids
                    and (own.get('announcementNumber') or '') not in announcements):
                return 'not_this_posting', None
        text = questionnaire_json_to_text(raw) if raw is not None else None
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


def run_warnings(summary, status, today):
    """Plain-English problems with today's data, for last_run.json: the daily workflow
    opens an issue and the daily email shows them at the top."""
    out = []
    n = summary['open_postings_not_listed_today']
    if n >= WARN_NOT_LISTED:
        out.append(f"{n:,} open postings ({summary['open_postings_not_listed_today_with_loyalty_q']:,} with the "
                   "Loyalty Q) weren't in today's USAJobs listing although their own pages don't say they "
                   "closed. They are counted as open with their last re-checked flags.")
    blocked = status[status['fetch_status'] == 'skipped_blocked_domain']
    stale = blocked[blocked['checked_date'] < (today - timedelta(days=1)).isoformat()]
    if len(stale):
        out.append(f"{len(stale):,} questionnaires on portals that block automated checks (e.g. jobs.faa.gov) "
                   f"were last checked on {min(stale['checked_date']) or 'an unknown date'}, not yesterday: the "
                   "local re-check (recheck_blocked_portals.py) didn't run. "
                   f"{summary['open_postings_with_loyalty_q_checked_before_yesterday']:,} open postings with the "
                   "Loyalty Q rest on an older check.")
    if summary['failed'] >= WARN_FAILED:
        out.append(f"{summary['failed']:,} questionnaires couldn't be fetched today; they keep their last "
                   "re-checked flags.")
    return out


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

    jobs, links, not_in_job_data, link_in_posting_ids = open_postings_and_links(pd.Timestamp(today))
    n_open = len(jobs)
    by_url = links.groupby('questionnaire_url')['usajobs_control_number'].apply(
        lambda ids: ';'.join(str(int(i)) for i in sorted(ids)))
    verify_urls = set(links.loc[links['blacklisted'] | links['inferred'], 'questionnaire_url'])
    announcements_by_url = links.groupby('questionnaire_url')['announcement_number'].agg(set)
    urls = sorted(by_url.index)
    if args.limit:
        urls = urls[:args.limit]
    print(f'{n_open:,} open postings; refreshing {len(urls):,} questionnaires', flush=True)

    previous_status = (pd.read_csv(status_path, dtype=str, keep_default_na=False) if status_path.exists()
                       else pd.DataFrame(columns=STATUS_COLUMNS))
    previous = previous_status.set_index('questionnaire_url').to_dict('index')
    postings_path = out_dir / 'open_postings.csv'
    previous_postings = (pd.read_csv(postings_path, dtype=str, keep_default_na=False) if postings_path.exists()
                         else pd.DataFrame(columns=['usajobs_control_number', 'position_close_date']))
    last_run_path = out_dir / 'last_run.json'
    previous_run_date = json.loads(last_run_path.read_text())['date'] if last_run_path.exists() else ''
    blocked_checked = blocked_portal_check_dates(out_dir)

    # Postings open last run that USAJobs doesn't list today, though their close date
    # hasn't passed: kept unless their own page says they closed.
    listed_ids = set(jobs['usajobs_control_number'])
    kept_unlisted, unlisted_outcomes = (unlisted_postings(today, previous_postings, listed_ids)
                                        if not args.limit else (previous_postings.iloc[0:0], {}))
    print(f'{len(unlisted_outcomes):,} postings open last run are missing from today\'s listing before their '
          f'close date: ' + json.dumps(pd.Series(unlisted_outcomes, dtype=str).value_counts().to_dict()), flush=True)

    with ThreadPoolExecutor(args.workers) as pool:
        fetched = list(pool.map(lambda u: fetch(u, by_url[u].split(';'), u in verify_urls, announcements_by_url[u]), urls))
    # Blacklisted links that turned out invalid or someone else's questionnaire
    # don't belong to these postings at all.
    kept = [(u, f) for u, f in zip(urls, fetched) if f[0] not in ('invalid', 'not_this_posting')]
    print(f'{len(verify_urls):,} blacklisted or inferred links verified; '
          f'{len(urls) - len(kept):,} excluded (invalid or another posting\'s questionnaire)', flush=True)
    urls, fetched = [u for u, _ in kept], [f for _, f in kept]

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
        # The day these flags were last confirmed against the questionnaire itself.
        prev_checked = (prev.get('checked_date') or (previous_run_date if prev['fetch_status'] == 'ok' else '')
                        if prev else '')
        locally_rechecked = None
        if fetch_status == 'skipped_blocked_domain':
            # Blocked portals are re-checked locally through a real browser
            # (recheck_blocked_portals.py), which saves each copy that changed in a
            # dated folder here. Use the newest stored copy, not yesterday's flags.
            latest = latest_stored_copy(url, out_dir)
            if latest is not None and (not prev or str(latest) != prev['text_file']):
                locally_rechecked = latest
                text = latest.read_text(encoding='utf-8', errors='ignore')
                if prev:
                    # Compare against what was reported last, not the new copy itself.
                    before = {k: prev[k] == 'True' for k in ('has_loyalty_q', 'has_new_wording', 'has_question_5')}
        if text is None:
            # Not fetched today: keep the last known flags and text.
            carried = before or {'has_loyalty_q': False, 'has_new_wording': False, 'has_question_5': False}
            row.update({k: str(v) for k, v in carried.items()})
            row.update({
                'text_sha1': prev['text_sha1'] if prev else '',
                'text_file': prev['text_file'] if prev else (str(stored) if stored else ''),
                'last_changed_date': prev['last_changed_date'] if prev else '',
            })
            last_text = Path(row['text_file']) if row['text_file'] else None
            row['possible_variant'] = (possible_variants(last_text.read_text(encoding='utf-8', errors='ignore'))
                                       if last_text and last_text.exists() else '')
            row['checked_date'] = ((blocked_checked.get(url) or prev_checked)
                                   if fetch_status == 'skipped_blocked_domain' else prev_checked)
            rows.append(row)
            continue

        now = flags(text)
        sha = hashlib.sha1(text.encode('utf-8')).hexdigest()
        if prev and prev['text_sha1']:
            changed = sha != prev['text_sha1']
        else:
            # First refresh of this questionnaire: record it only if it no longer matches its stored copy.
            changed = before is None or now != before

        if locally_rechecked is not None:
            # A newer copy saved by the local re-check; point at it.
            text_file, last_changed = str(locally_rechecked), today.isoformat()
            log_rows.append({
                'date': today.isoformat(), 'questionnaire_url': url,
                'change': 'changed' if before is not None else 'first_seen',
                **{f'had_{k[4:]}': (before or {}).get(k, '') for k in now},
                **now, 'text_file': text_file,
            })
        elif changed:
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
        row.update({'text_sha1': sha, 'text_file': text_file, 'last_changed_date': last_changed,
                    'possible_variant': possible_variants(text),
                    'checked_date': (blocked_checked.get(url) or prev_checked
                                     if fetch_status == 'skipped_blocked_domain' else today.isoformat())})
        rows.append(row)

    status = pd.DataFrame(rows, columns=STATUS_COLUMNS)
    # A --limit test run covers only some questionnaires, so it would log false removals.
    link_rows = [] if args.limit else link_changes(today, previous_status, previous_postings, status, jobs,
                                                   unlisted_outcomes)
    kept_ids = set(kept_unlisted['usajobs_control_number'].astype('int64'))
    status = carry_status_rows(status, previous_status.reindex(columns=STATUS_COLUMNS, fill_value=''), kept_ids)
    kept_jobs = pd.DataFrame({
        'usajobs_control_number': kept_unlisted['usajobs_control_number'].astype('int64'),
        'hiring_agency': kept_unlisted['hiring_agency'], 'position_title': kept_unlisted['position_title'],
        'position_open_date': pd.to_datetime(kept_unlisted['position_open_date'], errors='coerce'),
        'position_close_date': pd.to_datetime(kept_unlisted['position_close_date'], errors='coerce')})
    if 'questionnaire_link_in_posting' in kept_unlisted:
        link_in_posting_ids = set(link_in_posting_ids) | set(
            kept_jobs.loc[kept_unlisted['questionnaire_link_in_posting'].values == 'True', 'usajobs_control_number'])
    postings = write_open_postings(pd.concat([jobs, kept_jobs], ignore_index=True), status, postings_path,
                                   link_in_posting_ids, kept_unlisted)
    status.to_csv(status_path, index=False)
    if log_rows:
        pd.DataFrame(log_rows, columns=LOG_COLUMNS).to_csv(log_path, mode='a', index=False,
                                                           header=not log_path.exists())
    link_log_path = out_dir / 'link_changes_log.csv'
    if link_rows:
        pd.DataFrame(link_rows, columns=LINK_LOG_COLUMNS).to_csv(link_log_path, mode='a', index=False,
                                                                 header=not link_log_path.exists())

    summary = {
        'date': today.isoformat(),
        'open_postings': n_open,
        'listed_postings_not_in_job_data': not_in_job_data,
        'links_added': sum(r['change'] == 'link_added' for r in link_rows),
        'links_removed': sum(r['change'] == 'link_removed' for r in link_rows),
        'postings_no_longer_listed': sum(r['change'] == 'no_longer_listed' for r in link_rows),
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
        'open_postings_with_possible_variant': int((postings['possible_variant'] != '').sum()),
        # Open postings USAJobs didn't list today but whose pages don't say they closed
        # (counted in open_postings.csv, listed_today = False), and what happened to
        # the postings that dropped out of the listing.
        'open_postings_not_listed_today': int((postings['listed_today'] == 'False').sum()),
        'open_postings_not_listed_today_with_loyalty_q': int(
            ((postings['listed_today'] == 'False') & (postings['has_loyalty_q'] == 'True')).sum()),
        'unlisted_outcomes': pd.Series(unlisted_outcomes, dtype=str).value_counts().to_dict(),
        'blocked_portal_last_checked': max(blocked_checked.values(), default=''),
        # Open postings with the Loyalty Q whose flags rest on a check older than yesterday.
        'open_postings_with_loyalty_q_checked_before_yesterday': int(
            ((postings['has_loyalty_q'] == 'True')
             & (postings['checked_date'] < (today - timedelta(days=1)).isoformat())).sum()),
    }
    summary['warnings'] = run_warnings(summary, status, today)
    (out_dir / 'last_run.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
