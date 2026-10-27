"""Record which posting each new USAStaffing questionnaire belongs to.

Some postings don't link their questionnaire and have no number in their announcement
number to guess it from (e.g. ICE Health Service Corps). USAStaffing numbers
questionnaires in order (about 1,500-2,000 a day), and each one's public JSON names its
posting's control number, so this walks the new IDs each day and appends them to
questionnaire_rechecks/usastaffing_ids.csv (qid, control_number, close_date).
refresh_open_questionnaires.py checks the questionnaires listed there for open postings
that apply through USAStaffing and have no link; on 2026-10-27 a one-off scan like this
found questionnaires for 187 such postings.

Each run re-tries the last RESCAN IDs it doesn't have (an ID can be taken before its
questionnaire is published) and goes on until STOP_AFTER IDs in a row don't exist. It
stops early, keeping what it found, if USAStaffing starts refusing requests (403).
Rows whose close date is more than KEEP_DAYS past are dropped.
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from questionnaire_utils import BROWSER_HEADERS, USASTAFFING_API_URL

TABLE = Path(__file__).resolve().parent / 'questionnaire_rechecks' / 'usastaffing_ids.csv'
# Read by refresh_open_questionnaires.py, which warns when today's scan didn't finish.
LAST_SCAN = TABLE.with_name('usastaffing_ids_last_scan.json')
COLUMNS = ['qid', 'control_number', 'close_date']
RESCAN = 10_000
STOP_AFTER = 3_000
BATCH = 500
KEEP_DAYS = 90


def lookup(session, qid):
    """(qid, status, control_number, close_date); status is 200, 'missing', 403 or 'error'."""
    for attempt in range(4):
        try:
            r = session.get(USASTAFFING_API_URL.format(qid), headers={**BROWSER_HEADERS, 'Accept': 'application/json'},
                            timeout=30)
        except requests.RequestException:
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (400, 404):
            return qid, 'missing', None, None
        if r.status_code == 403:
            return qid, 403, None, None
        if r.status_code == 200:
            j = r.json()
            return qid, 200, str(j.get('controlNumber') or ''), (j.get('closeDate') or '')[:10]
        time.sleep(2 ** attempt)
    return qid, 'error', None, None


def scan(table, workers):
    """New rows for IDs past the table's last ones. Returns (rows, summary, blocked)."""
    known = set(table['qid'])
    qid = max(known) - RESCAN if known else None
    if qid is None:
        raise SystemExit(f'{TABLE} is empty; seed it with a one-off scan first')
    rows, misses, counts, sessions = [], 0, {}, [requests.Session() for _ in range(workers)]
    with ThreadPoolExecutor(workers) as pool:
        while misses < STOP_AFTER:
            batch = [q for q in range(qid, qid + BATCH) if q not in known]
            qid += BATCH
            results = list(pool.map(lambda q: lookup(sessions[q % workers], q), batch))
            for q, status, cn, close in results:
                counts[status] = counts.get(status, 0) + 1
                if status == 200:
                    rows.append({'qid': q, 'control_number': cn, 'close_date': close})
            if counts.get(403, 0) > 20:
                return rows, f'stopped at {qid:,}: USAStaffing refused requests (403) {counts}', True
            # Only IDs past the last known one count toward the end of the sequence.
            for q, status, _, _ in sorted(results):
                if q > max(known):
                    misses = misses + 1 if status == 'missing' else 0
    return rows, f'scanned to {qid:,}: {counts}', False


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    table = pd.read_csv(TABLE, dtype={'control_number': str, 'close_date': str}, keep_default_na=False)
    rows, summary, blocked = scan(table, args.workers)
    new = pd.DataFrame(rows, columns=COLUMNS)
    today = datetime.now(timezone.utc).date()  # UTC, as the refresh uses
    cutoff = (today - timedelta(days=KEEP_DAYS)).isoformat()
    out = pd.concat([table, new]).drop_duplicates('qid', keep='last')
    out = out[(out['close_date'] >= cutoff) | (out['close_date'] == '')].sort_values('qid')
    out.to_csv(TABLE, index=False)
    print(f'{summary}; {len(new):,} new questionnaires; table has {len(out):,} '
          f'(dropped those closed before {cutoff})', flush=True)
    LAST_SCAN.write_text(json.dumps({'date': today.isoformat(), 'finished': not blocked,
                                     'summary': summary, 'new': len(new)}, indent=2) + '\n')
    if blocked:
        sys.exit(2)


if __name__ == '__main__':
    main()
