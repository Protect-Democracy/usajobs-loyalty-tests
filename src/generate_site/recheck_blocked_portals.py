#!/usr/bin/env python3
"""Re-check open postings' questionnaires on portals that block automated access.

The daily refresh (refresh_open_questionnaires.py) can't fetch portals such as
jobs.faa.gov, which turn away scripted requests, and records them as
skipped_blocked_domain. This script fetches them through a real Chrome window
attached over the DevTools Protocol (see backfill_blocked_portal_questionnaires.py
for why that gets through), and records what each says now:

  questionnaire_rechecks/<YYYY-MM-DD>/<name>.txt  The text, when it differs from
                       the newest stored copy. The next daily refresh uses the
                       newest stored copy for these portals.
  questionnaire_rechecks/blocked_portal_checks.csv  Append-only. One row per
                       questionnaire per run, so there's a date each was last
                       verified even when nothing changed.

It runs locally (scheduled on a laptop), never in GitHub Actions. Exits 1 if any
questionnaire couldn't be fetched or the page didn't match its posting.

Usage:
    1. Start Chrome with remote debugging:
       "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
           --remote-debugging-port=9222 --user-data-dir="$HOME/chrome-debug"
    2. cd src/generate_site
       python3 recheck_blocked_portals.py
"""
import argparse
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

from questionnaire_utils import extract_questionnaire_id, get_questionnaire_filename
from refresh_open_questionnaires import flags, latest_stored_copy

BLOCK_PAGE = re.compile(r'access denied|just a moment|verifying you are human|ray id', re.IGNORECASE)
MIN_CHARS = 1500
LOG_COLUMNS = ['date', 'questionnaire_url', 'status', 'has_loyalty_q', 'has_new_wording', 'has_question_5',
               'text_file', 'saved_new_copy']


def fetch_page_text(page, url):
    """Returns (status, text). Status is 'ok', or why the page can't be trusted."""
    try:
        page.goto(url, wait_until='networkidle', timeout=60000)
        text = page.inner_text('body')
    except Exception as e:
        return f'error: {type(e).__name__}', None
    if len(text) < MIN_CHARS or BLOCK_PAGE.search(text[:3000]):
        return 'blocked_or_empty_page', None
    # The page must be this questionnaire: its ID appears in the announcement number.
    if extract_questionnaire_id(url)[1] not in text:
        return 'wrong_page', None
    return 'ok', text


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--out-dir', default='questionnaire_rechecks')
    parser.add_argument('--cdp-url', default='http://localhost:9222')
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    status = pd.read_csv(out_dir / 'current_status.csv', dtype=str, keep_default_na=False)
    urls = status.loc[status['fetch_status'] == 'skipped_blocked_domain', 'questionnaire_url'].tolist()
    today = datetime.now(timezone.utc).date().isoformat()
    print(f'{len(urls)} open questionnaires on blocked portals', flush=True)

    rows = []
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(args.cdp_url)
        page = browser.contexts[0].new_page()
        # A fixed size, so responsive layout (e.g. FAA's header spelling out
        # "AVIATOR") doesn't make the same questionnaire read as changed text.
        page.set_viewport_size({'width': 1280, 'height': 900})
        for url in urls:
            fetch_status, text = fetch_page_text(page, url)
            row = {'date': today, 'questionnaire_url': url, 'status': fetch_status, 'text_file': '',
                   'saved_new_copy': False}
            if text is not None:
                row.update({k: str(v) for k, v in flags(text).items()})
                latest = latest_stored_copy(url, out_dir)
                if latest is not None and latest.read_text(encoding='utf-8', errors='ignore') == text:
                    row['text_file'] = str(latest)
                else:
                    text_file = out_dir / today / get_questionnaire_filename(url)
                    if text_file.exists():
                        # Never overwrite a copy saved earlier the same day.
                        row['status'] = 'already_saved_today_with_different_text'
                    else:
                        text_file.parent.mkdir(exist_ok=True)
                        text_file.write_text(text, encoding='utf-8')
                        row.update({'text_file': str(text_file), 'saved_new_copy': True})
            rows.append(row)
            print(f"  {url}: {row['status']} loyalty_q={row.get('has_loyalty_q', '')}", flush=True)
            time.sleep(2 + random.random() * 2)
        page.close()

    log_path = out_dir / 'blocked_portal_checks.csv'
    pd.DataFrame(rows, columns=LOG_COLUMNS).to_csv(log_path, mode='a', index=False, header=not log_path.exists())
    failed = [r for r in rows if r['status'] != 'ok']
    print(f"{len(rows) - len(failed)} checked, {sum(r['saved_new_copy'] for r in rows)} with new text, "
          f"{len(failed)} failed", flush=True)
    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
