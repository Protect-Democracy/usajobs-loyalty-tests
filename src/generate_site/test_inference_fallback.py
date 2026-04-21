#!/usr/bin/env python3
"""Exercise the announcement-number inference fallback end-to-end.

Iterates parquet jobs, collects URLs produced only by the announcement-number
fallback (not direct API URLs), attempts to scrape each one, and writes a
GitHub Actions step summary. Fails loudly if the AUSA control number 852814700
does not produce an inferred URL, or if zero inferred URLs scrape successfully.
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd

from extract_questionnaires import (
    extract_questionnaire_links_from_job,
    scrape_questionnaire,
)
from questionnaire_utils import get_questionnaire_filepath, RAW_QUESTIONNAIRES_DIR

AUSA_CONTROL_NUMBER = 852814700
AUSA_EXPECTED_URL = 'https://apply.usastaffing.gov/ViewQuestionnaire/12855055'

# Cap how many inferred URLs we actually try to scrape in a single CI run.
# The full list could be thousands; we just need a signal that the path works.
MAX_SCRAPE = int(os.environ.get('MAX_INFERRED_SCRAPES') or '50')


def write_summary(lines):
    """Append markdown lines to $GITHUB_STEP_SUMMARY (or stdout locally)."""
    path = os.environ.get('GITHUB_STEP_SUMMARY')
    text = '\n'.join(lines) + '\n'
    if path:
        with open(path, 'a') as f:
            f.write(text)
    else:
        print('\n--- SUMMARY ---')
        print(text)


def main():
    data_dir = Path(os.environ.get('DATA_DIR', '../../data'))
    parquet_files = sorted(data_dir.glob('current_jobs_*.parquet')) + \
                    sorted(data_dir.glob('historical_jobs_*.parquet'))

    if not parquet_files:
        print(f'ERROR: no parquet files found in {data_dir}', file=sys.stderr)
        sys.exit(2)

    print(f'Scanning {len(parquet_files)} parquet files for inferred URLs...')

    total_jobs = 0
    direct_link_jobs = 0
    inferred_jobs = 0
    no_link_jobs = 0
    inferred_records = []  # list of dicts
    seen_urls = set()
    ausa_found_url = None

    for pf in parquet_files:
        df = pd.read_parquet(pf)
        for _, row in df.iterrows():
            total_jobs += 1
            links, *_rest, inferred = extract_questionnaire_links_from_job(row)

            if not links:
                no_link_jobs += 1
                continue

            if inferred:
                inferred_jobs += 1
                for link in links:
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    inferred_records.append({
                        'url': link,
                        'control_number': row.get('usajobsControlNumber'),
                        'announcement': row.get('announcementNumber'),
                        'title': row.get('positionTitle'),
                        'agency': row.get('hiringAgencyName'),
                    })
                    if row.get('usajobsControlNumber') == AUSA_CONTROL_NUMBER:
                        ausa_found_url = link
            else:
                direct_link_jobs += 1

    print(f'Total jobs:          {total_jobs:,}')
    print(f'  Direct-link jobs:  {direct_link_jobs:,}')
    print(f'  Inferred-link jobs:{inferred_jobs:,}')
    print(f'  No-link jobs:      {no_link_jobs:,}')
    print(f'Unique inferred URLs: {len(inferred_records):,}')
    print(f'AUSA (852814700) inferred URL: {ausa_found_url}')

    # --- Loud assertions ---
    errors = []
    if ausa_found_url != AUSA_EXPECTED_URL:
        errors.append(
            f'AUSA control number {AUSA_CONTROL_NUMBER} did not produce expected '
            f'URL {AUSA_EXPECTED_URL} (got: {ausa_found_url!r})'
        )

    if not inferred_records:
        errors.append('No inferred URLs found at all — the fallback did not fire')

    # --- Scrape a capped sample to verify real success ---
    RAW_QUESTIONNAIRES_DIR.mkdir(parents=True, exist_ok=True)

    sample = inferred_records[:MAX_SCRAPE]
    # Ensure AUSA is in the sample if it was found
    if ausa_found_url and not any(r['url'] == ausa_found_url for r in sample):
        sample = [r for r in inferred_records if r['url'] == ausa_found_url][:1] + sample[:-1]

    print(f'\nScraping {len(sample)} inferred URLs (cap={MAX_SCRAPE})...')

    scrape_results = []
    t0 = time.time()
    for i, rec in enumerate(sample, 1):
        url = rec['url']
        print(f'[{i}/{len(sample)}] {url}')
        try:
            text = scrape_questionnaire(url, str(RAW_QUESTIONNAIRES_DIR), timeout_seconds=45, headless=True)
            ok = text is not None and len(text) >= 500
        except Exception as e:
            print(f'    exception: {e}')
            ok = False
        scrape_results.append({**rec, 'scrape_ok': ok})

    elapsed = time.time() - t0
    ok_count = sum(1 for r in scrape_results if r['scrape_ok'])
    fail_count = len(scrape_results) - ok_count
    print(f'\nScraped {ok_count}/{len(scrape_results)} successfully in {elapsed:.1f}s')

    ausa_scrape_ok = any(
        r['scrape_ok'] and r['url'] == AUSA_EXPECTED_URL for r in scrape_results
    )

    if scrape_results and ok_count == 0:
        errors.append('All inferred URLs failed to scrape — something is wrong with scraping, not just inference')

    # --- Write GitHub step summary ---
    lines = []
    lines.append('## Questionnaire inference fallback — test run')
    lines.append('')
    lines.append(f'- Parquet files scanned: **{len(parquet_files)}**')
    lines.append(f'- Total jobs scanned: **{total_jobs:,}**')
    lines.append(f'- Jobs with direct API links: **{direct_link_jobs:,}**')
    lines.append(f'- Jobs with inferred-from-announcement links: **{inferred_jobs:,}**')
    lines.append(f'- Jobs with no questionnaire link: **{no_link_jobs:,}**')
    lines.append(f'- Unique inferred URLs: **{len(inferred_records):,}**')
    lines.append('')
    lines.append('### Scrape sample')
    lines.append(f'- URLs attempted: **{len(scrape_results)}** (cap: {MAX_SCRAPE})')
    lines.append(f'- Scraped OK: **{ok_count}**')
    lines.append(f'- Failed: **{fail_count}**')
    if scrape_results:
        lines.append(f'- Success rate: **{ok_count / len(scrape_results) * 100:.1f}%**')
    lines.append('')
    lines.append('### AUSA target (control 852814700)')
    lines.append(f'- Inferred URL produced: `{ausa_found_url}`')
    lines.append(f'- Expected: `{AUSA_EXPECTED_URL}`')
    lines.append(f'- Match: **{"✅" if ausa_found_url == AUSA_EXPECTED_URL else "❌"}**')
    lines.append(f'- Scrape OK: **{"✅" if ausa_scrape_ok else "❌"}**')
    lines.append('')
    if fail_count:
        lines.append('### Failed URLs (first 20)')
        lines.append('')
        lines.append('| URL | Announcement | Control # | Agency |')
        lines.append('| --- | --- | --- | --- |')
        for r in [r for r in scrape_results if not r['scrape_ok']][:20]:
            lines.append(f"| `{r['url']}` | {r['announcement']} | {r['control_number']} | {r['agency']} |")
        lines.append('')
    if errors:
        lines.append('### ❌ Errors')
        for e in errors:
            lines.append(f'- {e}')
    else:
        lines.append('### ✅ All assertions passed')

    write_summary(lines)

    if errors:
        print('\nERRORS:', file=sys.stderr)
        for e in errors:
            print(f'  - {e}', file=sys.stderr)
        sys.exit(1)

    print('\nAll assertions passed.')


if __name__ == '__main__':
    main()
