#!/usr/bin/env python3
"""
Internal litigation-support report: Loyalty Q compliance tracking.

Not part of the public site pipeline (see ../generate_site) — this is a data
pull for the litigation team only, per Ori Lev's 2026-09-17 request. Produces:
  - daily new-postings counts (with / confirmed-without / no-link-found
    Loyalty Q) since a start date
  - a snapshot of currently-live postings with the Loyalty Q

A job's questionnaire may not have been scraped, or may not have had a
questionnaire link discoverable at all, so "no Loyalty Q" is split into
"confirmed_no_loyalty_q" (a questionnaire was scraped and doesn't have it)
and "no_questionnaire_link_found" (no link was ever found for this job, so
its status is unknown — this is NOT simply "pending"; most of this bucket
never resolves. See the "known coverage gaps" note below.)

Known coverage gaps in "no_questionnaire_link_found" (as of 2026-09-17):
  - Some jobs apply through an agency-specific system that isn't USAStaffing
    or Monster Government at all (e.g. Navy's navsea.recsolu.com) — genuinely
    unrecoverable via this pipeline.
  - Some jobs apply through an agency-branded USAStaffing-style portal on a
    different domain (e.g. FAA's jobs.faa.gov) that the link-finder doesn't
    recognize, AND whose fallback discovery never even runs because it's
    gated behind `usesUsastaffing` being true — a real, likely-fixable gap.
  - Even confirmed USAStaffing jobs sometimes fail both fallback heuristics
    (the announcement-number guess, ~89% hit rate, and the posting-HTML
    scrape, ~25% hit rate since USAJOBS pages are JS-rendered) — a smaller,
    likely-fixable gap.
  Fixed 2026-09-17: src/generate_site/extract_questionnaires.py's HTML-scrape
  fallback no longer requires usesUsastaffing, and now also recognizes known
  agency-branded portal URLs (see
  questionnaire_utils.discover_questionnaire_url_from_usajobs_posting()).
  The announcement-number-guess fallback's ~11% miss rate on confirmed
  USAStaffing jobs is unchanged and still a known gap.

Usage:
    cd src/litigation_report
    python loyalty_q_report.py [--start-date 2026-09-14] [--out-dir ./output]
"""
import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATE_SITE_DIR = REPO_ROOT / 'src' / 'generate_site'
sys.path.insert(0, str(GENERATE_SITE_DIR))

from questionnaire_utils import extract_questionnaire_id, RAW_QUESTIONNAIRES_DIR  # noqa: E402

# Same pattern generate_website_json.py uses to identify the Loyalty Q.
EO_PATTERN = re.compile(
    r"How would you help advance the President's Executive Orders and policy priorities in this role\?",
    re.IGNORECASE,
)

DEFAULT_START_DATE = '2026-09-14'
RAW_QUESTIONNAIRES_DIR = GENERATE_SITE_DIR / RAW_QUESTIONNAIRES_DIR


def regenerate_all_jobs_clean():
    """Rebuild all_jobs_clean.csv from the latest parquet files (same step the public pipeline runs)."""
    subprocess.run([sys.executable, 'generate_all_jobs_data.py'], check=True, cwd=GENERATE_SITE_DIR)


def find_questionnaire_ids_with_loyalty_q(raw_dir: Path) -> set:
    ids_with_q = set()
    for txt_file in raw_dir.glob('*.txt'):
        try:
            content = txt_file.read_text(encoding='utf-8')
        except Exception:
            continue
        if EO_PATTERN.search(content):
            ids_with_q.add(txt_file.stem.split('_', 1)[1])
    return ids_with_q


def load_job_level_flags() -> pd.DataFrame:
    """One row per job (usajobs_control_number) with a loyalty_q_status flag."""
    all_jobs_df = pd.read_csv(GENERATE_SITE_DIR / 'all_jobs_clean.csv', low_memory=False)
    links_df = pd.read_csv(GENERATE_SITE_DIR / 'questionnaire_links.csv')

    links_df['questionnaire_id'] = links_df['questionnaire_url'].apply(
        lambda url: extract_questionnaire_id(url)[1]
    )

    scraped_ids = {f.stem.split('_', 1)[1] for f in RAW_QUESTIONNAIRES_DIR.glob('*.txt')}
    ids_with_q = find_questionnaire_ids_with_loyalty_q(RAW_QUESTIONNAIRES_DIR)

    links_df['questionnaire_scraped'] = links_df['questionnaire_id'].isin(scraped_ids)
    links_df['has_loyalty_q'] = links_df['questionnaire_id'].isin(ids_with_q)

    # A job can have more than one questionnaire link scraped over time; prefer
    # a scraped row, and among scraped rows prefer one that shows the Loyalty Q.
    links_df = links_df.sort_values(
        ['usajobs_control_number', 'questionnaire_scraped', 'has_loyalty_q'],
        ascending=[True, False, False],
    )
    job_flags = links_df.drop_duplicates(subset='usajobs_control_number', keep='first')[
        ['usajobs_control_number', 'questionnaire_scraped', 'has_loyalty_q']
    ]

    jobs = all_jobs_df.merge(job_flags, on='usajobs_control_number', how='left')
    jobs['questionnaire_scraped'] = jobs['questionnaire_scraped'].fillna(False)
    jobs['has_loyalty_q'] = jobs['has_loyalty_q'].fillna(False)

    def status(row):
        if row['has_loyalty_q']:
            return 'has_loyalty_q'
        if row['questionnaire_scraped']:
            return 'confirmed_no_loyalty_q'
        return 'no_questionnaire_link_found'

    jobs['loyalty_q_status'] = jobs.apply(status, axis=1)
    jobs['position_open_date'] = pd.to_datetime(jobs['position_open_date'], format='mixed', errors='coerce')
    jobs['position_close_date'] = pd.to_datetime(jobs['position_close_date'], format='mixed', errors='coerce')
    return jobs


def _status_pivot(df: pd.DataFrame, group_col, prefix: str) -> pd.DataFrame:
    """Pivot loyalty_q_status counts by an arbitrary grouping column."""
    pivot = df.groupby([group_col, 'loyalty_q_status']).size().unstack(fill_value=0)
    for col in ('has_loyalty_q', 'confirmed_no_loyalty_q', 'no_questionnaire_link_found'):
        if col not in pivot.columns:
            pivot[col] = 0
    pivot[f'total_{prefix}'] = pivot[
        ['has_loyalty_q', 'confirmed_no_loyalty_q', 'no_questionnaire_link_found']
    ].sum(axis=1)
    pivot = pivot.rename(columns={
        'has_loyalty_q': f'{prefix}_with_loyalty_q',
        'confirmed_no_loyalty_q': f'{prefix}_confirmed_without_loyalty_q',
        'no_questionnaire_link_found': f'{prefix}_no_questionnaire_link_found',
    })
    return pivot[[
        f'total_{prefix}', f'{prefix}_with_loyalty_q',
        f'{prefix}_confirmed_without_loyalty_q', f'{prefix}_no_questionnaire_link_found',
    ]]


def daily_new_postings(jobs: pd.DataFrame, start_date: str) -> pd.DataFrame:
    start = pd.Timestamp(start_date)
    new_jobs = jobs[jobs['position_open_date'] >= start].copy()
    new_jobs['open_date'] = new_jobs['position_open_date'].dt.date
    return _status_pivot(new_jobs, 'open_date', 'new').reset_index().sort_values('open_date')


def agency_breakdown(jobs: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """New-postings breakdown by hiring agency, since start_date."""
    start = pd.Timestamp(start_date)
    new_jobs = jobs[jobs['position_open_date'] >= start].copy()
    new_jobs['hiring_agency'] = new_jobs['hiring_agency'].fillna('Not Specified')
    result = _status_pivot(new_jobs, 'hiring_agency', 'new').reset_index()
    return result.sort_values('total_new', ascending=False)


USAJOBS_POSTING_URL = 'https://www.usajobs.gov/job/{control_number}'


def new_postings_detail(jobs: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """One row per new posting since start_date, with a clickable USAJOBS link."""
    start = pd.Timestamp(start_date)
    new_jobs = jobs[jobs['position_open_date'] >= start].copy()
    new_jobs['usajobs_link'] = new_jobs['usajobs_control_number'].apply(
        lambda cid: USAJOBS_POSTING_URL.format(control_number=int(cid))
    )
    cols = [
        'position_open_date', 'hiring_agency', 'position_title',
        'usajobs_control_number', 'usajobs_link', 'loyalty_q_status',
    ]
    return new_jobs[cols].sort_values(['hiring_agency', 'position_open_date'])


def live_postings_snapshot(jobs: pd.DataFrame, as_of: pd.Timestamp) -> dict:
    live = jobs[(jobs['position_close_date'].isna()) | (jobs['position_close_date'] >= as_of)]
    return {
        'as_of': as_of.strftime('%Y-%m-%d'),
        'total_live_postings': int(len(live)),
        'live_with_loyalty_q': int((live['loyalty_q_status'] == 'has_loyalty_q').sum()),
        'live_confirmed_without_loyalty_q': int((live['loyalty_q_status'] == 'confirmed_no_loyalty_q').sum()),
        'live_no_questionnaire_link_found': int((live['loyalty_q_status'] == 'no_questionnaire_link_found').sum()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--start-date', default=DEFAULT_START_DATE)
    parser.add_argument('--out-dir', default=str(Path(__file__).resolve().parent / 'output'))
    parser.add_argument('--skip-rebuild', action='store_true',
                         help='Skip regenerating all_jobs_clean.csv from parquet (use the checked-in file as-is)')
    parser.add_argument('--no-pdf', action='store_true', help='Skip generating summary.pdf')
    args = parser.parse_args()

    if not args.skip_rebuild:
        print('Regenerating all_jobs_clean.csv from latest parquet files...')
        regenerate_all_jobs_clean()

    jobs = load_job_level_flags()

    as_of = pd.Timestamp(datetime.now(timezone.utc).date())
    daily_df = daily_new_postings(jobs, args.start_date)
    snapshot = live_postings_snapshot(jobs, as_of)
    agency_df = agency_breakdown(jobs, args.start_date)
    detail_df = new_postings_detail(jobs, args.start_date)

    pct = (
        snapshot['live_with_loyalty_q'] / snapshot['total_live_postings'] * 100
        if snapshot['total_live_postings'] else 0
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_csv = out_dir / 'daily_new_postings.csv'
    daily_df.to_csv(daily_csv, index=False)

    agency_csv = out_dir / 'agency_breakdown_since_start.csv'
    agency_df.to_csv(agency_csv, index=False)

    detail_csv = out_dir / 'new_postings_detail.csv'
    detail_df.to_csv(detail_csv, index=False)

    live_csv = out_dir / 'live_snapshot.csv'
    pd.DataFrame([{**snapshot, 'live_with_loyalty_q_pct_of_all_live': round(pct, 1)}]).to_csv(live_csv, index=False)

    summary_lines = [
        f"Loyalty Q litigation report — as of {snapshot['as_of']}",
        "",
        f"Daily new postings since {args.start_date}:",
        "",
        daily_df.to_string(index=False),
        "",
        f"New postings since {args.start_date}, by agency:",
        "",
        agency_df.to_string(index=False),
        "",
        f"Live postings snapshot as of {snapshot['as_of']}:",
        f"  total_live_postings: {snapshot['total_live_postings']:,}",
        f"  live_with_loyalty_q: {snapshot['live_with_loyalty_q']:,} ({pct:.1f}%)",
        f"  live_confirmed_without_loyalty_q: {snapshot['live_confirmed_without_loyalty_q']:,}",
        f"  live_no_questionnaire_link_found: {snapshot['live_no_questionnaire_link_found']:,}",
        "",
        "Note: live_no_questionnaire_link_found is NOT 'pending' — most of it is",
        "structurally unrecoverable (postings on non-USAStaffing agency systems).",
        "See loyalty_q_report.py's module docstring for known coverage gaps.",
        "",
        f"Per-posting detail with USAJOBS links for every new posting since "
        f"{args.start_date}: {detail_csv}",
    ]
    summary_txt = out_dir / 'summary.txt'
    summary_txt.write_text('\n'.join(summary_lines) + '\n')

    print('\n'.join(summary_lines))
    print(f"\nWrote: {daily_csv}\nWrote: {agency_csv}\nWrote: {detail_csv}"
          f"\nWrote: {live_csv}\nWrote: {summary_txt}")

    if not args.no_pdf:
        from render_pdf import render_pdf
        pdf_path = out_dir / 'summary.pdf'
        render_pdf(daily_df, agency_df, snapshot, args.start_date, pdf_path)
        print(f"Wrote: {pdf_path}")


if __name__ == '__main__':
    main()
