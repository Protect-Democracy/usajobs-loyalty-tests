#!/usr/bin/env python3
"""
Generate JSON data for the Essay Question analysis web interface
"""
import pandas as pd
import json
import os
from pathlib import Path
import html
import re
from datetime import datetime
from questionnaire_utils import (
    transform_monster_url, extract_questionnaire_id, get_questionnaire_filename,
    RAW_QUESTIONNAIRES_DIR, QUESTIONNAIRE_LINKS_CSV
)

# Define paths
BASE_DIR = Path('../..')
QUESTIONNAIRE_DIR = Path('.')
PUBLIC_DIR = Path('../public')
DATA_DIR = BASE_DIR / 'data'

def calculate_eo_stats(all_jobs_df, scraped_df, group_column, top_n=None, column_name=None):
    """Calculate EO question statistics for any grouping column"""
    # Fill NaN values with a placeholder to ensure they're included in groupby
    all_jobs_df = all_jobs_df.copy()
    scraped_df = scraped_df.copy()
    all_jobs_df[group_column] = all_jobs_df[group_column].fillna('Not Specified')
    scraped_df[group_column] = scraped_df[group_column].fillna('Not Specified')
    
    # Get total job counts from the full dataset
    total_stats = all_jobs_df.groupby(group_column).size().reset_index(name='Total Jobs')
    
    # For scraped data, count unique jobs (not questionnaire rows)
    # First deduplicate by usajobs_control_number to get one row per job
    scraped_jobs = scraped_df.drop_duplicates(subset='usajobs_control_number')
    
    # Get scraped counts and essay question counts
    scraped_stats = scraped_jobs.groupby(group_column).agg({
        'has_executive_order': 'sum',
        'usajobs_control_number': 'count'
    }).reset_index()
    scraped_stats.columns = [group_column, 'Jobs with Essay Question', 'Jobs with Questionnaires']
    
    # Merge the stats
    stats = pd.merge(total_stats, scraped_stats, on=group_column, how='left')
    stats = stats.fillna(0)
    
    # Use provided column name or default to the group column name
    display_name = column_name if column_name else group_column
    stats.rename(columns={group_column: display_name}, inplace=True)
    
    # Calculate percentage (of jobs with questionnaires that have essay question)
    stats['% with Essay Question'] = stats.apply(
        lambda row: round(row['Jobs with Essay Question'] / row['Jobs with Questionnaires'] * 100, 1) 
        if row['Jobs with Questionnaires'] > 0 else 0, axis=1
    )
    
    # Convert to int for cleaner display
    stats['Total Jobs'] = stats['Total Jobs'].astype(int)
    stats['Jobs with Questionnaires'] = stats['Jobs with Questionnaires'].astype(int)
    stats['Jobs with Essay Question'] = stats['Jobs with Essay Question'].astype(int)
    
    # Reorder columns to match desired order
    stats = stats[[display_name, 'Total Jobs', 'Jobs with Questionnaires', 
                   'Jobs with Essay Question', '% with Essay Question']]
    
    if top_n:
        stats = stats.nlargest(top_n, 'Total Jobs')
    
    return stats.sort_values('Total Jobs', ascending=False).reset_index(drop=True)

def check_executive_order_mentions(questionnaire_dir=RAW_QUESTIONNAIRES_DIR):
    """Check which questionnaires mention the specific executive order question"""
    mentions = {}
    # Straight or curly apostrophe; Monster pages can also spell it as an HTML entity
    # ("President&rsquo;s"), so entities are decoded before matching.
    pattern = re.compile(r"How would you help advance the President['\u2019]s Executive Orders and policy priorities in this role\?", re.IGNORECASE)
    
    if not questionnaire_dir.exists():
        print(f"Warning: {questionnaire_dir} does not exist")
        return mentions
    
    txt_files = list(questionnaire_dir.glob('*.txt'))
    print(f"Found {len(txt_files):,} scraped questionnaire files")
    
    # An unreadable file stops the build: skipping it would count that posting as
    # not having the question.
    for txt_file in txt_files:
        with open(txt_file, 'r', encoding='utf-8') as f:
            content = f.read()
        if pattern.search(html.unescape(content)):
            file_id = txt_file.stem.split('_')[1]
            mentions[file_id] = 1
    
    return mentions


RECHECK_DIR = Path('questionnaire_rechecks')
RECHECK_RESULT_FLAGS = {'still old wording': True, 'switched to new wording': False, 'neither wording': False}


def latest_rechecked_eo_flags(recheck_dir=RECHECK_DIR):
    """Latest known essay-question flag per questionnaire_url, from re-fetches made
    after the first scrape. raw_questionnaires/ only has each questionnaire as first
    scraped, and agencies have since edited many to drop the question.

    Applied oldest to newest, so later checks win: the 2026-09-28 one-off re-check
    (recheck_log.csv), then the daily refresh's changes_log.csv, then
    current_status.csv (every questionnaire of a currently-open posting, as of the
    latest refresh). Questionnaires none of these cover aren't in the result and
    keep their first-scrape flag."""
    flags = {}
    recheck_log = recheck_dir / 'recheck_log.csv'
    if recheck_log.exists():
        log = pd.read_csv(recheck_log, dtype=str, keep_default_na=False)
        # 'rescrape failed' told us nothing, so it's left out.
        log = log[log['result'].isin(RECHECK_RESULT_FLAGS)]
        flags.update(zip(log['questionnaire_url'], log['result'].map(RECHECK_RESULT_FLAGS)))
    for name in ['changes_log.csv', 'current_status.csv']:
        path = recheck_dir / name
        if not path.exists():
            print(f"Warning: {path} does not exist")
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df = df[df['has_loyalty_q'].isin(['True', 'False'])]
        if 'date' in df.columns:
            df = df.sort_values('date', kind='stable')
        flags.update(zip(df['questionnaire_url'], df['has_loyalty_q'] == 'True'))
    return flags


def build_analysis(all_jobs_df, scraped_df, scraped_df_in_current, links_df):
    """Overview numbers and the service/grade/location/agency/occupation/timeline
    tables for one set of postings. main() calls it for all postings and again for
    the currently open ones."""
    all_jobs_df, scraped_df, scraped_df_in_current, links_df = (
        all_jobs_df.copy(), scraped_df.copy(), scraped_df_in_current.copy(), links_df.copy())

    # Calculate overall statistics based on unique jobs
    total_jobs_with_questionnaires = len(scraped_df)
    total_jobs_with_eo = int(scraped_df['has_executive_order'].sum())
    percentage_with_eo = (total_jobs_with_eo / total_jobs_with_questionnaires * 100) if total_jobs_with_questionnaires > 0 else 0
    
    # Get date range
    if 'position_open_date' in scraped_df.columns:
        scraped_df['position_open_date'] = pd.to_datetime(scraped_df['position_open_date'], format='mixed')
        earliest_date = scraped_df['position_open_date'].min()
        latest_date = scraped_df['position_open_date'].max()
        data_coverage = f"Analyzing questionnaires from federal job postings from {earliest_date.strftime('%B %d, %Y')} to {latest_date.strftime('%B %d, %Y')}"
    else:
        data_coverage = "Date information not available"
    
    # Prepare the data structure
    analysis_data = {
        'overview': {
            'total_jobs': len(all_jobs_df),
            'total_jobs_with_questionnaires': total_jobs_with_questionnaires,
            'total_jobs_with_eo': total_jobs_with_eo,
            'percentage_with_eo': round(percentage_with_eo, 1),
            'data_coverage': data_coverage
        }
    }
    
    # Add grade_level column to scraped dataframe (using grade_code as-is)
    if 'grade_code' in links_df.columns:
        links_df['grade_level'] = links_df['grade_code'].fillna('Not Specified')
        scraped_df['grade_level'] = scraped_df['grade_code'].fillna('Not Specified')
        scraped_df_in_current['grade_level'] = scraped_df_in_current['grade_code'].fillna('Not Specified')
    
    # Add occupation_full column to both dataframes - pad occupation series to 4 digits
    links_df['occupation_full'] = links_df['occupation_series'].astype(str).str.zfill(4) + ' - ' + links_df['occupation_name'].fillna('Unknown')
    scraped_df['occupation_full'] = scraped_df['occupation_series'].astype(str).str.zfill(4) + ' - ' + scraped_df['occupation_name'].fillna('Unknown')
    scraped_df_in_current['occupation_full'] = scraped_df_in_current['occupation_series'].astype(str).str.zfill(4) + ' - ' + scraped_df_in_current['occupation_name'].fillna('Unknown')
    
    # Service Type Analysis
    if 'service_type' in scraped_df.columns and 'service_type' in all_jobs_df.columns:
        service_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'service_type', column_name='Service Type')
        analysis_data['service_analysis'] = service_stats.to_dict('records')
    else:
        analysis_data['service_analysis'] = []
    
    # Grade Level Analysis
    if 'grade_level' in scraped_df.columns and 'grade_level' in all_jobs_df.columns:
        grade_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'grade_level', top_n=None, column_name='Grade Level')
        analysis_data['grade_analysis'] = grade_stats.to_dict('records')
    else:
        analysis_data['grade_analysis'] = []
    
    # Location Analysis
    if 'position_location' in scraped_df.columns and 'position_location' in all_jobs_df.columns:
        location_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'position_location', top_n=None, column_name='Location')
        analysis_data['location_analysis'] = location_stats.to_dict('records')
    else:
        analysis_data['location_analysis'] = []
    
    # Agency Analysis
    if 'hiring_agency' in all_jobs_df.columns:
        agency_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'hiring_agency', top_n=None, column_name='Agency')
        analysis_data['agency_analysis'] = agency_stats.to_dict('records')
    else:
        analysis_data['agency_analysis'] = []
    
    # Occupation Analysis
    # DEBUG: sample occupation_full values from each side before normalization
    print("\n=== DEBUG: occupation_full pre-normalization ===")
    print(f"all_jobs_df['occupation_series'] dtype: {all_jobs_df['occupation_series'].dtype}")
    print(f"all_jobs_df['occupation_full'] sample: {all_jobs_df['occupation_full'].dropna().head(5).tolist()}")
    print(f"scraped_df['occupation_full'] sample: {scraped_df['occupation_full'].dropna().head(5).tolist()}")
    all_set = set(all_jobs_df['occupation_full'].dropna().unique())
    scr_set = set(scraped_df['occupation_full'].dropna().unique())
    print(f"unique occupation_full in all_jobs_df: {len(all_set)}, in scraped_df: {len(scr_set)}, overlap: {len(all_set & scr_set)}")
    print("=== END DEBUG ===\n")

    # Normalize occupation_full on all_jobs_df using the same zfill derivation as scraped_df,
    # so a merge on occupation_full finds matches regardless of how pandas inferred
    # occupation_series dtype when reading the CSV.
    if 'occupation_series' in all_jobs_df.columns and 'occupation_name' in all_jobs_df.columns:
        all_jobs_df['occupation_full'] = (
            all_jobs_df['occupation_series'].astype(str).str.zfill(4)
            + ' - '
            + all_jobs_df['occupation_name'].fillna('Unknown')
        )

    if 'occupation_full' in all_jobs_df.columns:
        occupation_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'occupation_full', top_n=None, column_name='Occupation Series')
        analysis_data['occupation_analysis'] = occupation_stats.to_dict('records')
    else:
        analysis_data['occupation_analysis'] = []
    
    # Timeline Analysis - use all_jobs_df to be consistent with other analyses
    if 'position_open_date' in all_jobs_df.columns:
        all_jobs_df['open_date'] = pd.to_datetime(all_jobs_df['position_open_date'], format='mixed', errors='coerce')
        jobs_with_dates = all_jobs_df[all_jobs_df['open_date'].notna()].copy()
        
        # Get the scraped and EO status for each job
        jobs_with_dates['has_questionnaire'] = jobs_with_dates['usajobs_control_number'].isin(scraped_df_in_current['usajobs_control_number'])
        jobs_with_dates['has_executive_order'] = jobs_with_dates['usajobs_control_number'].isin(
            scraped_df[scraped_df['has_executive_order']]['usajobs_control_number']
        )
        
        # Extract date components
        jobs_with_dates['year'] = jobs_with_dates['open_date'].dt.year
        jobs_with_dates['month'] = jobs_with_dates['open_date'].dt.month
        jobs_with_dates['month_name'] = jobs_with_dates['open_date'].dt.strftime('%B %Y')
        jobs_with_dates['week_of_month'] = jobs_with_dates['open_date'].dt.day.apply(lambda d: (d-1)//7 + 1)
        
        # Group by month and week - count all jobs, those with questionnaires, and those with EO question
        weekly_stats = jobs_with_dates.groupby(['year', 'month', 'month_name', 'week_of_month']).agg({
            'usajobs_control_number': 'count',  # Total jobs
            'has_questionnaire': 'sum',  # Jobs with questionnaires
            'has_executive_order': 'sum'  # Jobs with EO question
        }).reset_index()
        weekly_stats.columns = ['year', 'month', 'month_name', 'week_of_month', 'total_jobs', 'has_questionnaire', 'has_executive_order']
        
        # Calculate percentage of jobs with questionnaires that have EO question
        weekly_stats['percentage'] = weekly_stats.apply(
            lambda row: round(row['has_executive_order'] / row['has_questionnaire'] * 100, 1) if row['has_questionnaire'] > 0 else None,
            axis=1
        )
        
        # Pivot to create heatmap format
        heatmap_data = weekly_stats.pivot_table(
            index=['year', 'month', 'month_name'],
            columns='week_of_month',
            values='percentage',
            aggfunc='first'
        ).reset_index()
        
        # Calculate monthly totals
        monthly_totals = weekly_stats.groupby(['year', 'month', 'month_name']).agg({
            'total_jobs': 'sum',
            'has_questionnaire': 'sum',
            'has_executive_order': 'sum'
        }).reset_index()
        # Monthly percentage = EO jobs / questionnaire jobs (to match other analyses)
        monthly_totals['monthly_percentage'] = monthly_totals.apply(
            lambda row: round(row['has_executive_order'] / row['has_questionnaire'] * 100, 1) if row['has_questionnaire'] > 0 else 0,
            axis=1
        )
        
        # Format for display
        timeline_data = []
        for _, row in heatmap_data.iterrows():
            month_name = row['month_name']
            month_totals = monthly_totals[monthly_totals['month_name'] == month_name].iloc[0]
            
            timeline_entry = {
                'Month': month_name,
                'Week 1': row.get(1) if pd.notna(row.get(1)) else None,
                'Week 2': row.get(2) if pd.notna(row.get(2)) else None,
                'Week 3': row.get(3) if pd.notna(row.get(3)) else None,
                'Week 4': row.get(4) if pd.notna(row.get(4)) else None,
                'Week 5': row.get(5) if pd.notna(row.get(5)) else None,
                'Monthly Percentage': month_totals['monthly_percentage'],
                'Total Jobs': int(month_totals['total_jobs']),
                'Jobs with Questionnaires': int(month_totals['has_questionnaire']),
                'Jobs with Essay Question': int(month_totals['has_executive_order'])
            }
            timeline_data.append(timeline_entry)
        
        analysis_data['timeline_analysis'] = timeline_data
    else:
        analysis_data['timeline_analysis'] = []

    return analysis_data


def main():
    # Always regenerate the clean all jobs data to get the latest
    print("Generating clean all jobs data from latest parquet files...")
    import subprocess
    subprocess.run(['python3', 'generate_all_jobs_data.py'], check=True)
    
    # Load the clean all jobs data
    all_jobs_df = pd.read_csv('all_jobs_clean.csv')
    print(f"Total jobs loaded: {len(all_jobs_df):,}")
    
    # Load questionnaire links
    links_df = pd.read_csv(QUESTIONNAIRE_DIR / 'questionnaire_links.csv')
    print(f"\nLoaded {len(links_df):,} questionnaire links")
    
    # Check for the specific executive order question
    eo_mentions = check_executive_order_mentions()
    print(f"\nFound {len(eo_mentions):,} questionnaires with the essay question")
    
    # Get all scraped IDs
    scraped_ids = set()
    for txt_file in RAW_QUESTIONNAIRES_DIR.glob('*.txt'):
        file_id = txt_file.stem.split('_')[1]
        scraped_ids.add(file_id)
    
    # Whether each posting asked the question when first scraped, over all the links
    # first collected for it (some may since have been removed from the posting).
    links_df['questionnaire_id'] = links_df['questionnaire_url'].apply(lambda url: extract_questionnaire_id(url)[1])
    first_scrape_had_q = links_df['questionnaire_id'].isin(eo_mentions).groupby(links_df['usajobs_control_number']).any()

    # Open postings are the ones the latest refresh found listed on USAJobs
    # (open_postings.csv, which the daily email reads), and their questionnaires are
    # the links it found in each posting today (current_status.csv), not the links
    # first collected: agencies add, swap, and remove questionnaire links.
    open_ids = set(pd.read_csv(RECHECK_DIR / 'open_postings.csv', dtype=str)['usajobs_control_number'])
    open_as_of = json.loads((RECHECK_DIR / 'last_run.json').read_text())['date']
    current = pd.read_csv(RECHECK_DIR / 'current_status.csv', dtype=str, keep_default_na=False)
    current = current.assign(usajobs_control_number=current['usajobs_control_numbers'].str.split(';')).explode(
        'usajobs_control_number')[['usajobs_control_number', 'questionnaire_url']]
    current['usajobs_control_number'] = current['usajobs_control_number'].astype('int64')
    current['questionnaire_id'] = current['questionnaire_url'].apply(lambda url: extract_questionnaire_id(url)[1])
    # Links the scraper judged broken aren't re-checked (the refresh drops them), but
    # they still tell viewers a posting had a link we couldn't verify, so they stay.
    from questionnaire_utils import load_known_bad_urls
    is_open_link = links_df['usajobs_control_number'].astype(str).isin(open_ids)
    open_known_bad = links_df[is_open_link & links_df['questionnaire_url'].isin(load_known_bad_urls())
                              & ~links_df['questionnaire_url'].isin(set(current['questionnaire_url']))]
    links_df = pd.concat([links_df[~is_open_link], current, open_known_bad], ignore_index=True)
    links_df['had_executive_order_at_first_scrape'] = links_df['questionnaire_id'].isin(eo_mentions)
    # The flag the site reports is the latest known one: a later re-check, if any,
    # overrides the first scrape.
    rechecked = latest_rechecked_eo_flags()
    links_df['has_executive_order'] = links_df['questionnaire_url'].map(rechecked).fillna(
        links_df['had_executive_order_at_first_scrape']).astype(bool)
    print(f"Re-checked questionnaires: {links_df['questionnaire_url'].isin(rechecked).sum():,} links; "
          f"first scrape had the question but latest re-check doesn't: "
          f"{(links_df['had_executive_order_at_first_scrape'] & ~links_df['has_executive_order']).sum():,} links")

    # Filter to scraped questionnaires (first scrape or a later re-check)
    scraped_df = links_df[links_df['questionnaire_id'].isin(scraped_ids)
                          | links_df['questionnaire_url'].isin(rechecked)].copy()
    
    # Keep track of the original scraped dataframe for later use
    scraped_df_all = scraped_df.copy()
    
    # IMPORTANT: Only count jobs that exist in the current all_jobs dataset
    # This ensures consistency across all aggregations
    scraped_df_in_current = scraped_df[scraped_df['usajobs_control_number'].isin(all_jobs_df['usajobs_control_number'])].copy()
    
    # Update location, grade, and other fields from all_jobs_df to ensure consistency
    # This is important because job details might have changed between when the questionnaire was scraped
    # and the current job data
    job_info_cols = ['position_location', 'grade_code', 'occupation_series', 'occupation_name',
                     'service_type', 'hiring_agency', 'position_open_date']
    all_jobs_info = all_jobs_df[['usajobs_control_number'] + job_info_cols].copy()
    
    # Drop the old columns from scraped_df_in_current and merge with authoritative data
    scraped_df_in_current = scraped_df_in_current.drop(columns=job_info_cols, errors='ignore')
    scraped_df_in_current = pd.merge(scraped_df_in_current, all_jobs_info, on='usajobs_control_number', how='left')
    
    # One row per posting. A posting has the question if any of its questionnaires
    # does (as the daily email counts it), not just the first one listed.
    original_count = len(scraped_df)
    has_q_any = scraped_df_in_current.groupby('usajobs_control_number')['has_executive_order'].any()
    scraped_df_in_current_dedup = scraped_df_in_current.drop_duplicates(subset='usajobs_control_number', keep='first').copy()
    scraped_df_in_current_dedup['has_executive_order'] = scraped_df_in_current_dedup['usajobs_control_number'].map(has_q_any)
    scraped_df_in_current_dedup['had_executive_order_at_first_scrape'] = (
        scraped_df_in_current_dedup['usajobs_control_number'].map(first_scrape_had_q).fillna(False).astype(bool))
    
    print(f"\nTotal questionnaire links scraped: {original_count:,}")
    print(f"Jobs in current dataset with questionnaires: {len(scraped_df_in_current_dedup):,}")
    print(f"Jobs with executive order mentions: {scraped_df_in_current_dedup['has_executive_order'].sum():,}")
    
    # Use the filtered dataset for all analysis
    scraped_df = scraped_df_in_current_dedup
    
    analysis_data = build_analysis(all_jobs_df, scraped_df, scraped_df_in_current, links_df)

    # The same numbers for open postings only.
    missing_open = open_ids - set(all_jobs_df['usajobs_control_number'].astype(str))
    if missing_open:
        raise ValueError(f"{len(missing_open)} open postings from open_postings.csv aren't in all_jobs_clean.csv, "
                         f"e.g. {sorted(missing_open)[:5]}")

    def is_open(df):
        return df['usajobs_control_number'].astype(str).isin(open_ids)

    analysis_data['open_now'] = build_analysis(all_jobs_df[is_open(all_jobs_df)], scraped_df[is_open(scraped_df)],
                                               scraped_df_in_current[is_open(scraped_df_in_current)], links_df)
    analysis_data['open_now']['overview']['as_of'] = open_as_of
    print(f"Open postings as of {open_as_of}: {analysis_data['open_now']['overview']['total_jobs']:,}; "
          f"with the EO question: {analysis_data['open_now']['overview']['total_jobs_with_eo']:,}")

    # Job Postings - Include ALL jobs with questionnaire status
    # Start with all jobs
    all_jobs_for_display = all_jobs_df.copy()
    
    # Create a mapping of jobs with questionnaires.
    # Split links into "valid" (still a live URL candidate) and "known-bad"
    # (URL has been confirmed not to work — bad inference guess or broken
    # Monster page). A job whose ONLY link is known-bad gets a distinct
    # status so we don't mislead users into thinking we found a real URL.
    from questionnaire_utils import load_known_bad_urls
    known_bad = load_known_bad_urls()
    valid_links_df = links_df[~links_df['questionnaire_url'].isin(known_bad)]
    jobs_with_valid_link = set(valid_links_df['usajobs_control_number'])
    jobs_with_any_link = set(links_df['usajobs_control_number'])
    jobs_with_only_bad_link = jobs_with_any_link - jobs_with_valid_link
    jobs_with_scraped = set(scraped_df_all['usajobs_control_number'])
    jobs_with_eo = set(scraped_df[scraped_df['has_executive_order']]['usajobs_control_number'])
    # Asked the question when first scraped, but doesn't now: the questionnaire was
    # edited, or (for an open posting) the link to it was removed or replaced.
    jobs_with_eo_removed = set(first_scrape_had_q[first_scrape_had_q].index) - jobs_with_eo

    # Add questionnaire status to all jobs
    all_jobs_for_display['has_valid_link'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_valid_link)
    all_jobs_for_display['only_bad_link'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_only_bad_link)
    all_jobs_for_display['questionnaire_scraped'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_scraped)
    all_jobs_for_display['eo_question_removed'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_eo_removed)
    all_jobs_for_display['has_eo_question'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_eo)

    # Create questionnaire status column (later assignments override earlier ones)
    all_jobs_for_display['questionnaire_status'] = 'No questionnaire'
    all_jobs_for_display.loc[all_jobs_for_display['only_bad_link'], 'questionnaire_status'] = 'Questionnaire URL could not be verified'
    all_jobs_for_display.loc[all_jobs_for_display['has_valid_link'], 'questionnaire_status'] = 'Has questionnaire (not scraped)'
    all_jobs_for_display.loc[all_jobs_for_display['questionnaire_scraped'], 'questionnaire_status'] = 'Questionnaire without EO question'
    all_jobs_for_display.loc[all_jobs_for_display['eo_question_removed'], 'questionnaire_status'] = 'EO question removed after posting'
    all_jobs_for_display.loc[all_jobs_for_display['has_eo_question'], 'questionnaire_status'] = 'Questionnaire with EO question'

    all_jobs_for_display['is_open'] = is_open(all_jobs_for_display)

    # Get questionnaire URLs for jobs that have them
    questionnaire_urls = links_df.drop_duplicates('usajobs_control_number')[['usajobs_control_number', 'questionnaire_url']]
    all_jobs_for_display = pd.merge(all_jobs_for_display, questionnaire_urls, on='usajobs_control_number', how='left')
    
    # Format dates
    all_jobs_for_display['open_date'] = pd.to_datetime(all_jobs_for_display['position_open_date'], format='mixed', errors='coerce').dt.strftime('%m/%d/%Y')
    all_jobs_for_display['close_date'] = pd.to_datetime(all_jobs_for_display['position_close_date'], format='mixed', errors='coerce').dt.strftime('%m/%d/%Y')
    
    # Create occupation display - pad occupation series to 4 digits
    all_jobs_for_display['occupation'] = all_jobs_for_display['occupation_series'].astype(str).str.zfill(4) + ' - ' + all_jobs_for_display['occupation_name'].fillna('Unknown')
    
    # Prepare job postings data for ALL jobs
    job_postings = []
    for _, job in all_jobs_for_display.iterrows():
        posting = {
            'position_title': job['position_title'][:100] if pd.notna(job['position_title']) else '',
            'occupation': job['occupation'][:50] if pd.notna(job['occupation']) else '',
            'agency': job['hiring_agency'][:50] if pd.notna(job['hiring_agency']) else '',
            'location': job.get('position_location', '')[:50] if pd.notna(job.get('position_location', '')) else '',
            'grade': job.get('grade_code', '') if pd.notna(job.get('grade_code', '')) else '',
            'service': job.get('service_type', '') if pd.notna(job.get('service_type', '')) else '',
            'open_date': job['open_date'] if pd.notna(job['open_date']) else '',
            'close_date': job['close_date'] if pd.notna(job['close_date']) else '',
            'usajobs_link': job['usajobs_control_number'] if pd.notna(job['usajobs_control_number']) else '',
            'questionnaire_status': job['questionnaire_status'],
            'open': bool(job['is_open']),
            'questionnaire_link': transform_monster_url(job['questionnaire_url']) if pd.notna(job.get('questionnaire_url')) else ''
        }
        job_postings.append(posting)
    
    print(f"\nJob postings by status:")
    print(all_jobs_for_display['questionnaire_status'].value_counts())
    
    # Convert numpy types to Python native types for JSON serialization
    def convert_to_native(obj):
        if isinstance(obj, pd.Int64Dtype) or hasattr(obj, 'item'):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_native(i) for i in obj]
        else:
            return obj
    
    # Summary stats and the per-job listing go in separate files. The listing
    # is ~100+ MB; bundling it into analysis_data.json meant the overview
    # numbers stayed blank until the whole thing downloaded and parsed.
    # Compact separators (no indent) roughly halve the listing's size.
    with open(PUBLIC_DIR / 'analysis_data.json', 'w') as f:
        json.dump(analysis_data, f, indent=2, default=str)

    with open(PUBLIC_DIR / 'job_postings.json', 'w') as f:
        json.dump(job_postings, f, separators=(',', ':'), default=str)

    print(f"\nAnalysis data written to {PUBLIC_DIR}/analysis_data.json")
    print(f"Job postings ({len(job_postings):,}) written to {PUBLIC_DIR}/job_postings.json")
    print(f"Total data points: {sum(len(v) if isinstance(v, list) else 1 for v in analysis_data.values())}")

if __name__ == '__main__':
    main()
