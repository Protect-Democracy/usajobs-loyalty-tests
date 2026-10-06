#!/usr/bin/env python3
"""Check that the site's tracker agrees with the daily Loyalty Q email for every open posting.

The email reads questionnaire_rechecks/open_postings.csv (each open posting's
latest re-check); the tracker is ../public/job_postings.json and
../public/analysis_data.json. Checks that:
  - the postings the site marks open are exactly the ones in open_postings.csv;
  - an open posting the re-check finds the question in is 'Questionnaire with EO
    question' on the site, and one it doesn't find it in isn't;
  - the site's open-postings view (analysis_data.json's open_now) counts the same
    open postings with the question as the email, in total and agency by agency.

Exits 1 and prints the mismatches if any disagree; the daily workflow opens an
issue when that happens.

Usage:
    cd src/generate_site
    python check_tracker_matches_email.py
"""
import csv
import json
import sys
from collections import Counter
from pathlib import Path

OPEN_POSTINGS = Path('questionnaire_rechecks/open_postings.csv')
JOB_POSTINGS = Path('../public/job_postings.json')
ANALYSIS_DATA = Path('../public/analysis_data.json')
WITH_EO = 'Questionnaire with EO question'


def main():
    with open(OPEN_POSTINGS, newline='', encoding='utf-8') as f:
        open_rows = list(csv.DictReader(f))
    email_open = {r['usajobs_control_number'] for r in open_rows}
    email = {r['usajobs_control_number']: r['has_loyalty_q'] for r in open_rows if r['questionnaire_found'] == 'True'}
    with open(JOB_POSTINGS, encoding='utf-8') as f:
        jobs = json.load(f)
    site = {str(job['usajobs_link']): job['questionnaire_status'] for job in jobs}
    site_open = {str(job['usajobs_link']) for job in jobs if job['open']}
    with open(ANALYSIS_DATA, encoding='utf-8') as f:
        open_now = json.load(f)['open_now']

    mismatches = []
    for cn in sorted(email_open - site_open):
        mismatches.append({'usajobs_control_number': cn, 'problem': 'open in the email data, not open on the site'})
    for cn in sorted(site_open - email_open):
        mismatches.append({'usajobs_control_number': cn, 'problem': 'open on the site, not in the email data'})
    for cn, has_q in sorted(email.items()):
        status = site.get(cn, '(not on site)')
        if (has_q == 'True') != (status == WITH_EO):
            mismatches.append({'usajobs_control_number': cn, 'email_has_loyalty_q': has_q,
                               'tracker_status': status})

    in_email = sum(v == 'True' for v in email.values())
    site_with_q = open_now['overview']['total_jobs_with_eo']
    if site_with_q != in_email:
        mismatches.append({'problem': f"site's open-postings overview says {site_with_q:,} with the question; "
                                      f"the email data has {in_email:,}"})
    if open_now['overview']['total_jobs'] != len(email_open):
        mismatches.append({'problem': f"site's open-postings overview counts {open_now['overview']['total_jobs']:,} "
                                      f"open postings; the email data has {len(email_open):,}"})
    email_by_agency = Counter(r['hiring_agency'] for r in open_rows if r['has_loyalty_q'] == 'True')
    site_by_agency = {row['Agency']: int(row['Jobs with Essay Question']) for row in open_now['agency_analysis']
                      if row['Jobs with Essay Question']}
    for agency in sorted(set(email_by_agency) | set(site_by_agency)):
        if email_by_agency.get(agency, 0) != site_by_agency.get(agency, 0):
            mismatches.append({'problem': f"{agency}: site's open-postings agency table has "
                                          f"{site_by_agency.get(agency, 0)} with the question; the email data has "
                                          f"{email_by_agency.get(agency, 0)}"})
    print(f"Open postings: {len(email_open):,} (site: {len(site_open):,}); with a questionnaire: {len(email):,}; "
          f"with the question: {in_email:,} (site: {site_with_q:,}); agencies with the question: "
          f"{len(email_by_agency)} (site: {len(site_by_agency)})")
    if mismatches:
        print(f"MISMATCH: {len(mismatches):,} problems:")
        for row in mismatches:
            print(f"  {row}")
        sys.exit(1)
    print("Email and tracker agree on every open posting.")


if __name__ == '__main__':
    main()
