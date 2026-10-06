#!/usr/bin/env python3
"""Check that the site's tracker agrees with the daily Loyalty Q email for every open posting.

The email reads questionnaire_rechecks/open_postings.csv (each open posting's
latest re-check); the tracker is ../public/job_postings.json. An open posting the
re-check finds the question in must be 'Questionnaire with EO question' on the
site, and one it doesn't find it in must not be.

Exits 1 and prints the mismatches if any disagree; the daily workflow opens an
issue when that happens.

Usage:
    cd src/generate_site
    python check_tracker_matches_email.py
"""
import csv
import json
import sys
from pathlib import Path

OPEN_POSTINGS = Path('questionnaire_rechecks/open_postings.csv')
JOB_POSTINGS = Path('../public/job_postings.json')
WITH_EO = 'Questionnaire with EO question'


def main():
    with open(OPEN_POSTINGS, newline='', encoding='utf-8') as f:
        email = {r['usajobs_control_number']: r['has_loyalty_q'] for r in csv.DictReader(f)
                 if r['questionnaire_found'] == 'True'}
    with open(JOB_POSTINGS, encoding='utf-8') as f:
        site = {str(job['usajobs_link']): job['questionnaire_status'] for job in json.load(f)}

    mismatches = []
    for cn, has_q in sorted(email.items()):
        status = site.get(cn, '(not on site)')
        if (has_q == 'True') != (status == WITH_EO):
            mismatches.append({'usajobs_control_number': cn, 'email_has_loyalty_q': has_q,
                               'tracker_status': status})

    in_email = sum(v == 'True' for v in email.values())
    print(f"Open postings with a questionnaire: {len(email):,}; with the question in the email: {in_email:,}")
    if mismatches:
        print(f"MISMATCH: {len(mismatches):,} open postings disagree:")
        for row in mismatches:
            print(f"  {row}")
        sys.exit(1)
    print("Email and tracker agree on every open posting.")


if __name__ == '__main__':
    main()
