"""Shared utilities for questionnaire processing"""
import re
from pathlib import Path


# Common paths
RAW_QUESTIONNAIRES_DIR = Path('./raw_questionnaires')
QUESTIONNAIRE_LINKS_CSV = Path('./questionnaire_links.csv')
KNOWN_BAD_URLS_FILE = Path('./questionnaire_known_bad.txt')


def transform_monster_url(url):
    """Transform Monster dashboard URL to preview format"""
    if 'monstergovt.com' in url and '/ros/rosDashboard.hms' in url:
        # Handle both /ros/rosDashboard and /nga/ros/rosDashboard patterns
        match = re.search(r'https://jobs\.monstergovt\.com/([^/]+)/(?:nga/)?ros/rosDashboard\.hms\?O=(\d+)&J=(\d+)', url)
        if match:
            subdomain = match.group(1)
            org_id = match.group(2)
            job_num = match.group(3)
            return f'https://jobs.monstergovt.com/{subdomain}/vacancy/previewVacancyQuestions.hms?orgId={org_id}&jnum={job_num}'
    elif 'monstergovt.com' in url and '/rospost/' in url:
        match = re.search(r'https://jobs\.monstergovt\.com/([^/]+)/rospost/\?O=(\d+)&J=(\d+)', url)
        if match:
            subdomain = match.group(1)
            org_id = match.group(2)
            job_num = match.group(3)
            return f'https://jobs.monstergovt.com/{subdomain}/vacancy/previewVacancyQuestions.hms?orgId={org_id}&jnum={job_num}'
    return url


def extract_questionnaire_id(url):
    """Extract questionnaire ID and prefix from URL"""
    if 'usastaffing.gov' in url:
        match = re.search(r'ViewQuestionnaire/(\d+)', url)
        file_id = match.group(1) if match else 'unknown'
        return 'usastaffing', file_id
    elif 'monstergovt.com' in url:
        # Try jnum first
        match = re.search(r'jnum=(\d+)', url)
        if not match:
            # Try J= format
            match = re.search(r'J=(\d+)', url)
        file_id = match.group(1) if match else str(hash(url))[:8]
        return 'monster', file_id
    else:
        file_id = str(hash(url))[:8]
        return 'other', file_id


def get_questionnaire_filename(url):
    """Get the filename for a questionnaire based on URL"""
    prefix, file_id = extract_questionnaire_id(url)
    return f'{prefix}_{file_id}.txt'


def get_questionnaire_filepath(url):
    """Get the full file path for a questionnaire"""
    return RAW_QUESTIONNAIRES_DIR / get_questionnaire_filename(url)


def questionnaire_exists(url):
    """Check if a questionnaire has already been scraped"""
    return get_questionnaire_filepath(url).exists()


def infer_questionnaire_url_from_announcement(announcement_number):
    """Guess a USAStaffing ViewQuestionnaire URL from an announcement number.

    ~89% of USAStaffing announcement numbers embed the questionnaire ID as an
    8-digit token (e.g. '26-DC-12855055-AUSA' -> 12855055). Returns the guessed
    URL or None if no plausible token is found.
    """
    if not announcement_number:
        return None
    # Prefer 8-digit tokens (typical QID length); fall back to 7 or 9.
    for width in (8, 7, 9):
        match = re.search(rf'(?<!\d)(\d{{{width}}})(?!\d)', str(announcement_number))
        if match:
            return f'https://apply.usastaffing.gov/ViewQuestionnaire/{match.group(1)}'
    return None


# Matches literal and JSON-Unicode-escaped ViewQuestionnaire URLs in HTML.
_QID_IN_HTML_RE = re.compile(r'ViewQuestionnaire/(\d+)', re.IGNORECASE)


def discover_qid_from_usajobs_html(html):
    """Extract the first USAStaffing questionnaire QID from a USAJobs posting HTML.

    USAJobs postings that list a questionnaire embed the URL as
    `apply.usastaffing.gov/ViewQuestionnaire/<qid>`, sometimes JSON-escaped
    (\\u0022 etc.). Returns the QID string or None.
    """
    if not html:
        return None
    match = _QID_IN_HTML_RE.search(html)
    return match.group(1) if match else None


def questionnaire_text_matches_announcement(text, announcement_number):
    """Return True iff the scraped questionnaire text plausibly belongs to
    the given announcement number.

    USAStaffing questionnaires render the announcement in a header like
    `Announcement Number\n<ann> Opens in new window`. For inferred URLs
    (guessed from announcement or discovered via USAJobs HTML scraping)
    we verify the questionnaire we actually scraped really is the one for
    this posting, by checking that the source announcement string appears
    somewhere in the scraped text.

    Returns True on match, False on clear mismatch, and None if the check
    is not applicable (no announcement given, empty text, or Monster-style
    text with no announcement header).
    """
    if not announcement_number or not text:
        return None
    # Monster preview pages don't include an announcement header; skip the check.
    if 'Announcement Number' not in text:
        return None
    ann = str(announcement_number).strip()
    if not ann:
        return None
    return ann in text


def discover_qid_from_usajobs_posting(position_uri, session=None, timeout=15):
    """Fetch a USAJobs posting page and return a discovered QID, or None.

    Last-resort fallback for USAStaffing jobs whose announcement number
    does not embed the QID. Caller should rate-limit at the loop level.
    """
    if not position_uri:
        return None
    import requests
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; questionnaire-tracker/1.0)'}
    get = session.get if session else requests.get
    try:
        resp = get(position_uri, headers=headers, timeout=timeout, allow_redirects=True)
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    return discover_qid_from_usajobs_html(resp.text)


def load_known_bad_urls(path=None):
    """Load the set of URLs previously confirmed to not exist."""
    path = Path(path) if path else KNOWN_BAD_URLS_FILE
    if not path.exists():
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}


def append_known_bad_url(url, path=None):
    """Append a URL to the known-bad list so we skip it on future runs."""
    path = Path(path) if path else KNOWN_BAD_URLS_FILE
    with open(path, 'a') as f:
        f.write(url + '\n')


def create_git_commit_message(new_count, scraped_count, failed_count, total_links, total_files):
    """Create standardized git commit message for questionnaire updates"""
    if new_count > 0:
        message = f"""Update questionnaires: {new_count:,} new links found, {scraped_count:,} scraped

- Extracted {new_count:,} new questionnaire links
- Scraped {scraped_count:,} questionnaire files  
- Failed to scrape: {failed_count} files
- Total questionnaire links: {total_links:,}
- Total scraped files: {total_files:,}"""
    else:
        message = f"""Update questionnaires: scraped {scraped_count} previously unscraped files

- No new questionnaire links found
- Scraped {scraped_count} previously unscraped questionnaires
- Failed to scrape: {failed_count} files
- Total questionnaire links: {total_links:,}
- Total scraped files: {total_files:,}"""
    
    # Add attribution footer
    message += """

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    
    return message