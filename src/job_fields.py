#!/usr/bin/env python3
"""Derive the flat job columns that used to be re-parsed out of the raw blob.

Every collected row used to carry the entire USAJobs ``MatchedObjectDescriptor``
JSON — about 26 KB/row, 98.9% of the parquet bytes. Both site scripts re-parsed
it on every run to pull out a dozen values and to regex-grep for questionnaire
links, so ``current_jobs_2026.parquet`` grew past 1 GB and started OOM-ing the CI
runner mid-write (2026-07-28 and 2026-07-29).

Everything the pipeline actually needs is now derived here once, at collection
time, and stored as flat columns; the raw blob is no longer persisted. The full
raw JSON stays available in the sibling ``usajobs_historical`` project, which
mirrors its parquets (blob included) to r2://usajobs-data/data/ — that is where
to re-run extraction from if this logic ever changes and history needs redoing.

Both ``src/generate_data/`` and ``src/generate_site/`` import this module, so it
must stay dependency-light: stdlib only, no pandas, no network.
"""
import json
import re

# UserArea.Details.ServiceType is a code, not a name.
SERVICE_TYPE_MAP = {
    '01': 'Competitive',
    '02': 'Excepted',
    '03': 'Senior Executive',
}

# Columns produced by derive_job_fields(). Kept as an explicit list so the
# collector, the backfill, and the tests all agree on the schema.
DERIVED_COLUMNS = [
    'serviceType',
    'positionLocation',
    'occupationSeries',
    'occupationName',
    'positionSchedule',
    'positionURI',
    'applyOnlineUrl',
    'detailStatusUrl',
    'payScale',
    'minimumGrade',
    'maximumGrade',
    'questionnaireLinks',
    'hasMonsterLink',
    'usesUsastaffing',
    'mentionsQuestionnaire',
]

USASTAFFING_QUESTIONNAIRE_RE = re.compile(
    r'https://apply\.usastaffing\.gov/ViewQuestionnaire/\d+'
)

MONSTER_PATTERNS = [
    re.compile(r'https://jobs\.monstergovt\.com/[^/]+/vacancy/previewVacancyQuestions\.hms\?[^"\'\s<>]+'),
    re.compile(r'https://jobs\.monstergovt\.com/[^/]+/(?:nga/)?ros/rosDashboard\.hms\?[^"\'\s<>]+'),
    re.compile(r'https://jobs\.monstergovt\.com/[^/]+/rospost/\?[^"\'\s<>]+'),
]

USASTAFFING_ANY_RE = re.compile(r'apply\.usastaffing\.gov')
QUESTIONNAIRE_WORD_RE = re.compile(r'questionnaire|assessment', re.IGNORECASE)


def find_questionnaire_links(text):
    """Find questionnaire URLs in `text`. Returns (links, has_monster_link).

    Order matters: USAStaffing links first, then Monster, matching the original
    in-line implementation in extract_questionnaires.py so the CSV written by
    the site pipeline is byte-identical for a given job.
    """
    links = []
    has_monster_link = False

    for match in USASTAFFING_QUESTIONNAIRE_RE.findall(text):
        if match not in links:
            links.append(match)

    for pattern in MONSTER_PATTERNS:
        for match in pattern.findall(text):
            if match not in links:
                links.append(match)
                has_monster_link = True

    return links, has_monster_link


def format_position_location(city, state):
    """Combine city and state the way the site displays them.

    DC is the special case the original code guarded: the city name already
    contains the state ("Washington, District of Columbia"), so appending the
    state again would double it.
    """
    city = city or ''
    state = state or ''
    if city and state:
        if state.lower() in city.lower():
            return city
        return f"{city}, {state}"
    return city or state or None


def derive_job_fields(mod, extra_text=''):
    """Derive every flat column the pipeline needs from a descriptor dict.

    `mod` is the parsed MatchedObjectDescriptor. `extra_text` is any additional
    text that should participate in link/signal detection — at collection time
    that is the rest of the flattened record, matching the site extractor's old
    `str(job_row.to_dict())` behaviour.

    Returns a dict keyed by DERIVED_COLUMNS. Values are parquet-friendly:
    `questionnaireLinks` is a JSON-encoded list (a plain list would make the
    column a nested type that the consumers don't expect), the rest are scalars.
    """
    mod = mod or {}
    fields = {name: None for name in DERIVED_COLUMNS}

    blob_text = json.dumps(mod)
    search_text = f"{extra_text} {blob_text}" if extra_text else blob_text

    details = (mod.get('UserArea') or {}).get('Details') or {}

    service_code = details.get('ServiceType')
    if service_code:
        fields['serviceType'] = SERVICE_TYPE_MAP.get(str(service_code), str(service_code))

    locations = mod.get('PositionLocation')
    if isinstance(locations, list) and locations:
        loc = locations[0] or {}
        fields['positionLocation'] = format_position_location(
            loc.get('CityName'), loc.get('CountrySubDivisionCode')
        )

    categories = mod.get('JobCategory')
    if isinstance(categories, list) and categories:
        cat = categories[0] or {}
        code = cat.get('Code')
        if code:
            # Stored exactly as the API returns it. generate_all_jobs_data.py
            # zero-pads to 4 digits for grouping; extract_questionnaires.py
            # never did. Padding here would silently change the values written
            # to questionnaire_links.csv, so callers keep doing their own.
            fields['occupationSeries'] = code
        fields['occupationName'] = cat.get('Name')

    schedules = mod.get('PositionSchedule')
    if isinstance(schedules, list) and schedules:
        fields['positionSchedule'] = (schedules[0] or {}).get('Name')

    fields['positionURI'] = mod.get('PositionURI')
    fields['applyOnlineUrl'] = details.get('ApplyOnlineUrl')
    fields['detailStatusUrl'] = details.get('DetailStatusUrl')

    grades = mod.get('JobGrade')
    if isinstance(grades, list) and grades:
        fields['payScale'] = (grades[0] or {}).get('Code')
    fields['minimumGrade'] = details.get('LowGrade')
    fields['maximumGrade'] = details.get('HighGrade')

    links, has_monster = find_questionnaire_links(search_text)
    fields['questionnaireLinks'] = json.dumps(links)
    fields['hasMonsterLink'] = has_monster

    # These two drive the announcement-number inference fallback, which fires
    # only when no direct link was found. Precomputed because they were
    # originally evaluated against the whole blob, which no longer exists.
    fields['usesUsastaffing'] = bool(USASTAFFING_ANY_RE.search(search_text))
    fields['mentionsQuestionnaire'] = bool(QUESTIONNAIRE_WORD_RE.search(search_text))

    return fields


def load_questionnaire_links(value):
    """Read the questionnaireLinks column back into a list.

    Tolerates the column being absent (historical parquets), null, an already
    decoded list, or a JSON string.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        if not value.strip():
            return []
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError):
            return []
        return decoded if isinstance(decoded, list) else []
    return []
