#!/usr/bin/env python3
"""Offline unit tests for the two inference fallbacks.

Covers:
- discover_qid_from_usajobs_html: parses plain and JSON-Unicode-escaped
  ViewQuestionnaire URLs out of USAJobs posting HTML
- extract_questionnaire_links_from_job: fires the announcement-number
  fallback on postings that say "assessment" (not just "questionnaire")

No network, no parquet — runs in under a second.
"""
import json
import sys
import pandas as pd

import os
import tempfile

from questionnaire_utils import (
    discover_qid_from_usajobs_html,
    questionnaire_text_matches_announcement,
)
import questionnaire_utils
from extract_questionnaires import (
    extract_questionnaire_links_from_job,
    is_error_page_and_blacklist,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from job_fields import derive_job_fields, load_questionnaire_links


def _job_row(text, announcement='26-ABC-12345678-XY', apply_url='https://apply.usastaffing.gov/Application/Apply'):
    """Build a minimal job_row wrapped as a pd.Series.

    Rows are built by running a descriptor through derive_job_fields(), exactly
    as the collector does, so these tests cover the real derivation path. The
    raw MatchedObjectDescriptor is no longer stored on rows — it was 98.9% of
    the parquet bytes and OOM-ed the CI runner.
    """
    mod = {
        'JobCategory': [],
        'PositionLocation': [],
        'PositionSchedule': [],
        'UserArea': {
            'Details': {
                'Evaluations': text,
                'ApplyOnlineUrl': apply_url,
            }
        },
        'PositionURI': 'https://www.usajobs.gov/job/999999',
    }
    row = {'announcementNumber': announcement}
    row.update(derive_job_fields(mod, extra_text=str(row)))
    return pd.Series(row)


def _historical_job_row(text):
    """A historical-parquet row: no derived columns, no descriptor.

    These still take the row-grep path in the extractor, so the fallback has to
    keep working for them.
    """
    return pd.Series({
        'announcementNumber': '26-ABC-12345678-XY',
        'positionTitle': 'Analyst',
        'JobCategories': json.dumps([{'series': '0343'}]),
        'someTextField': text,
    })


def test_discover_plain_url():
    html = 'See the <a href="https://apply.usastaffing.gov/ViewQuestionnaire/12345678">questionnaire</a>.'
    assert discover_qid_from_usajobs_html(html) == '12345678'


def test_discover_json_escaped_url():
    # USAJobs sometimes serves the URL JSON-escaped (" for ", etc.)
    html = r'..."https://apply.usastaffing.gov/ViewQuestionnaire/87654321"...'
    assert discover_qid_from_usajobs_html(html) == '87654321'


def test_discover_no_match_returns_none():
    assert discover_qid_from_usajobs_html('no questionnaire here') is None
    assert discover_qid_from_usajobs_html('') is None
    assert discover_qid_from_usajobs_html(None) is None


def test_gate_fires_on_questionnaire_word():
    row = _job_row('Candidates will complete an online questionnaire.')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == ['https://apply.usastaffing.gov/ViewQuestionnaire/12345678']
    assert inferred_ann is True
    assert inferred_html is False


def test_gate_fires_on_assessment_word_only():
    # Exercises the broadened gate — posting says "assessment", not "questionnaire".
    row = _job_row('Candidates will complete an online assessment tool.')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == ['https://apply.usastaffing.gov/ViewQuestionnaire/12345678']
    assert inferred_ann is True
    assert inferred_html is False


def test_gate_skips_non_usastaffing():
    # Posting mentions questionnaire but applies via Monster — gate must not fire.
    row = _job_row(
        'Complete the online questionnaire.',
        apply_url='https://jobs.monstergovt.com/nga/vacancy/previewVacancyQuestions.hms',
    )
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == []
    assert inferred_ann is False
    assert inferred_html is False


def test_gate_skips_when_no_assessment_or_questionnaire_word():
    row = _job_row('Submit your resume and cover letter.')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == []
    assert inferred_ann is False
    assert inferred_html is False


def test_html_fallback_disabled_when_flag_false():
    # Announcement number has no 8/7/9-digit token — only the HTML fallback
    # could produce a URL. With fetch_usajobs_html=False we must return empty.
    row = _job_row('Complete the questionnaire.', announcement='BPA-DH-26-4')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == []
    assert inferred_ann is False
    assert inferred_html is False


def test_ann_match_returns_true_when_present():
    txt = ('Position Title\nDental Assistant\n'
           'Announcement Number\nOCA-FY25-0681-DentalAsst3 Opens in new window\n')
    assert questionnaire_text_matches_announcement(txt, 'OCA-FY25-0681-DentalAsst3') is True


def test_ann_match_returns_false_on_mismatch():
    txt = ('Position Title\nSomething Else\n'
           'Announcement Number\nDIFFERENT-ANN-123 Opens in new window\n')
    assert questionnaire_text_matches_announcement(txt, 'OCA-FY25-0681-DentalAsst3') is False


def test_ann_match_returns_none_for_monster_style_text():
    # No "Announcement Number" header → we can't run the check, so return None.
    txt = 'Seeker - Vacancy - Questions Preview Skip to main content ...'
    assert questionnaire_text_matches_announcement(txt, 'MONSTER-123') is None


def test_ann_match_returns_none_for_empty_inputs():
    assert questionnaire_text_matches_announcement('', 'ANN') is None
    assert questionnaire_text_matches_announcement('text', None) is None
    assert questionnaire_text_matches_announcement('text', '') is None


def _with_temp_known_bad():
    """Point the known_bad path at a tempfile for isolated testing."""
    tf = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    tf.close()
    questionnaire_utils.KNOWN_BAD_URLS_FILE = type(questionnaire_utils.KNOWN_BAD_URLS_FILE)(tf.name)
    return tf.name


def test_error_page_helper_detects_monster_error():
    path = _with_temp_known_bad()
    try:
        txt = "... We're sorry, we encountered an unexpected error ..."
        url = 'https://jobs.monstergovt.com/foo/vacancy/previewVacancyQuestions.hms?orgId=1&jnum=99999'
        assert is_error_page_and_blacklist(txt, url) is True
        # URL should now be in known_bad
        assert url in open(path).read()
    finally:
        os.unlink(path)


def test_error_page_helper_detects_404():
    path = _with_temp_known_bad()
    try:
        txt = 'Page not found'
        url = 'https://apply.usastaffing.gov/ViewQuestionnaire/99999999'
        assert is_error_page_and_blacklist(txt, url) is True
        assert url in open(path).read()
    finally:
        os.unlink(path)


def test_error_page_helper_passes_valid_text():
    path = _with_temp_known_bad()
    try:
        txt = 'Position Title\nDental Assistant\nAnnouncement Number\nOCA-FY25\n...'
        url = 'https://apply.usastaffing.gov/ViewQuestionnaire/12345678'
        assert is_error_page_and_blacklist(txt, url) is False
        assert url not in open(path).read()
    finally:
        os.unlink(path)


def test_error_page_helper_handles_empty():
    assert is_error_page_and_blacklist('', 'http://x') is False
    assert is_error_page_and_blacklist(None, 'http://x') is False


def test_direct_link_read_from_derived_column():
    # A real ViewQuestionnaire URL in the posting text must be found at
    # collection time and read straight back off the derived column — no
    # announcement-number guessing involved.
    row = _job_row('Complete https://apply.usastaffing.gov/ViewQuestionnaire/55554444 to apply.')
    assert load_questionnaire_links(row['questionnaireLinks']) == [
        'https://apply.usastaffing.gov/ViewQuestionnaire/55554444'
    ]
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == ['https://apply.usastaffing.gov/ViewQuestionnaire/55554444']
    assert inferred_ann is False
    assert inferred_html is False


def test_monster_link_sets_flag_on_derived_row():
    url = 'https://jobs.monstergovt.com/nga/vacancy/previewVacancyQuestions.hms?orgId=1&jnum=42'
    row = _job_row(f'Apply here: {url}')
    assert row['hasMonsterLink'] is True
    links, *rest = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == [url]
    assert rest[8] is True  # has_monster_link


def test_historical_row_still_grepped():
    # Historical parquets have no derived columns; the extractor must fall back
    # to grepping the row itself or those jobs silently lose their links.
    row = _historical_job_row('See https://apply.usastaffing.gov/ViewQuestionnaire/77778888 for the questionnaire.')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == ['https://apply.usastaffing.gov/ViewQuestionnaire/77778888']
    assert inferred_ann is False
    assert inferred_html is False


def test_historical_row_inference_gate_still_fires():
    row = _historical_job_row('Apply at apply.usastaffing.gov and complete the questionnaire.')
    links, *_, inferred_ann, inferred_html = extract_questionnaire_links_from_job(row, fetch_usajobs_html=False)
    assert links == ['https://apply.usastaffing.gov/ViewQuestionnaire/12345678']
    assert inferred_ann is True


def test_derived_row_has_no_raw_descriptor():
    # The whole point of the refactor: the blob must not come back.
    row = _job_row('Complete the questionnaire.')
    assert 'MatchedObjectDescriptor' not in row.index


TESTS = [
    test_direct_link_read_from_derived_column,
    test_monster_link_sets_flag_on_derived_row,
    test_historical_row_still_grepped,
    test_historical_row_inference_gate_still_fires,
    test_derived_row_has_no_raw_descriptor,
    test_discover_plain_url,
    test_discover_json_escaped_url,
    test_discover_no_match_returns_none,
    test_gate_fires_on_questionnaire_word,
    test_gate_fires_on_assessment_word_only,
    test_gate_skips_non_usastaffing,
    test_gate_skips_when_no_assessment_or_questionnaire_word,
    test_html_fallback_disabled_when_flag_false,
    test_ann_match_returns_true_when_present,
    test_ann_match_returns_false_on_mismatch,
    test_ann_match_returns_none_for_monster_style_text,
    test_ann_match_returns_none_for_empty_inputs,
    test_error_page_helper_detects_monster_error,
    test_error_page_helper_detects_404,
    test_error_page_helper_passes_valid_text,
    test_error_page_helper_handles_empty,
]


def main():
    failures = []
    for t in TESTS:
        try:
            t()
            print(f'PASS  {t.__name__}')
        except AssertionError as e:
            failures.append((t.__name__, repr(e)))
            print(f'FAIL  {t.__name__}  {e!r}')
        except Exception as e:
            failures.append((t.__name__, repr(e)))
            print(f'ERROR {t.__name__}  {e!r}')
    if failures:
        print(f'\n{len(failures)} / {len(TESTS)} failed')
        sys.exit(1)
    print(f'\nAll {len(TESTS)} tests passed.')


if __name__ == '__main__':
    main()
