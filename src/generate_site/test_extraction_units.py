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
from pathlib import Path

from questionnaire_utils import (
    discover_qid_from_usajobs_html,
    questionnaire_json_to_text,
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


def test_questionnaire_json_to_text_renders_questions_and_header():
    raw = json.dumps({
        'positionTitle': 'Dental Assistant', 'agencyName': 'Some Agency',
        'announcementNumber': 'OCA-FY25-0681-DentalAsst3', 'openPeriod': 'Sep 1, 2026 to Sep 30, 2026',
        'announcementSections': [{'sectionTitle': 'Essay Questions', 'questions': [
            {'displayIdentifier': '3', 'text': '<p>How would you help advance the President\u2019s policy?</p>',
             'answers': [{'text': 'Yes'}, {'text': None}]},
        ]}],
        'assessmentSections': None,
    })
    txt = questionnaire_json_to_text(raw)
    assert 'Announcement Number\nOCA-FY25-0681-DentalAsst3\n' in txt
    assert '3.\nHow would you help advance the President\u2019s policy?\n Yes\n' in txt
    assert '<p>' not in txt
    # The announcement check used for inferred URLs must work on API-rendered text too.
    assert questionnaire_text_matches_announcement(txt, 'OCA-FY25-0681-DentalAsst3') is True


def test_old_question_matched_through_html_entities():
    # Monster pages can spell the apostrophe as "&rsquo;"; a Department of
    # Transportation posting was missed that way (2026-09-29).
    import refresh_open_questionnaires
    from generate_website_json import check_executive_order_mentions
    variants = [
        "How would you help advance the President's Executive Orders and policy priorities in this role?",
        "How would you help advance the President\u2019s Executive Orders and policy priorities in this role?",
        "6 How would you help advance the President&rsquo;s Executive Orders and policy priorities in this role? Identify",
        "How would you help advance the President&#39;s Executive Orders and policy priorities in this role?",
    ]
    for text in variants:
        assert refresh_open_questionnaires.flags(text)['has_loyalty_q'], text
    with tempfile.TemporaryDirectory() as d:
        for i, text in enumerate(variants):
            with open(os.path.join(d, f'monster_{i}.txt'), 'w', encoding='utf-8') as f:
                f.write(text)
        from pathlib import Path
        assert len(check_executive_order_mentions(Path(d))) == len(variants)


def test_verified_usastaffing_link_counts_only_for_its_own_posting():
    # Blacklisted and inferred links are verified: the questionnaire must belong to a
    # posting linking to it, or to the same announcement (re-posted).
    import refresh_open_questionnaires as r
    raw = json.dumps({'controlNumber': 879910000, 'announcementNumber': 'WA-13032090-AR-26-0001',
                      'positionTitle': 'Production Controller', 'announcementSections': [], 'assessmentSections': []})
    original = r.fetch_usastaffing_questionnaire_json
    try:
        r.fetch_usastaffing_questionnaire_json = lambda url: (raw, False)
        url = 'https://apply.usastaffing.gov/ViewQuestionnaire/13032090'
        assert r.fetch(url, ['879910000'], verify=True)[0] == 'ok'
        assert r.fetch(url, ['111111111'], verify=True)[0] == 'not_this_posting'
        assert r.fetch(url, ['111111111'], verify=True, announcements={'WA-13032090-AR-26-0001'})[0] == 'ok'  # re-posted
        assert r.fetch(url, ['111111111'], verify=True, announcements={'WA-99999999-AR-26-0001'})[0] == 'not_this_posting'
        assert r.fetch(url, ['111111111'], verify=False)[0] == 'ok'  # links in the posting text aren't checked
        r.fetch_usastaffing_questionnaire_json = lambda url: (None, True)
        assert r.fetch(url, ['879910000'], verify=True)[0] == 'invalid'
    finally:
        r.fetch_usastaffing_questionnaire_json = original


def test_possible_variants_flags_rewordings_with_full_question():
    import refresh_open_questionnaires as r
    exact_eo = ("3.\nHow would you help advance the President's Executive Orders and policy priorities in this role? "
                "Identify one or two.\n4.\nNext")
    exact_q5 = ("This position supports DOE's mission and current priorities, including X. Describe how your skills "
                "and experience would help the agency advance those priorities in this role.")
    assert r.possible_variants(exact_eo) == ''
    assert r.possible_variants(exact_q5) == ''
    assert r.possible_variants('Describe your experience coordinating grant programs with state officials.') == ''
    singular = "3.\nHow would you help advance the President's Executive Order and policy priorities in this role? Identify one.\n4.\nNext"
    assert r.possible_variants(singular) == ("Executive Orders question: How would you help advance the President's "
                                             "Executive Order and policy priorities in this role? Identify one.")
    assert r.possible_variants("How will your work advance the Administration&rsquo;s priorities?").startswith('Executive Orders question: ')
    q5 = "This role supports the Department's current priorities, including energy dominance. How would your experience help?"
    assert r.possible_variants(q5) == 'Question 5: ' + q5


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


# ── postings missing from USAJobs search (2026-10-23) ──────────────────────────

def _prev_postings(rows):
    cols = ['usajobs_control_number', 'hiring_agency', 'position_title', 'position_open_date',
            'position_close_date', 'questionnaire_found', 'has_loyalty_q', 'has_new_wording', 'has_question_5',
            'usajobs_link', 'possible_variant', 'questionnaire_link_in_posting', 'listed_today',
            'not_listed_since', 'checked_date']
    base = dict.fromkeys(cols, '')
    return pd.DataFrame([base | r for r in rows], columns=cols)


def test_unlisted_postings_kept_unless_their_page_says_closed():
    import refresh_open_questionnaires as r
    from datetime import date
    today = date(2026, 10, 23)
    prev = _prev_postings([
        {'usajobs_control_number': '1', 'position_close_date': '2026-11-01'},   # listed today
        {'usajobs_control_number': '2', 'position_close_date': '2026-11-01'},   # page: closed early
        {'usajobs_control_number': '3', 'position_close_date': '2026-11-01'},   # page: still open
        {'usajobs_control_number': '4', 'position_close_date': '2026-11-01'},   # page unreadable, day 1
        {'usajobs_control_number': '5', 'position_close_date': '2026-11-01',    # unreadable for 3 days
         'listed_today': 'False', 'not_listed_since': '2026-10-20'},
        {'usajobs_control_number': '6', 'position_close_date': '2026-10-22'},   # close date passed
    ])
    pages = {2: 'closed', 3: 'open', 4: 'unreadable', 5: 'unreadable'}
    looked_up = []

    def page_status(cn):
        looked_up.append(cn)
        return pages[cn]
    kept, outcomes = r.unlisted_postings(today, prev, {1}, page_status=page_status, workers=1)
    assert sorted(looked_up) == [2, 3, 4, 5]                      # not the listed or the expired one
    assert outcomes == {2: 'closed_early', 3: 'kept_page_open', 4: 'kept_page_unreadable',
                        5: 'dropped_page_unreadable'}
    assert kept['usajobs_control_number'].tolist() == ['3', '4']
    assert set(kept['listed_today']) == {'False'} and set(kept['not_listed_since']) == {'2026-10-23'}


def test_usajobs_page_status_reads_the_closed_line():
    import refresh_open_questionnaires as r

    class Resp:
        def __init__(self, code, text):
            self.status_code, self.text = code, text

    class Session:
        def __init__(self, resp):
            self.resp = resp

        def get(self, *a, **k):
            return self.resp
    closed = '<div>This job announcement has closed</div><p>This job is open to</p>'
    assert r.usajobs_page_status(1, Session(Resp(200, closed))) == 'closed'
    assert r.usajobs_page_status(1, Session(Resp(200, '<h2>This job is open to</h2>'))) == 'open'
    assert r.usajobs_page_status(1, Session(Resp(200, '<html>Something went wrong</html>'))) == 'unreadable'
    assert r.usajobs_page_status(1, Session(Resp(503, closed))) == 'unreadable'


def test_kept_postings_keep_their_questionnaires():
    import refresh_open_questionnaires as r
    cols = r.STATUS_COLUMNS
    row = dict.fromkeys(cols, '')
    today = pd.DataFrame([row | {'questionnaire_url': 'u1', 'usajobs_control_numbers': '10', 'fetch_status': 'ok'}],
                         columns=cols)
    prev = pd.DataFrame([
        row | {'questionnaire_url': 'u1', 'usajobs_control_numbers': '10;20', 'fetch_status': 'ok'},
        row | {'questionnaire_url': 'u2', 'usajobs_control_numbers': '20', 'fetch_status': 'skipped_blocked_domain',
               'has_loyalty_q': 'True', 'checked_date': '2026-10-22'},
        row | {'questionnaire_url': 'u3', 'usajobs_control_numbers': '30', 'fetch_status': 'ok'},
    ], columns=cols)
    out = r.carry_status_rows(today, prev, {20})
    by = out.set_index('questionnaire_url')
    assert by.loc['u1', 'usajobs_control_numbers'] == '10;20'           # shared with a listed posting
    assert by.loc['u2', 'fetch_status'] == 'posting_not_listed'          # kept as it was
    assert by.loc['u2', 'has_loyalty_q'] == 'True' and by.loc['u2', 'checked_date'] == '2026-10-22'
    assert 'u3' not in by.index                                          # its posting wasn't kept


def test_link_log_records_the_drop_and_what_happened():
    import refresh_open_questionnaires as r
    from datetime import date
    jobs = pd.DataFrame({'usajobs_control_number': [1], 'position_close_date': pd.to_datetime(['2026-11-01'])})
    empty = pd.DataFrame(columns=r.STATUS_COLUMNS)
    day1 = _prev_postings([{'usajobs_control_number': n, 'position_close_date': '2026-11-01'} for n in ('1', '2')])
    rows = r.link_changes(date(2026, 10, 23), empty, day1, empty, jobs, {2: 'kept_page_open'})
    assert [(x['usajobs_control_number'], x['change']) for x in rows] == [(2, 'no_longer_listed'), (2, 'kept_page_open')]
    # next day it's still kept: nothing new to log
    day2 = _prev_postings([{'usajobs_control_number': '1', 'position_close_date': '2026-11-01'},
                           {'usajobs_control_number': '2', 'position_close_date': '2026-11-01', 'listed_today': 'False'}])
    assert r.link_changes(date(2026, 10, 24), empty, day2, empty, jobs, {2: 'kept_page_open'}) == []
    # the day its page says closed
    rows = r.link_changes(date(2026, 10, 25), empty, day2, empty, jobs, {2: 'closed_early'})
    assert [(x['usajobs_control_number'], x['change']) for x in rows] == [(2, 'closed_early')]


def test_open_postings_mark_unlisted_and_oldest_check():
    import refresh_open_questionnaires as r
    cols = r.STATUS_COLUMNS
    row = dict.fromkeys(cols, '')
    status = pd.DataFrame([
        row | {'questionnaire_url': 'u1', 'usajobs_control_numbers': '1', 'has_loyalty_q': 'False',
               'has_new_wording': 'False', 'has_question_5': 'False', 'checked_date': '2026-10-23'},
        row | {'questionnaire_url': 'u2', 'usajobs_control_numbers': '1', 'has_loyalty_q': 'True',
               'has_new_wording': 'False', 'has_question_5': 'False', 'checked_date': '2026-10-21'},
        row | {'questionnaire_url': 'u3', 'usajobs_control_numbers': '2', 'has_loyalty_q': 'True',
               'has_new_wording': 'False', 'has_question_5': 'False', 'checked_date': '2026-10-22'},
    ], columns=cols)
    jobs = pd.DataFrame({'usajobs_control_number': [1, 2], 'hiring_agency': ['A', 'DOT'], 'position_title': ['x', 'y'],
                         'position_open_date': pd.to_datetime(['2026-10-01'] * 2),
                         'position_close_date': pd.to_datetime(['2026-11-01'] * 2)})
    kept = pd.DataFrame({'usajobs_control_number': ['2'], 'not_listed_since': ['2026-10-23']})
    with tempfile.TemporaryDirectory() as d:
        out = r.write_open_postings(jobs, status, os.path.join(d, 'o.csv'), set(), kept).set_index(
            'usajobs_control_number')
    assert out.loc[1, 'checked_date'] == '2026-10-21'                     # the oldest of its questionnaires
    assert out.loc[1, 'listed_today'] == 'True' and out.loc[1, 'not_listed_since'] == ''
    assert out.loc[2, 'listed_today'] == 'False' and out.loc[2, 'not_listed_since'] == '2026-10-23'
    assert out.loc[2, 'has_loyalty_q'] == 'True'


def test_run_warnings_for_a_day_like_2026_10_23():
    import refresh_open_questionnaires as r
    from datetime import date
    cols = r.STATUS_COLUMNS
    row = dict.fromkeys(cols, '')
    status = pd.DataFrame([row | {'fetch_status': 'skipped_blocked_domain', 'checked_date': '2026-10-22'}] * 22
                          + [row | {'fetch_status': 'ok', 'checked_date': '2026-10-24'}], columns=cols)
    summary = {'open_postings_not_listed_today': 124, 'open_postings_not_listed_today_with_loyalty_q': 22,
               'open_postings_with_loyalty_q_checked_before_yesterday': 22, 'failed': 331}
    w = r.run_warnings(summary, status, date(2026, 10, 24))
    assert len(w) == 4
    assert w[0].startswith("124 open postings (22 with the Loyalty Q) weren't in today's USAJobs listing")
    assert 'last checked on 2026-10-22: no re-check has succeeded since' in w[1]
    assert w[2] == '22 open postings with the Loyalty Q rest on one of those older answers.'
    assert w[3].startswith('331 questionnaires')
    quiet = {'open_postings_not_listed_today': 3, 'open_postings_not_listed_today_with_loyalty_q': 0,
             'open_postings_with_loyalty_q_checked_before_yesterday': 0, 'failed': 0}
    fresh = status.assign(checked_date='2026-10-23')
    assert r.run_warnings(quiet, fresh, date(2026, 10, 24)) == []        # yesterday's local check is fresh
    # 2026-10-27: the re-check ran the evening before, but one FAA posting reopened that morning
    tue = status.assign(checked_date='2026-10-26')
    tue.loc[0, 'checked_date'] = ''
    tue.loc[0, 'text_file'] = '../raw_questionnaires/x.txt'
    w = r.run_warnings(quiet | {'open_postings_with_loyalty_q_checked_before_yesterday': 1}, tue, date(2026, 10, 27))
    assert len(w) == 2 and "haven't been re-checked since their postings joined the list" in w[0], w
    assert w[1] == '1 open postings with the Loyalty Q rest on one of those older answers.'


def test_kept_postings_faa_questionnaires_stay_on_the_local_recheck_list():
    import refresh_open_questionnaires as r
    cols = r.STATUS_COLUMNS
    row = dict.fromkeys(cols, '')
    prev = pd.DataFrame([
        row | {'questionnaire_url': 'https://jobs.faa.gov/Preview/1', 'usajobs_control_numbers': '20'},
        row | {'questionnaire_url': 'https://jobs.faa.gov/Preview/2', 'usajobs_control_numbers': '20;30'},
        row | {'questionnaire_url': 'https://apply.usastaffing.gov/ViewQuestionnaire/9', 'usajobs_control_numbers': '20'},
        row | {'questionnaire_url': 'https://jobs.faa.gov/Preview/3', 'usajobs_control_numbers': '40'},
        row | {'questionnaire_url': 'https://jobs.faa.gov/Preview/4', 'usajobs_control_numbers': '20'},
    ], columns=cols)
    out = r.blocked_portal_urls_of(prev, {20, 30}, known_urls={'https://jobs.faa.gov/Preview/4'})
    assert out == {'https://jobs.faa.gov/Preview/1': '20', 'https://jobs.faa.gov/Preview/2': '20;30'}
    assert r.is_blocked_portal('https://jobs.faa.gov/Preview/1')
    assert not r.is_blocked_portal('https://apply.usastaffing.gov/ViewQuestionnaire/9')
    assert not r.is_blocked_portal('https://jobs.monstergovt.com/x/vacancy/previewVacancyQuestions.hms?jnum=1')
    # fetch() puts them in the blocked-portal state the local re-check reads
    assert r.fetch('https://jobs.faa.gov/Preview/1')[0] == 'skipped_blocked_domain'


def test_reopened_postings_read_from_the_usajobs_page():
    import refresh_open_questionnaires as r
    from datetime import date
    d = pd.to_datetime
    jobs = pd.DataFrame({
        'usajobs_control_number': [1, 2, 3, 4, 5, 6],
        'position_open_date': d(['2026-09-04', '2026-05-18', '2026-10-01', '2026-07-15', '2026-07-02', '2026-07-01']),
        'first_close_date': d(['2026-09-24', '2026-06-05', '2026-11-01', '2026-08-04', '2026-07-17', '2026-07-20']),
        'position_close_date': d(['2026-11-16', '2026-11-05', '2026-11-01', '2026-11-05', '2026-11-20', '2026-11-30']),
    })
    prev = _prev_postings([
        # 4 was read and found reopened on an earlier run; its close date hasn't changed
        {'usajobs_control_number': '4', 'position_close_date': '2026-11-05'},
        # 5 was read before, but its close date changed since
        {'usajobs_control_number': '5', 'position_close_date': '2026-11-09'},
        # 6 was read before and the page couldn't be read
        {'usajobs_control_number': '6', 'position_close_date': '2026-11-30'},
    ])
    prev['page_open_date'] = ['2026-10-23', '2026-10-21', '']
    prev['reopened_date'] = ['2026-10-23', '2026-10-21', '']
    prev['original_close_date'] = ['2026-08-04', '2026-07-17', '']
    pages = {1: '2026-10-27', 2: '2026-05-18', 5: '2026-10-21', 6: ''}
    looked_up = []

    def page_open_date(cn):
        looked_up.append(cn)
        return pages[cn]
    out = r.reopened_postings(date(2026, 10, 27), jobs, prev, page_open_date=page_open_date, workers=1)
    assert sorted(looked_up) == [1, 2, 5, 6]          # not 3 (first close date ahead) or 4 (carried)
    assert out.loc[1, 'reopened_date'] == '2026-10-27' and out.loc[1, 'original_close_date'] == '2026-09-24'
    assert out.loc[2, 'reopened_date'] == '' and out.loc[2, 'page_open_date'] == '2026-05-18'   # extended
    assert (out.loc[3] == '').all()
    assert out.loc[4, 'reopened_date'] == '2026-10-23' and out.loc[4, 'original_close_date'] == '2026-08-04'
    assert out.loc[5, 'reopened_date'] == '2026-10-21'
    assert (out.loc[6] == '').all()                    # unreadable: tried again next run

    # Not reopened: a new cycle before the first close date, and renewals on or the day after it
    cycles = pd.DataFrame({
        'usajobs_control_number': [7, 8, 9, 10],
        'position_open_date': d(['2025-10-01', '2026-06-05', '2026-09-11', '2026-10-01']),
        'first_close_date': d(['2026-09-30', '2026-09-04', '2026-09-30', '2026-10-13']),
        'position_close_date': d(['2027-01-31', '2026-10-30', '2027-09-30', '2026-11-01']),
    })
    pages = {7: '2026-09-01', 8: '2026-09-04', 9: '2026-10-01', 10: '2026-10-15'}
    out = r.reopened_postings(date(2026, 10, 27), cycles, prev.iloc[0:0], page_open_date=pages.get, workers=1)
    assert out['reopened_date'].tolist() == ['', '', '', '2026-10-15']   # 10: closed all of 10/14


def test_usajobs_page_open_date_reads_the_date_display():
    import refresh_open_questionnaires as r

    class Resp:
        status_code = 200
        text = ('<script>x = { "DateDisplay": "Open 10/27/2026 to 11/16/2026", "ClockDisplay": "" }</script>'
                '<p>males born after 12/31/1959</p>')

    class Session:
        def get(self, *a, **k):
            return Resp()
    assert r.usajobs_page_open_date(1, Session()) == '2026-10-27'
    Resp.text = '<html>Something went wrong 01/02/2026 to 01/03/2026</html>'
    assert r.usajobs_page_open_date(1, Session()) == ''


def test_open_postings_carry_reopened_dates():
    import refresh_open_questionnaires as r
    cols = r.STATUS_COLUMNS
    row = dict.fromkeys(cols, '')
    status = pd.DataFrame([row | {'questionnaire_url': 'u', 'usajobs_control_numbers': '1;2', 'has_loyalty_q': 'True',
                                  'has_new_wording': 'False', 'has_question_5': 'False'}], columns=cols)
    jobs = pd.DataFrame({'usajobs_control_number': [1, 2], 'hiring_agency': ['DOT', 'HHS'], 'position_title': ['x', 'y'],
                         'position_open_date': pd.to_datetime(['2026-09-04'] * 2),
                         'position_close_date': pd.to_datetime(['2026-11-16'] * 2)})
    reopened = pd.DataFrame({'page_open_date': ['2026-10-27'], 'reopened_date': ['2026-10-27'],
                             'original_close_date': ['2026-09-24']}, index=[1])
    kept = pd.DataFrame({'usajobs_control_number': ['2'], 'not_listed_since': ['2026-10-27'],
                         'page_open_date': ['2026-10-23'], 'reopened_date': ['2026-10-23'],
                         'original_close_date': ['2026-08-04']})
    with tempfile.TemporaryDirectory() as d:
        out = r.write_open_postings(jobs, status, os.path.join(d, 'o.csv'), set(), kept, reopened).set_index(
            'usajobs_control_number')
    assert out.loc[1, 'reopened_date'] == '2026-10-27' and out.loc[1, 'original_close_date'] == '2026-09-24'
    assert out.loc[2, 'reopened_date'] == '2026-10-23'                    # kept unlisted: carried over


def test_check_blocked_portals_reports_and_never_raises():
    import refresh_open_questionnaires as r
    seen = []

    def ok(urls, out_dir):
        seen.append(list(urls))
        return [{'status': 'ok'}, {'status': 'blocked_or_empty_page'}]
    assert r.check_blocked_portals(['a', 'b'], 'd', recheck=ok) == {'questionnaires': 2, 'ok': 1, 'failed': 1, 'error': ''}
    assert seen == [['a', 'b']]

    def boom(urls, out_dir):
        raise RuntimeError('Chrome did not start')
    out = r.check_blocked_portals(['a', 'b'], 'd', recheck=boom)
    assert out['failed'] == 2 and out['error'] == 'RuntimeError: Chrome did not start'
    # and it shows up as a warning
    from datetime import date
    status = pd.DataFrame(columns=r.STATUS_COLUMNS)
    w = r.run_warnings({'open_postings_not_listed_today': 0, 'open_postings_not_listed_today_with_loyalty_q': 0,
                        'open_postings_with_loyalty_q_checked_before_yesterday': 0, 'failed': 0,
                        'blocked_portal_check': out}, status, date(2026, 10, 28))
    assert w == ["Today's re-check of questionnaires on portals that block automated checks failed for 2 of 2 "
                 "(RuntimeError: Chrome did not start); they keep their last check."], w

def test_blacklisted_faa_links_are_kept_and_unread_ones_warned():
    """2026-10-27: the scraper had blacklisted 136 open FAA postings' questionnaires because
    jobs.faa.gov's bot check turned it away, and the refresh dropped them."""
    import refresh_open_questionnaires as r
    from datetime import date
    faa, usas, monster = ('https://jobs.faa.gov/Preview/100931', 'https://apply.usastaffing.gov/ViewQuestionnaire/1',
                          'https://jobs.monstergovt.com/x/vacancy/previewVacancyQuestions.hms?jnum=1')
    listing = pd.DataFrame({'usajobs_control_number': ['1', '2', '3'], 'questionnaire_links': [json.dumps([u]) for u in (faa, usas, monster)],
                            'mentions_questionnaire': 'True', 'uses_usastaffing': 'False', 'announcement_number': 'A',
                            'position_uri': '', 'position_close_date': '2026-11-12', 'listed_date': '2026-10-27'})
    jobs = pd.DataFrame({'usajobs_control_number': [1, 2, 3], 'hiring_agency': 'X', 'position_title': 'T',
                         'position_open_date': '2026-10-20', 'position_close_date': '2026-11-12'})
    with tempfile.TemporaryDirectory() as d:
        listing.to_csv(Path(d) / 'listing.csv', index=False)
        jobs.to_csv(Path(d) / 'all_jobs_clean.csv', index=False)
        old = (r.CURRENT_LISTING, r.load_known_bad_urls, os.getcwd())
        r.CURRENT_LISTING, r.load_known_bad_urls = Path(d) / 'listing.csv', lambda: {faa, usas, monster}
        os.chdir(d)
        try:
            _, links, _, _ = r.open_postings_and_links(pd.Timestamp('2026-10-27'))
        finally:
            r.CURRENT_LISTING, r.load_known_bad_urls = old[:2]
            os.chdir(old[2])
    assert sorted(links['questionnaire_url']) == [usas, faa]     # the Monster one is still dropped
    # An FAA questionnaire the Chrome check couldn't read and that was never saved
    cols = r.STATUS_COLUMNS
    status = pd.DataFrame([dict.fromkeys(cols, '') | {'fetch_status': 'skipped_blocked_domain'}], columns=cols)
    quiet = {'open_postings_not_listed_today': 0, 'open_postings_not_listed_today_with_loyalty_q': 0,
             'open_postings_with_loyalty_q_checked_before_yesterday': 0, 'failed': 0}
    assert r.run_warnings(quiet, status, date(2026, 10, 28)) == [
        '1 questionnaires on portals that block automated checks (e.g. jobs.faa.gov) have never been read, so '
        'their postings are counted as not having the Loyalty Q or Question 5 whether they do or not.']


def test_announcement_guess_only_for_postings_that_apply_through_usastaffing():
    """2026-10-27: a Navy police officer "public notice flyer" (apply by email, no Apply
    button) matched a USAStaffing questionnaire with the Loyalty Q through its
    announcement number, though its applicants may never see it."""
    import refresh_open_questionnaires as r
    listing = pd.DataFrame({'usajobs_control_number': ['882412600', '883170300'], 'questionnaire_links': '[]',
                            'mentions_questionnaire': 'False', 'uses_usastaffing': ['False', 'True'],
                            'announcement_number': ['DE-13045838-26-LE', 'ST-13053378-26-LJ'],
                            'position_uri': '', 'position_close_date': '2027-03-01', 'listed_date': '2026-10-27'})
    jobs = pd.DataFrame({'usajobs_control_number': [882412600, 883170300], 'hiring_agency': 'X', 'position_title': 'T',
                         'position_open_date': '2026-10-20', 'position_close_date': '2027-03-01'})
    with tempfile.TemporaryDirectory() as d:
        listing.to_csv(Path(d) / 'listing.csv', index=False)
        jobs.to_csv(Path(d) / 'all_jobs_clean.csv', index=False)
        old = (r.CURRENT_LISTING, r.apply_online_urls, os.getcwd())
        r.CURRENT_LISTING = Path(d) / 'listing.csv'
        r.apply_online_urls = lambda cns: {882412600: '', 883170300: 'https://apply.usastaffing.gov/Application/Apply'}
        os.chdir(d)
        try:
            _, links, _, _ = r.open_postings_and_links(pd.Timestamp('2026-10-27'))
        finally:
            r.CURRENT_LISTING, r.apply_online_urls = old[:2]
            os.chdir(old[2])
    assert list(links['questionnaire_url']) == ['https://apply.usastaffing.gov/ViewQuestionnaire/13053378']
    assert r.applies_through_usastaffing('https://apply.usastaffing.gov/Application/Apply')
    assert not r.applies_through_usastaffing('') and not r.applies_through_usastaffing('https://jobs.faa.gov/x')


def test_usastaffing_id_scan_walks_to_the_end_and_stops_when_blocked():
    import scan_usastaffing_ids as sc
    table = pd.DataFrame({'qid': [1000, 1001], 'control_number': ['1', '2'], 'close_date': '2026-11-01'})
    old = (sc.lookup, sc.RESCAN, sc.STOP_AFTER, sc.BATCH)
    sc.RESCAN, sc.STOP_AFTER, sc.BATCH = 5, 20, 7
    try:
        # IDs up to 1010 exist, except 1005 (taken but not published yet)
        sc.lookup = lambda s, q: ((q, 200, str(q), '2026-12-01') if q <= 1010 and q != 1005 else (q, 'missing', None, None))
        rows, _, blocked = sc.scan(table, 2)
        assert not blocked and sorted(r['qid'] for r in rows) == [q for q in range(996, 1011) if q not in (1000, 1001, 1005)]
        sc.lookup = lambda s, q: (q, 403, None, None)
        rows, summary, blocked = sc.scan(table, 2)
        assert blocked and rows == [] and '403' in summary
    finally:
        sc.lookup, sc.RESCAN, sc.STOP_AFTER, sc.BATCH = old


def test_refresh_adds_questionnaires_whose_control_number_is_the_postings():
    """ICE Health Service Corps postings don't link their questionnaire and have no number
    to guess from; usastaffing_ids.csv says which questionnaires are theirs."""
    import refresh_open_questionnaires as r
    listing = pd.DataFrame({'usajobs_control_number': ['1', '2', '3'],
                            'questionnaire_links': ['[]', '[]', json.dumps(['https://apply.usastaffing.gov/ViewQuestionnaire/7'])],
                            'mentions_questionnaire': 'False', 'uses_usastaffing': 'False', 'announcement_number': 'HSC-26',
                            'position_uri': '', 'position_close_date': '2026-11-12', 'listed_date': '2026-10-27'})
    jobs = pd.DataFrame({'usajobs_control_number': [1, 2, 3], 'hiring_agency': 'X', 'position_title': 'T',
                         'position_open_date': '2026-10-20', 'position_close_date': '2026-11-12'})
    with tempfile.TemporaryDirectory() as d:
        listing.to_csv(Path(d) / 'listing.csv', index=False)
        jobs.to_csv(Path(d) / 'all_jobs_clean.csv', index=False)
        pd.DataFrame({'qid': [11, 22, 33], 'control_number': ['1', '2', '3'], 'close_date': '2026-11-12'}).to_csv(
            Path(d) / 'ids.csv', index=False)
        old = (r.CURRENT_LISTING, r.apply_online_urls, r.USASTAFFING_IDS, os.getcwd())
        r.CURRENT_LISTING, r.USASTAFFING_IDS = Path(d) / 'listing.csv', Path(d) / 'ids.csv'
        # 2 has no Apply button, so its applicants may never see questionnaire 22
        r.apply_online_urls = lambda cns: {1: 'https://apply.usastaffing.gov/Application/Apply', 2: '',
                                           3: 'https://apply.usastaffing.gov/Application/Apply'}
        os.chdir(d)
        try:
            _, links, _, _ = r.open_postings_and_links(pd.Timestamp('2026-10-27'))
        finally:
            r.CURRENT_LISTING, r.apply_online_urls, r.USASTAFFING_IDS = old[:3]
            os.chdir(old[3])
    got = sorted(zip(links['usajobs_control_number'], links['questionnaire_url'].str.rsplit('/', n=1).str[1], links['inferred']))
    # 3 keeps its own link as direct, and gets questionnaire 33 too (on 2026-10-27, 890164100 linked another posting's)
    assert got == [(1, '11', True), (3, '33', True), (3, '7', False)]


TESTS = [
    test_usastaffing_id_scan_walks_to_the_end_and_stops_when_blocked,
    test_refresh_adds_questionnaires_whose_control_number_is_the_postings,
    test_announcement_guess_only_for_postings_that_apply_through_usastaffing,
    test_blacklisted_faa_links_are_kept_and_unread_ones_warned,
    test_check_blocked_portals_reports_and_never_raises,
    test_reopened_postings_read_from_the_usajobs_page,
    test_usajobs_page_open_date_reads_the_date_display,
    test_open_postings_carry_reopened_dates,
    test_kept_postings_faa_questionnaires_stay_on_the_local_recheck_list,
    test_unlisted_postings_kept_unless_their_page_says_closed,
    test_usajobs_page_status_reads_the_closed_line,
    test_kept_postings_keep_their_questionnaires,
    test_link_log_records_the_drop_and_what_happened,
    test_open_postings_mark_unlisted_and_oldest_check,
    test_run_warnings_for_a_day_like_2026_10_23,
    test_possible_variants_flags_rewordings_with_full_question,
    test_verified_usastaffing_link_counts_only_for_its_own_posting,
    test_old_question_matched_through_html_entities,
    test_questionnaire_json_to_text_renders_questions_and_header,
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
