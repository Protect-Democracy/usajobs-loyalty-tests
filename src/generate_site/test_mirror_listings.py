#!/usr/bin/env python3
"""Offline unit tests for closed postings' dates and shared questionnaires.

Covers:
- mirror_listings.apply_latest_listing: closed postings take their latest
  listing's close date from the usajobs_historical mirror; re-opened ones get a
  reopened date; open postings and postings the mirror lacks are left alone
- mirror_listings.load_latest_listings: a mirror file that cannot be fetched
  fails the build rather than falling back to stale dates
- extract_all_links_to_csv: a questionnaire linked from several postings gets a
  row for each posting whose own text links it, and not for an inferred guess

Fixtures are real postings, checked against the live Historical API on
2026-11-01:
  842340200  closed; site showed 8/6/2025, USAJOBS says 8/18/2025
  861246000  closed, re-opened: first listed 3/13-3/27/2026, re-listed
             5/6-5/20/2026
  841163600 / 841164000  one Air Force announcement split in two; questionnaire
             12765890 names 841164000 but was credited to 841163600 only

No network — runs in under a second.
"""
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

import extract_questionnaires
from mirror_listings import apply_latest_listing, load_latest_listings, mirror_years


def _jobs():
    return pd.DataFrame({
        "usajobs_control_number": [842340200, 861246000, 900000001, 900000002],
        "position_open_date": ["2025-08-01", "2026-03-13", "2026-10-20", "2025-09-01"],
        "position_close_date": ["2025-08-06", "2026-03-27", "2026-11-20", "2025-09-15"],
    })


def _latest():
    return pd.DataFrame({
        "usajobs_control_number": ["842340200", "861246000", "900000001"],
        "latest_open": ["2025-08-01", "2026-05-06", "2026-10-20"],
        "latest_close": ["2025-08-18", "2026-05-20", "2026-10-25"],
    })


def test_closed_posting_takes_the_latest_close_date():
    jobs, counts = apply_latest_listing(_jobs(), _latest(),
                                        pd.Series([False, False, True, False]))
    row = jobs.set_index("usajobs_control_number").loc[842340200]
    assert row["position_close_date"] == "2025-08-18", row["position_close_date"]
    assert row["reopened_open_date"] is None
    assert counts["close_date_changed"] == 2


def test_reopened_closed_posting_keeps_its_first_open_date():
    jobs, counts = apply_latest_listing(_jobs(), _latest(),
                                        pd.Series([False, False, True, False]))
    row = jobs.set_index("usajobs_control_number").loc[861246000]
    assert row["position_open_date"] == "2026-03-13"
    assert row["position_close_date"] == "2026-05-20"
    assert row["reopened_open_date"] == "2026-05-06"
    assert counts["reopened"] == 1


def test_open_postings_keep_todays_dates():
    # An open posting's close date comes from today's listing; the mirror,
    # re-polled less often, must not overwrite it.
    jobs, _ = apply_latest_listing(_jobs(), _latest(),
                                   pd.Series([False, False, True, False]))
    assert jobs.set_index("usajobs_control_number").loc[900000001, "position_close_date"] == "2026-11-20"


def test_postings_the_mirror_lacks_are_counted_not_guessed():
    jobs, counts = apply_latest_listing(_jobs(), _latest(),
                                        pd.Series([False, False, True, False]))
    assert jobs.set_index("usajobs_control_number").loc[900000002, "position_close_date"] == "2025-09-15"
    assert counts["closed_not_in_mirror"] == 1


class _Resp:
    def __init__(self, status, content=b""):
        self.status_code, self.content = status, content


class _Session:
    def __init__(self, files):
        self.files = files

    def get(self, url, timeout=None):
        name = url.rsplit("/", 1)[1]
        return _Resp(200, self.files[name]) if name in self.files else _Resp(404)


def _parquet_bytes(rows):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "x.parquet")
        pd.DataFrame(rows).to_parquet(path, index=False)
        return Path(path).read_bytes()


def test_a_missing_mirror_year_fails_the_build():
    session = _Session({"historical_jobs_2025.parquet": _parquet_bytes(
        [{"usajobsControlNumber": 1, "positionOpenDate": "2025-07-01",
          "positionCloseDate": "2025-07-15"}])})
    try:
        load_latest_listings([2025, 2026], base_url="https://example", session=session)
    except RuntimeError as e:
        assert "2026" in str(e)
    else:
        raise AssertionError("expected a RuntimeError for the missing 2026 file")


def test_latest_listings_read_across_years():
    session = _Session({
        "historical_jobs_2025.parquet": _parquet_bytes(
            [{"usajobsControlNumber": 861246000, "positionOpenDate": "2025-12-01T00:00:00",
              "positionCloseDate": "2025-12-15T00:00:00"}]),
        "historical_jobs_2026.parquet": _parquet_bytes(
            [{"usajobsControlNumber": 861246000, "positionOpenDate": "2026-05-06T00:00:00",
              "positionCloseDate": "2026-05-20T00:00:00"}])})
    got = load_latest_listings([2025, 2026], base_url="https://example", session=session)
    assert got.to_dict("records") == [{"usajobs_control_number": "861246000",
                                       "latest_open": "2026-05-06",
                                       "latest_close": "2026-05-20"}]


def test_mirror_years_reach_this_year():
    years = mirror_years("2025-06-01")
    assert years[0] == 2025 and years[-1] >= 2026


def _extract(rows, links_by_cn, existing=None):
    """Run extract_all_links_to_csv over a one-file data dir, with the per-job
    link finder stubbed: links_by_cn maps control number -> (links, inferred)."""
    def fake(row, fetch_usajobs_html=True):
        links, inferred = links_by_cn.get(row["usajobsControlNumber"], ([], False))
        return (links, "2210", "IT", "X", None, None, None, None, None, False,
                inferred, False)
    real = extract_questionnaires.extract_questionnaire_links_from_job
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "data"
        data.mkdir()
        pd.DataFrame(rows).to_parquet(data / "current_jobs_2026.parquet", index=False)
        os.chdir(tmp)
        try:
            if existing is not None:
                pd.DataFrame(existing).to_csv("questionnaire_links.csv", index=False)
            extract_questionnaires.extract_questionnaire_links_from_job = fake
            extract_questionnaires.extract_all_links_to_csv(str(data), "2025-06-01")
            return pd.read_csv("questionnaire_links.csv")
        finally:
            extract_questionnaires.extract_questionnaire_links_from_job = real
            os.chdir(cwd)


_Q = "https://apply.usastaffing.gov/ViewQuestionnaire/12765890"
_ROWS = [{"usajobsControlNumber": 841163600, "positionOpenDate": "2026-04-01"},
         {"usajobsControlNumber": 841164000, "positionOpenDate": "2026-04-01"}]


def test_a_shared_questionnaire_is_credited_to_each_posting_linking_it():
    out = _extract(_ROWS, {841163600: ([_Q], False), 841164000: ([_Q], False)})
    assert sorted(out["usajobs_control_number"]) == [841163600, 841164000]


def test_an_inferred_guess_is_not_credited_to_a_second_posting():
    out = _extract(_ROWS, {841163600: ([_Q], False), 841164000: ([_Q], True)})
    assert list(out["usajobs_control_number"]) == [841163600]


def test_the_missing_pair_is_added_to_an_existing_csv():
    # The columns extract_all_links_to_csv writes, so appended rows line up.
    header = ["questionnaire_url", "usajobs_control_number", "position_title",
              "announcement_number", "hiring_agency", "occupation_series",
              "occupation_name", "position_open_date", "position_close_date",
              "position_location", "grade_code", "position_schedule", "service_type",
              "extracted_from_file", "extracted_date", "inferred_from_announcement",
              "inferred_from_posting_html"]
    existing = [{c: None for c in header}]
    existing[0].update({"questionnaire_url": _Q, "usajobs_control_number": 841163600,
                        "inferred_from_announcement": False,
                        "inferred_from_posting_html": False})
    out = _extract(_ROWS, {841163600: ([_Q], False), 841164000: ([_Q], False)},
                   existing=existing)
    assert sorted(out["usajobs_control_number"]) == [841163600, 841164000]
    assert len(out) == 2


TESTS = [
    test_closed_posting_takes_the_latest_close_date,
    test_reopened_closed_posting_keeps_its_first_open_date,
    test_open_postings_keep_todays_dates,
    test_postings_the_mirror_lacks_are_counted_not_guessed,
    test_a_missing_mirror_year_fails_the_build,
    test_latest_listings_read_across_years,
    test_mirror_years_reach_this_year,
    test_a_shared_questionnaire_is_credited_to_each_posting_linking_it,
    test_an_inferred_guess_is_not_credited_to_a_second_posting,
    test_the_missing_pair_is_added_to_an_existing_csv,
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
