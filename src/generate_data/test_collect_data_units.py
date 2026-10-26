#!/usr/bin/env python3
"""Offline tests for collect_data.get_job_data_page. No network.

Run: cd src/generate_data && python test_collect_data_units.py
"""
import sys

import collect_data


class Resp:
    def __init__(self, status, text):
        self.status_code, self.text, self.url = status, text, 'https://data.usajobs.gov/api/historicjoa'

    def raise_for_status(self):
        if self.status_code >= 400:
            raise collect_data.requests.HTTPError(str(self.status_code))

    def json(self):
        import json
        return json.loads(self.text)


def with_response(resp, fn):
    original = collect_data.requests.get
    collect_data.requests.get = lambda *a, **k: resp
    try:
        return fn()
    finally:
        collect_data.requests.get = original


def test_empty_204_is_no_jobs_not_a_failure():
    # What the API sends since about 2026-10-24 for a day with no data yet.
    out = with_response(Resp(204, ''), lambda: collect_data.get_job_data_page(params={}))
    assert out == {'data': []}
    pages = with_response(Resp(204, ''), lambda: collect_data.fetch_all_pages({}))
    assert pages == []


def test_old_204_text_is_still_no_jobs():
    assert with_response(Resp(200, '204 No Content'), lambda: collect_data.get_job_data_page(params={})) == {'data': []}


def test_real_page_is_parsed():
    out = with_response(Resp(200, '{"data": [{"usajobsControlNumber": 1}]}'),
                        lambda: collect_data.get_job_data_page(params={}))
    assert out['data'][0]['usajobsControlNumber'] == 1


TESTS = [test_empty_204_is_no_jobs_not_a_failure, test_old_204_text_is_still_no_jobs, test_real_page_is_parsed]

if __name__ == '__main__':
    failed = 0
    for t in TESTS:
        try:
            t()
            print(f'PASS  {t.__name__}')
        except Exception as e:
            failed += 1
            print(f'FAIL  {t.__name__}  {e!r}')
    sys.exit(1 if failed else 0)
