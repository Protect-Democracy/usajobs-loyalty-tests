"""Each posting's latest listing, from the usajobs_historical mirror.

Closed postings' close dates on the site used to be the ones in this repo's own
parquets, which are as each posting was first collected (current_jobs keeps the
Search API's view of a posting while it was open, and nothing here re-polls
historical_jobs). Agencies extend postings after they open, and re-open them
under the same control number with new dates. On 2026-11-01, 32,625 of the
site's 301,523 closed postings (10.8%) showed a close date different from the
Historical API's, 28,047 of them earlier; of 15 random EO-question postings
among them checked against the live API, 14 matched the mirror and none matched
the site (e.g. 842340200: site 8/6/2025, USAJOBS 8/18/2025).

The mirror (github.com/abigailhaddad/usajobs_historical) re-polls statuses and
dates nightly and keeps one row per control number holding its latest listing
(its scripts/listings.py, since 2026-11-01). It is public; no credentials.

If it cannot be read, the build fails. There is deliberately no fallback to the
local dates: they are the stale ones this exists to replace.
"""

import os
import tempfile
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import requests

MIRROR = os.environ.get(
    "USAJOBS_MIRROR_URL",
    "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/data")

COLUMNS = ["usajobsControlNumber", "positionOpenDate", "positionCloseDate"]


def mirror_years(cutoff_date):
    """Every year a listing of a posting opened on or after cutoff_date can
    be filed under: its first year through this one. A re-opened posting is
    filed under its latest listing's year, which can be later than the year
    it first opened."""
    return list(range(pd.to_datetime(cutoff_date).year, date.today().year + 1))


def load_latest_listings(years, base_url=MIRROR, session=None):
    """DataFrame of usajobs_control_number (str), latest_open, latest_close
    (YYYY-MM-DD strings) over the given years' mirror files.

    Raises if any year's file cannot be fetched or read.
    """
    session = session or requests.Session()
    frames = []
    with tempfile.TemporaryDirectory() as tmp:
        for year in years:
            url = f"{base_url}/historical_jobs_{year}.parquet"
            resp = session.get(url, timeout=300)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"usajobs_historical mirror: {url} returned HTTP "
                    f"{resp.status_code}; not building with stale close dates")
            path = os.path.join(tmp, f"{year}.parquet")
            with open(path, "wb") as f:
                f.write(resp.content)
            frames.append(pq.read_table(path, columns=COLUMNS).to_pandas())
    df = pd.concat(frames, ignore_index=True)
    out = pd.DataFrame({
        "usajobs_control_number": df["usajobsControlNumber"].astype("int64").astype(str),
        "latest_open": df["positionOpenDate"].astype(str).str[:10],
        "latest_close": df["positionCloseDate"].astype(str).str[:10],
    })
    out = out.replace({"latest_open": {"None": None, "nan": None},
                       "latest_close": {"None": None, "nan": None}})
    # One row per control number in the mirror; if a file ever repeats one,
    # the later listing is the latest.
    return (out.sort_values("latest_open", na_position="first")
               .drop_duplicates("usajobs_control_number", keep="last")
               .reset_index(drop=True))


def apply_latest_listing(jobs, latest, is_open):
    """Close and re-opened dates for postings not open today.

    jobs: frame with usajobs_control_number, position_open_date,
    position_close_date. latest: load_latest_listings(). is_open: boolean
    Series aligned with jobs -- open postings keep the dates from today's
    listing (open_postings.csv), which the caller has already applied.

    For each closed posting the mirror has: position_close_date becomes the
    latest listing's close date, and reopened_open_date is set to the latest
    listing's open date when it is later than the open date shown (the first
    listing's, as the site and the daily email show it). Returns the updated
    frame and a dict of counts for the build log.
    """
    jobs = jobs.copy()
    cn = jobs["usajobs_control_number"].astype(str)
    lat = latest.set_index("usajobs_control_number")
    in_mirror = cn.isin(lat.index)
    target = (~is_open) & in_mirror

    new_close = cn[target].map(lat["latest_close"])
    old_close = pd.to_datetime(jobs.loc[target, "position_close_date"],
                               format="mixed", errors="coerce").dt.strftime("%Y-%m-%d")
    changed = int((new_close.notna() & (new_close != old_close)).sum())
    jobs.loc[target & cn.map(lat["latest_close"]).notna(), "position_close_date"] = new_close

    shown_open = pd.to_datetime(jobs["position_open_date"], format="mixed",
                                errors="coerce").dt.strftime("%Y-%m-%d")
    latest_open = cn.map(lat["latest_open"])
    reopened = target & latest_open.notna() & shown_open.notna() & (latest_open > shown_open)
    jobs["reopened_open_date"] = None
    jobs.loc[reopened, "reopened_open_date"] = latest_open[reopened]

    counts = {
        "closed": int((~is_open).sum()),
        "closed_in_mirror": int(target.sum()),
        "closed_not_in_mirror": int(((~is_open) & ~in_mirror).sum()),
        "close_date_changed": changed,
        "reopened": int(reopened.sum()),
    }
    return jobs, counts
