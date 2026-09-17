"""Render the Loyalty Q litigation report as a one-page PDF.

Uses Playwright (already a project dependency) to print an HTML page to PDF,
rather than pulling in a separate PDF library.
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @page {{ size: Letter; margin: 0.6in; }}
  body {{
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    color: #1a1a1a;
    font-size: 11px;
  }}
  h1 {{ font-size: 18px; margin: 0 0 2px 0; }}
  .subtitle {{ color: #555; font-size: 12px; margin: 0 0 18px 0; }}
  .stats {{ display: flex; gap: 10px; margin-bottom: 22px; }}
  .stat {{
    flex: 1;
    border: 1px solid #ddd;
    border-radius: 6px;
    padding: 10px 12px;
  }}
  .stat .value {{ font-size: 22px; font-weight: 700; }}
  .stat .label {{ font-size: 10px; color: #555; margin-top: 2px; }}
  h2 {{ font-size: 13px; margin: 0 0 8px 0; border-bottom: 1px solid #ddd; padding-bottom: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 18px; }}
  tr {{ break-inside: avoid; }}
  th, td {{ text-align: right; padding: 5px 8px; border-bottom: 1px solid #eee; }}
  th:first-child, td:first-child {{ text-align: left; }}
  th {{ font-size: 10px; color: #555; font-weight: 600; }}
  .note {{
    font-size: 9.5px;
    color: #555;
    font-style: italic;
    border-top: 1px solid #ddd;
    padding-top: 8px;
    line-height: 1.4;
  }}
</style>
</head>
<body>
  <h1>Loyalty Q Compliance Report</h1>
  <div class="subtitle">Internal — litigation team only &middot; as of {as_of}</div>

  <h2>Live postings snapshot</h2>
  <div class="stats">
    <div class="stat"><div class="value">{total_live:,}</div><div class="label">Total live postings</div></div>
    <div class="stat"><div class="value">{live_with_q:,} ({pct_with_q:.1f}%)</div><div class="label">With Loyalty Q</div></div>
    <div class="stat"><div class="value">{live_without_q:,}</div><div class="label">Confirmed without Loyalty Q</div></div>
    <div class="stat"><div class="value">{live_unknown:,}</div><div class="label">No questionnaire link found</div></div>
  </div>

  <h2>Daily new postings since {start_date}</h2>
  <table>
    <thead>
      <tr><th>Date</th><th>Total new</th><th>With Loyalty Q</th><th>Confirmed without</th><th>No link found</th></tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>

  <h2>New postings since {start_date}, by agency</h2>
  <table>
    <thead>
      <tr><th>Agency</th><th>Total new</th><th>With Loyalty Q</th><th>Confirmed without</th><th>No link found</th></tr>
    </thead>
    <tbody>
      {agency_rows}
    </tbody>
  </table>

  <div class="note">
    Per-posting detail with a clickable USAJOBS link for every new posting since {start_date}
    is in the accompanying new_postings_detail.csv.
  </div>

  <div class="note">
    "No questionnaire link found" is not simply pending review — most of this bucket is structurally
    unrecoverable (postings that apply through an agency-specific system other than USAStaffing or
    Monster Government, e.g. some Navy components). It should not be expected to trend toward zero.
    See loyalty_q_report.py for the full breakdown of known coverage gaps.
  </div>
</body>
</html>
"""


def render_pdf(daily_df, agency_df, snapshot, start_date, out_path):
    pct_with_q = (
        snapshot['live_with_loyalty_q'] / snapshot['total_live_postings'] * 100
        if snapshot['total_live_postings'] else 0
    )

    row_html = []
    for _, row in daily_df.iterrows():
        row_html.append(
            f"<tr><td>{row['open_date']}</td><td>{row['total_new']:,}</td>"
            f"<td>{row['new_with_loyalty_q']:,}</td><td>{row['new_confirmed_without_loyalty_q']:,}</td>"
            f"<td>{row['new_no_questionnaire_link_found']:,}</td></tr>"
        )

    agency_row_html = []
    for _, row in agency_df.iterrows():
        agency_row_html.append(
            f"<tr><td>{row['hiring_agency']}</td><td>{row['total_new']:,}</td>"
            f"<td>{row['new_with_loyalty_q']:,}</td><td>{row['new_confirmed_without_loyalty_q']:,}</td>"
            f"<td>{row['new_no_questionnaire_link_found']:,}</td></tr>"
        )

    html = _TEMPLATE.format(
        as_of=snapshot['as_of'],
        total_live=snapshot['total_live_postings'],
        live_with_q=snapshot['live_with_loyalty_q'],
        pct_with_q=pct_with_q,
        live_without_q=snapshot['live_confirmed_without_loyalty_q'],
        live_unknown=snapshot['live_no_questionnaire_link_found'],
        start_date=start_date,
        rows='\n      '.join(row_html),
        agency_rows='\n      '.join(agency_row_html),
    )

    out_path = Path(out_path)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html)
        page.pdf(path=str(out_path), format='Letter', print_background=True,
                 margin={'top': '0', 'bottom': '0', 'left': '0', 'right': '0'})
        browser.close()
    return out_path
