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
  tr.post-order {{ background: #fff8e6; }}
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

  <h2>Live postings split by court order date ({order_date})</h2>
  <table>
    <thead>
      <tr><th></th><th>Total live</th><th>With Loyalty Q</th><th>Confirmed without</th><th>No link found</th></tr>
    </thead>
    <tbody>
      <tr>
        <td>Pre-order (opened before {order_date})</td>
        <td>{pre_total:,}</td><td>{pre_with_q:,} ({pre_pct:.1f}%)</td>
        <td>{pre_without_q:,}</td><td>{pre_unknown:,}</td>
      </tr>
      <tr>
        <td>Post-order (opened on/after {order_date})</td>
        <td>{post_total:,}</td><td>{post_with_q:,} ({post_pct:.1f}%)</td>
        <td>{post_without_q:,}</td><td>{post_unknown:,}</td>
      </tr>
    </tbody>
  </table>
  <div class="note">
    Pre-order postings still live with the Loyalty Q are candidates for takedown/edit;
    post-order postings with the Q are new violations opened after the order.
  </div>

  <h2>Weekly trend, last {n_weeks} weeks (highlighted = on/after order date {order_date})</h2>
  <table>
    <thead>
      <tr><th>Week of</th><th>Total new</th><th>With Loyalty Q</th><th>% with Q</th><th>Confirmed without</th><th>No link found</th></tr>
    </thead>
    <tbody>
      {weekly_rows}
    </tbody>
  </table>
  <div class="note">
    Full weekly history back to {earliest_week} is in the accompanying new_postings_detail.html.
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

  <div class="note">
    Full breakdown by agency, and a clickable USAJOBS link for every new posting since {start_date},
    is in the accompanying new_postings_detail.html.
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


def render_pdf(daily_df, snapshot, order_split, weekly_df, start_date, order_date, out_path):
    pct_with_q = (
        snapshot['live_with_loyalty_q'] / snapshot['total_live_postings'] * 100
        if snapshot['total_live_postings'] else 0
    )
    pre_pct = (
        order_split['pre_order_live_with_loyalty_q'] / order_split['total_pre_order_live'] * 100
        if order_split['total_pre_order_live'] else 0
    )
    post_pct = (
        order_split['post_order_live_with_loyalty_q'] / order_split['total_post_order_live'] * 100
        if order_split['total_post_order_live'] else 0
    )

    row_html = []
    for _, row in daily_df.iterrows():
        row_html.append(
            f"<tr><td>{row['open_date']}</td><td>{row['total_new']:,}</td>"
            f"<td>{row['new_with_loyalty_q']:,}</td><td>{row['new_confirmed_without_loyalty_q']:,}</td>"
            f"<td>{row['new_no_questionnaire_link_found']:,}</td></tr>"
        )

    n_weeks = 8
    recent_weeks = weekly_df.tail(n_weeks)
    weekly_row_html = []
    for _, row in recent_weeks.iterrows():
        row_class = ' class="post-order"' if row['is_post_order'] else ''
        weekly_row_html.append(
            f"<tr{row_class}><td>{row['week_start']}</td><td>{row['total_new']:,}</td>"
            f"<td>{row['new_with_loyalty_q']:,}</td><td>{row['pct_with_loyalty_q']:.1f}%</td>"
            f"<td>{row['new_confirmed_without_loyalty_q']:,}</td>"
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
        order_date=order_date,
        pre_total=order_split['total_pre_order_live'],
        pre_with_q=order_split['pre_order_live_with_loyalty_q'],
        pre_pct=pre_pct,
        pre_without_q=order_split['pre_order_live_confirmed_without_loyalty_q'],
        pre_unknown=order_split['pre_order_live_no_questionnaire_link_found'],
        post_total=order_split['total_post_order_live'],
        post_with_q=order_split['post_order_live_with_loyalty_q'],
        post_pct=post_pct,
        post_without_q=order_split['post_order_live_confirmed_without_loyalty_q'],
        post_unknown=order_split['post_order_live_no_questionnaire_link_found'],
        n_weeks=n_weeks,
        earliest_week=weekly_df.iloc[0]['week_start'],
        weekly_rows='\n      '.join(weekly_row_html),
        rows='\n      '.join(row_html),
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
