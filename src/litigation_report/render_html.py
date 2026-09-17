"""Render the agency breakdown + per-posting detail as a browsable HTML page.

Replaces a CSV export so the USAJOBS links are actually clickable, and the
per-posting table is easier to scan/filter than raw CSV.
"""
import html as _html
from pathlib import Path

_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Loyalty Q — new postings detail</title>
<style>
  body {{
    font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
    color: #1a1a1a;
    max-width: 1100px;
    margin: 24px auto;
    padding: 0 16px;
  }}
  h1 {{ font-size: 20px; margin-bottom: 2px; }}
  .subtitle {{ color: #555; margin-bottom: 24px; }}
  h2 {{ font-size: 15px; border-bottom: 1px solid #ddd; padding-bottom: 6px; margin-top: 32px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ text-align: right; padding: 6px 10px; border-bottom: 1px solid #eee; }}
  th:first-child, td:first-child,
  th:nth-child(2), td:nth-child(2),
  th:nth-child(3), td:nth-child(3) {{ text-align: left; }}
  th {{ color: #555; font-weight: 600; position: sticky; top: 0; background: #fff; }}
  tr:hover {{ background: #f7f7f7; }}
  a {{ color: #1a56db; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .note {{ font-size: 12.5px; color: #555; font-style: italic; margin-top: 24px; line-height: 1.5; }}
  input#filter {{ padding: 6px 10px; width: 260px; margin: 12px 0; border: 1px solid #ccc; border-radius: 4px; }}
</style>
</head>
<body>
  <h1>Loyalty Q — new postings detail</h1>
  <div class="subtitle">Internal — litigation team only &middot; new postings since {start_date}, as of {as_of}</div>

  <h2>By agency</h2>
  <table>
    <thead>
      <tr><th>Agency</th><th>Total new</th><th>With Loyalty Q</th><th>Confirmed without</th><th>No link found</th></tr>
    </thead>
    <tbody>
      {agency_rows}
    </tbody>
  </table>

  <h2>Every new posting ({count:,} total)</h2>
  <input id="filter" type="text" placeholder="Filter by agency, title, or status...">
  <table id="detail-table">
    <thead>
      <tr><th>Opened</th><th>Agency</th><th>Position title</th><th>Control #</th><th>Status</th></tr>
    </thead>
    <tbody>
      {detail_rows}
    </tbody>
  </table>

  <div class="note">
    "no_questionnaire_link_found" is not simply pending review — most of this bucket is structurally
    unrecoverable (postings that apply through an agency-specific system other than USAStaffing or
    Monster Government, e.g. some Navy components). See loyalty_q_report.py for the full breakdown
    of known coverage gaps.
  </div>

  <script>
    document.getElementById('filter').addEventListener('input', function (e) {{
      var q = e.target.value.toLowerCase();
      document.querySelectorAll('#detail-table tbody tr').forEach(function (tr) {{
        tr.style.display = tr.textContent.toLowerCase().includes(q) ? '' : 'none';
      }});
    }});
  </script>
</body>
</html>
"""


def render_html(agency_df, detail_df, start_date, as_of, out_path):
    agency_rows = []
    for _, row in agency_df.iterrows():
        agency_rows.append(
            f"<tr><td>{_html.escape(str(row['hiring_agency']))}</td><td>{row['total_new']:,}</td>"
            f"<td>{row['new_with_loyalty_q']:,}</td><td>{row['new_confirmed_without_loyalty_q']:,}</td>"
            f"<td>{row['new_no_questionnaire_link_found']:,}</td></tr>"
        )

    detail_rows = []
    for _, row in detail_df.iterrows():
        opened = str(row['position_open_date'])[:10]
        title = _html.escape(str(row['position_title']))
        agency = _html.escape(str(row['hiring_agency']))
        status = _html.escape(str(row['loyalty_q_status']))
        link = _html.escape(str(row['usajobs_link']))
        cid = row['usajobs_control_number']
        detail_rows.append(
            f"<tr><td>{opened}</td><td>{agency}</td>"
            f"<td><a href=\"{link}\" target=\"_blank\" rel=\"noopener\">{title}</a></td>"
            f"<td>{cid}</td><td>{status}</td></tr>"
        )

    out_html = _TEMPLATE.format(
        start_date=start_date,
        as_of=as_of,
        agency_rows='\n      '.join(agency_rows),
        count=len(detail_df),
        detail_rows='\n      '.join(detail_rows),
    )

    out_path = Path(out_path)
    out_path.write_text(out_html)
    return out_path
