"""
Draws a data quality trend chart from the dq_results and pipeline_runs tables.

    python include/tools/dq_trend.py

Writes docs/dq_trend.html (open it in a browser). It has two charts, one point per pipeline run:
  1. rows failing a check, summed over all checks
  2. checks that ended in FAIL (Critical) or WARN (Warning)
A run that failed before the checks ran is shown as a cross at zero. Needs no extra packages.
"""
import math
from pathlib import Path

import duckdb

INCLUDE = Path(__file__).resolve().parents[1]
DB = INCLUDE / "warehouse_saksham.duckdb"
OUT = INCLUDE.parent / "docs" / "dq_trend.html"
ETL, ELT = "nammamart_etl_saksham", "nammamart_elt_saksham"
BLUE, ORANGE, RED, GREY = "#2563eb", "#d97706", "#dc2626", "#6b7280"

con = duckdb.connect(str(DB), read_only=True)
runs = con.execute("""
    SELECT r.dag_id, r.run_id, r.start_time, r.status,
           COALESCE(SUM(d.rows_failed), 0) AS rows_failed,
           COUNT(*) FILTER (WHERE d.status = 'FAIL') AS fail_checks,
           COUNT(*) FILTER (WHERE d.status = 'WARN') AS warn_checks,
           COUNT(d.check_name) AS checks
    FROM pipeline_runs r LEFT JOIN dq_results d ON d.run_id = r.run_id AND d.dag_id = r.dag_id
    GROUP BY ALL ORDER BY r.start_time
""").fetchall()

W, H, L, R, T, B = 1100, 300, 70, 20, 30, 60
n = len(runs)


def x(i):
    return L + (W - L - R) * (i + 0.5) / n


def nice(m):
    """Round the top of the axis up so the four gridline labels are round numbers."""
    step = 10 ** math.floor(math.log10(max(m, 4) / 4))
    return math.ceil(m / 4 / step) * step * 4


def chart(title, series, ymax):
    """series: list of (label, colour, dash, [(index, value, failed_run)])"""
    out = [f'<h2>{title}</h2><svg viewBox="0 0 {W} {H}" role="img" aria-label="{title}">']
    for k in range(5):
        v = ymax * k / 4
        y = H - B - (H - B - T) * k / 4
        out.append(f'<line x1="{L}" x2="{W - R}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e5e7eb"/>'
                   f'<text x="{L - 8}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="{GREY}">{v:,.0f}</text>')
    step = max(1, n // 12)
    for i, rr in enumerate(runs):
        if i % step == 0:
            out.append(f'<text x="{x(i):.1f}" y="{H - B + 16}" text-anchor="end" font-size="10" fill="{GREY}" '
                       f'transform="rotate(-40 {x(i):.1f} {H - B + 16})">{rr[2]:%d %b %H:%M}</text>')
    for label, colour, dash, pts in series:
        good = [(i, v) for i, v, bad in pts if not bad]
        path = " ".join(("M" if j == 0 else "L") + f"{x(i):.1f},{H - B - (H - B - T) * v / ymax:.1f}"
                        for j, (i, v) in enumerate(good))
        out.append(f'<path d="{path}" fill="none" stroke="{colour}" stroke-width="2" stroke-dasharray="{dash}"/>')
        for i, v, bad in pts:
            cy = H - B - (H - B - T) * v / ymax
            if bad:
                out.append(f'<path d="M{x(i) - 5:.1f},{cy - 5:.1f} l10,10 m-10,0 l10,-10" stroke="{RED}" stroke-width="2"/>')
            else:
                out.append(f'<circle cx="{x(i):.1f}" cy="{cy:.1f}" r="3.5" fill="{colour}"/>')
    lx = L
    for label, colour, dash, _ in series:
        out.append(f'<line x1="{lx}" x2="{lx + 24}" y1="12" y2="12" stroke="{colour}" stroke-width="2" stroke-dasharray="{dash}"/>'
                   f'<text x="{lx + 30}" y="16" font-size="12" fill="#111827">{label}</text>')
        lx += 190
    out.append(f'<path d="M{lx},8 l8,8 m-8,0 l8,-8" stroke="{RED}" stroke-width="2"/>'
               f'<text x="{lx + 14}" y="16" font-size="12" fill="#111827">run failed before the checks</text></svg>')
    return "\n".join(out)


def pts(dag, col):
    return [(i, rr[col], rr[3] == "failed") for i, rr in enumerate(runs) if rr[0] == dag]


rows_max = nice(max([rr[4] for rr in runs] + [1]))
chk_max = max([rr[5] + rr[6] for rr in runs] + [1])
c1 = chart("Rows failing a check, per run (all checks added up)",
           [("ETL", BLUE, "0", pts(ETL, 4)), ("ELT", ORANGE, "6 4", pts(ELT, 4))], rows_max)
c2 = chart("Checks in FAIL or WARN state, per run",
           [("ETL FAIL (Critical)", BLUE, "0", pts(ETL, 5)), ("ETL WARN (Warning)", "#16a34a", "0", pts(ETL, 6)),
            ("ELT FAIL (Critical)", ORANGE, "6 4", pts(ELT, 5)), ("ELT WARN (Warning)", "#9333ea", "6 4", pts(ELT, 6))],
           nice(max([rr[5] for rr in runs] + [rr[6] for rr in runs] + [1])))

OUT.parent.mkdir(exist_ok=True)
OUT.write_text(f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>NammaMart data quality trend</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{{font-family:system-ui,sans-serif;margin:24px auto;max-width:1140px;padding:0 16px;color:#111827;background:#fff}}
h1{{font-size:20px}}h2{{font-size:15px;margin:28px 0 4px}}svg{{width:100%;height:auto}}p{{color:#374151;font-size:14px}}</style></head>
<body><h1>NammaMart data quality trend (Saksham Tandon)</h1>
<p>One point per pipeline run, in the order the runs started, from the dq_results table. The two pipelines run the same
checks, so the ETL and ELT lines sit on top of each other except in the experiments that changed the input files.
The first points are the clean runs of the September data; the spikes and drops are the observability experiments.</p>
{c1}
{c2}
<p>Generated by include/tools/dq_trend.py from {n} runs.</p></body></html>""")
print(f"wrote {OUT} from {n} runs")
