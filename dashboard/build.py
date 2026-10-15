"""Build dashboard/index.html: the gold marts embedded in a self-contained HTML page.

    python dashboard/build.py --project my-gcp-project            # read the marts from BigQuery
    python dashboard/build.py --from-json marts.json              # or from a JSON file

The BigQuery path needs `pip install google-cloud-bigquery` and Application Default
Credentials (`gcloud auth application-default login`). It reads the tables with
`list_rows`, which is a storage read: it creates no query job and costs nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent

# dataset id used by the page -> table in the gold dataset
TABLES = {
    "daily": "agg_daily_trip_metrics",
    "monthly": "agg_monthly_summary",
    "hourweek": "agg_hour_of_week_profile",
    "boroughs": "agg_borough_od_flows",
    "airports": "agg_airport_trips",
    "payments": "agg_payment_mix",
    "zones": "agg_zone_scorecard",
    "toll": "agg_congestion_toll_weekly",
    "kpis": "agg_daily_kpis",
}


def _plain(value: object) -> object:
    """Make a BigQuery cell JSON-friendly: NUMERIC becomes a float, dates become ISO text."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def fetch_from_bigquery(project: str, dataset: str = "gold") -> dict[str, list[dict]]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    data = {}
    for key, table in TABLES.items():
        rows = client.list_rows(client.get_table(f"{project}.{dataset}.{table}"))
        data[key] = [{k: _plain(v) for k, v in dict(row.items()).items()} for row in rows]
    return data


def load_json(path: Path) -> dict[str, list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    missing = sorted(set(TABLES) - set(data))
    if missing:
        raise SystemExit(f"{path} is missing datasets: {', '.join(missing)}")
    return {key: data[key] for key in TABLES}


def render(template: str, data: dict[str, list[dict]], built_at: date) -> str:
    for placeholder in ("__DATA__", "__BUILT_AT__"):
        if template.count(placeholder) != 1:
            raise ValueError(f"template must contain {placeholder} exactly once")
    # "</" is escaped so a value can never close the <script> element it is embedded in.
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    return template.replace("__DATA__", payload).replace(
        "__BUILT_AT__", f"{built_at:%B} {built_at.day}, {built_at.year}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--project", help="GCP project that holds the gold dataset")
    source.add_argument("--from-json", type=Path, help="JSON file keyed by dataset id")
    parser.add_argument("--dataset", default="gold", help="BigQuery dataset (default: gold)")
    parser.add_argument("--template", type=Path, default=HERE / "template.html")
    parser.add_argument("--output", type=Path, default=HERE / "index.html")
    args = parser.parse_args(argv)

    data = (
        load_json(args.from_json)
        if args.from_json
        else fetch_from_bigquery(args.project, args.dataset)
    )
    html = render(args.template.read_text(encoding="utf-8"), data, date.today())
    args.output.write_text(html, encoding="utf-8", newline="\n")  # same bytes on every OS
    rows = sum(len(v) for v in data.values())
    print(f"wrote {args.output} ({len(html) / 1024:.0f} KiB, {rows} rows from {len(data)} marts)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
