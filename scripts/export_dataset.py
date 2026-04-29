"""
Export the scraped Kworb data from a working DB cache into a publishable,
citable dataset:

    dataset/
        chart_entries.parquet     # fact table (long format)
        chart_entries.csv         # same, archival format
        tracks.parquet            # dimension table
        tracks.csv
        README.md
        datapackage.json          # Frictionless Data descriptor

Backends: SQLite (default) or PostgreSQL.
  - SQLite: defaults to ./kworb_italy.db, override with DB_PATH env var.
  - Postgres: set DATABASE_URL=postgresql://user:pass@host/db
    (or pass --database-url on the command line).

Usage:
    pip install pandas pyarrow
    python export_dataset.py
    DATABASE_URL=postgresql://user:pass@host/charts python export_dataset.py
    python export_dataset.py --database-url postgresql://user:pass@host/charts

The Parquet files are what you'll actually use for analysis.
The CSVs + datapackage.json are for the paper / Zenodo upload.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd

DB_PATH = Path(os.environ.get("DB_PATH", "kworb_italy.db"))
OUT_DIR = Path("dataset")
COUNTRY_FILTER: list[str] | None = None  # e.g. ["IT"] to publish IT-only


@contextlib.contextmanager
def open_conn(database_url: str | None = None) -> Iterator:
    """Yield a DB-API connection for SQLite or PostgreSQL.

    Selection order: explicit --database-url > DATABASE_URL env > SQLite at DB_PATH.
    pandas.read_sql_query accepts either backend's connection.
    """
    url = database_url or os.environ.get("DATABASE_URL")
    if url and url.startswith(("postgresql://", "postgres://")):
        try:
            import psycopg2
        except ImportError as exc:
            raise SystemExit(
                "psycopg2 is required for Postgres. pip install psycopg2-binary"
            ) from exc
        conn = psycopg2.connect(url)
        try:
            yield conn
        finally:
            conn.close()
        return

    if not DB_PATH.exists():
        raise SystemExit(f"Cache DB not found: {DB_PATH}. Run the scraper first.")
    with sqlite3.connect(DB_PATH) as conn:
        yield conn

DATASET_VERSION = "1.0.0"
DATASET_TITLE = "Spotify Weekly Charts — Italy (2013–present)"
DATASET_DESCRIPTION = (
    "Long-format weekly Spotify chart history for Italy and co-occurring "
    "countries, reconstructed from per-track histories on kworb.net."
)


def load_frames(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    entries = pd.read_sql_query(
        "SELECT track_id, week_date, country, position, streams "
        "FROM chart_entries",
        conn,
        parse_dates=["week_date"],
    )
    if COUNTRY_FILTER is not None:
        entries = entries[entries["country"].isin(COUNTRY_FILTER)]

    # Only keep tracks actually present after country filtering.
    used_ids = entries["track_id"].unique()
    tracks = pd.read_sql_query(
        "SELECT track_id, title, artist, artist_id, "
        "total_streams AS total_streams_it, "
        "weeks_on_it AS weeks_on_chart_it, "
        "peak_it AS peak_position_it "
        "FROM tracks",
        conn,
    )
    tracks = tracks[tracks["track_id"].isin(used_ids)].reset_index(drop=True)

    # Type tightening — keeps Parquet small and clear.
    entries = entries.astype({
        "track_id": "string",
        "country": "string",
        "position": "int16",
        "streams": "int64",
    })
    tracks = tracks.astype({
        "track_id": "string",
        "title": "string",
        "artist": "string",
        "artist_id": "string",
        "total_streams_it": "Int64",
        "weeks_on_chart_it": "Int16",
        "peak_position_it": "Int16",
    })
    entries = entries.sort_values(["week_date", "country", "position"]).reset_index(drop=True)
    tracks = tracks.sort_values("track_id").reset_index(drop=True)
    return entries, tracks


def write_files(entries: pd.DataFrame, tracks: pd.DataFrame) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    entries.to_parquet(OUT_DIR / "chart_entries.parquet", compression="zstd", index=False)
    entries.to_csv(OUT_DIR / "chart_entries.csv", index=False, date_format="%Y-%m-%d")
    tracks.to_parquet(OUT_DIR / "tracks.parquet", compression="zstd", index=False)
    tracks.to_csv(OUT_DIR / "tracks.csv", index=False)


def write_readme(entries: pd.DataFrame, tracks: pd.DataFrame) -> None:
    min_d = entries["week_date"].min().date().isoformat()
    max_d = entries["week_date"].max().date().isoformat()
    countries = sorted(entries["country"].unique().tolist())

    md = f"""# {DATASET_TITLE}

**Version:** {DATASET_VERSION}
**Generated:** {datetime.now(timezone.utc).isoformat(timespec="seconds")}
**Coverage:** {min_d} → {max_d}
**Countries:** {", ".join(countries)}
**Tracks:** {len(tracks):,}
**Chart entries:** {len(entries):,}

## Files

- `chart_entries.parquet` / `.csv` — long-format fact table, one row per
  (track, ISO week, country) on the Spotify weekly Top-200 chart.
- `tracks.parquet` / `.csv` — track metadata (title, artist, IT totals).
- `datapackage.json` — Frictionless Data descriptor.

## Schema

### chart_entries

| column     | type   | description |
|------------|--------|-------------|
| track_id   | string | Spotify track ID |
| week_date  | date   | Chart week (Sunday, ISO 8601) |
| country    | string | ISO 3166-1 alpha-2 country code |
| position   | int16  | Chart position (1 = top) |
| streams    | int64  | Weekly stream count for that country |

### tracks

| column              | type   | description |
|---------------------|--------|-------------|
| track_id            | string | Spotify track ID (primary key) |
| title               | string | Track title |
| artist              | string | Primary/displayed artist name |
| artist_id           | string | Spotify artist ID |
| total_streams_it    | int64  | Cumulative Italian streams while charting |
| weeks_on_chart_it   | int16  | Weeks on Italian weekly chart |
| peak_position_it    | int16  | Best Italian chart position |

## Provenance

Reconstructed from per-track chart history pages on
[kworb.net](https://kworb.net/spotify/), which aggregates the official
Spotify weekly country charts. The Italian master index used is
`it_weekly_totals.html`. Pre-2017 data is partial: Spotify did not
publish a public Top 200 for Italy before 2017, so positions below the
top ~20 are not consistently available for 2013–2016.

## Known limitations

- Positions ≥ ~21 may be missing for 2013–2016.
- Stream counts reflect Spotify's public weekly chart; they are not the
  same as artist-dashboard streams and exclude private/local plays.
- Track and artist metadata is as displayed on Kworb; titles may include
  feature notation (`(feat. X)`) inconsistent across releases.

## How to load

```python
import pandas as pd
entries = pd.read_parquet("chart_entries.parquet")
tracks  = pd.read_parquet("tracks.parquet")

# Italian Top 50 for one week:
week = "2024-01-07"
top50 = (entries.query("country == 'IT' and week_date == @week and position <= 50")
                .merge(tracks, on="track_id")
                .sort_values("position"))
```

## Citation

If you use this dataset, please cite:

> [Your name]. *{DATASET_TITLE}*, version {DATASET_VERSION}, {datetime.now().year}.
> Derived from kworb.net.
"""
    (OUT_DIR / "README.md").write_text(md, encoding="utf-8")


def write_datapackage(entries: pd.DataFrame, tracks: pd.DataFrame) -> None:
    pkg = {
        "name": "spotify-weekly-charts-italy",
        "title": DATASET_TITLE,
        "version": DATASET_VERSION,
        "description": DATASET_DESCRIPTION,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "licenses": [{
            "name": "ODC-BY-1.0",
            "title": "Open Data Commons Attribution License 1.0",
            "path": "https://opendatacommons.org/licenses/by/1-0/",
        }],
        "sources": [{
            "title": "Kworb.net Spotify charts",
            "path": "https://kworb.net/spotify/",
        }],
        "resources": [
            {
                "name": "chart_entries",
                "path": "chart_entries.csv",
                "format": "csv",
                "mediatype": "text/csv",
                "encoding": "utf-8",
                "schema": {
                    "primaryKey": ["track_id", "week_date", "country"],
                    "foreignKeys": [{
                        "fields": "track_id",
                        "reference": {"resource": "tracks", "fields": "track_id"},
                    }],
                    "fields": [
                        {"name": "track_id", "type": "string"},
                        {"name": "week_date", "type": "date"},
                        {"name": "country", "type": "string"},
                        {"name": "position", "type": "integer"},
                        {"name": "streams", "type": "integer"},
                    ],
                },
            },
            {
                "name": "tracks",
                "path": "tracks.csv",
                "format": "csv",
                "mediatype": "text/csv",
                "encoding": "utf-8",
                "schema": {
                    "primaryKey": ["track_id"],
                    "fields": [
                        {"name": "track_id", "type": "string"},
                        {"name": "title", "type": "string"},
                        {"name": "artist", "type": "string"},
                        {"name": "artist_id", "type": "string"},
                        {"name": "total_streams_it", "type": "integer"},
                        {"name": "weeks_on_chart_it", "type": "integer"},
                        {"name": "peak_position_it", "type": "integer"},
                    ],
                },
            },
        ],
    }
    (OUT_DIR / "datapackage.json").write_text(json.dumps(pkg, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--database-url", type=str, default=None,
                   help="PostgreSQL URL (postgresql://user:pass@host/db). "
                        "Falls back to the DATABASE_URL env var, then SQLite at DB_PATH.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    with open_conn(args.database_url) as conn:
        entries, tracks = load_frames(conn)
    write_files(entries, tracks)
    write_readme(entries, tracks)
    write_datapackage(entries, tracks)
    print(f"Wrote dataset to {OUT_DIR.resolve()}")
    print(f"  {len(entries):,} chart entries, {len(tracks):,} tracks")
    print(f"  Coverage: {entries['week_date'].min().date()} → "
          f"{entries['week_date'].max().date()}")


if __name__ == "__main__":
    main()
