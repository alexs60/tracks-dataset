"""
Export the scraped Kworb data from a working DB cache into a publishable,
citable dataset:

    dataset/
        chart_entries.parquet         # fact table (long format)
        chart_entries.csv             # same, archival format
        tracks.parquet                # dimension table (track identification)
        tracks.csv
        track_country_totals.parquet  # dimension table (per-country summary)
        track_country_totals.csv
        README.md
        datapackage.json              # Frictionless Data descriptor

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

DATASET_VERSION = "2.0.0"
DATASET_TITLE = "Spotify Weekly Charts — Multi-country (2013–present)"
DATASET_DESCRIPTION = (
    "Long-format weekly Spotify chart history across multiple country charts "
    "(IT, ES, FR, DE, GB, US, PT, NL and any co-occurring countries), "
    "reconstructed from per-track histories on kworb.net."
)


def load_frames(conn) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    entries = pd.read_sql_query(
        "SELECT track_id, week_date, country, position, streams "
        "FROM chart_entries",
        conn,
        parse_dates=["week_date"],
    )
    if COUNTRY_FILTER is not None:
        entries = entries[entries["country"].isin(COUNTRY_FILTER)]

    used_ids = entries["track_id"].unique()
    tracks = pd.read_sql_query(
        "SELECT track_id, title, artist, artist_id FROM tracks",
        conn,
    )
    tracks = tracks[tracks["track_id"].isin(used_ids)].reset_index(drop=True)

    country_totals = pd.read_sql_query(
        "SELECT track_id, country, weeks_on, peak, total_streams, "
        "last_chart_week "
        "FROM track_country_totals",
        conn,
    )
    country_totals = country_totals[
        country_totals["track_id"].isin(used_ids)
    ].reset_index(drop=True)
    if COUNTRY_FILTER is not None:
        country_totals = country_totals[
            country_totals["country"].isin(COUNTRY_FILTER)
        ].reset_index(drop=True)

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
    })
    country_totals = country_totals.astype({
        "track_id": "string",
        "country": "string",
        "weeks_on": "Int16",
        "peak": "Int16",
        "total_streams": "Int64",
        "last_chart_week": "string",
    })

    entries = entries.sort_values(
        ["week_date", "country", "position"]
    ).reset_index(drop=True)
    tracks = tracks.sort_values("track_id").reset_index(drop=True)
    country_totals = country_totals.sort_values(
        ["track_id", "country"]
    ).reset_index(drop=True)
    return entries, tracks, country_totals


def write_files(
    entries: pd.DataFrame,
    tracks: pd.DataFrame,
    country_totals: pd.DataFrame,
) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    entries.to_parquet(
        OUT_DIR / "chart_entries.parquet", compression="zstd", index=False
    )
    entries.to_csv(
        OUT_DIR / "chart_entries.csv", index=False, date_format="%Y-%m-%d"
    )
    tracks.to_parquet(
        OUT_DIR / "tracks.parquet", compression="zstd", index=False
    )
    tracks.to_csv(OUT_DIR / "tracks.csv", index=False)
    country_totals.to_parquet(
        OUT_DIR / "track_country_totals.parquet",
        compression="zstd", index=False,
    )
    country_totals.to_csv(OUT_DIR / "track_country_totals.csv", index=False)


def write_readme(
    entries: pd.DataFrame,
    tracks: pd.DataFrame,
    country_totals: pd.DataFrame,
) -> None:
    min_d = entries["week_date"].min().date().isoformat()
    max_d = entries["week_date"].max().date().isoformat()
    countries = sorted(entries["country"].unique().tolist())
    discovery_countries = sorted(country_totals["country"].unique().tolist())

    md = f"""# {DATASET_TITLE}

**Version:** {DATASET_VERSION}
**Generated:** {datetime.now(timezone.utc).isoformat(timespec="seconds")}
**Coverage:** {min_d} → {max_d}
**Countries in chart_entries:** {", ".join(countries)}
**Discovery countries (track_country_totals):** {", ".join(discovery_countries)}
**Tracks:** {len(tracks):,}
**Chart entries:** {len(entries):,}
**Per-country totals rows:** {len(country_totals):,}

## Files

- `chart_entries.parquet` / `.csv` — long-format fact table, one row per
  (track, ISO week, country) on the Spotify weekly Top-200 chart.
- `tracks.parquet` / `.csv` — track metadata (title, artist).
- `track_country_totals.parquet` / `.csv` — per-country summary stats
  (weeks_on, peak, total_streams) for each track on each chart it
  appeared in.
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

| column     | type   | description |
|------------|--------|-------------|
| track_id   | string | Spotify track ID (primary key) |
| title      | string | Track title |
| artist     | string | Primary/displayed artist name |
| artist_id  | string | Spotify artist ID |

### track_country_totals

| column          | type   | description |
|-----------------|--------|-------------|
| track_id        | string | Spotify track ID |
| country         | string | ISO 3166-1 alpha-2 country code |
| weeks_on        | int16  | Weeks the track appeared on this country's chart |
| peak            | int16  | Best chart position in this country (1 = top) |
| total_streams   | int64  | Cumulative streams while charting in this country |
| last_chart_week | string | Most recent week the track charted in this country |

Primary key: (track_id, country).

## Provenance

Reconstructed from per-track chart history pages on
[kworb.net](https://kworb.net/spotify/), which aggregates the official
Spotify weekly country charts. Track discovery is seeded from each
country's `{{cc}}_weekly_totals.html` index. Pre-2017 data is partial:
Spotify did not publish a public Top 200 for some countries before 2017,
so positions below the top ~20 are not consistently available for
2013–2016.

## Known limitations

- Positions ≥ ~21 may be missing for 2013–2016 in some countries.
- Stream counts reflect Spotify's public weekly chart; they are not the
  same as artist-dashboard streams and exclude private/local plays.
- `chart_entries` may include countries beyond the discovery set
  (`track_country_totals.country`), because each track's per-track page
  contains weekly entries for all countries kworb tracks. Those rows are
  authoritative chart data; they just lack a per-country summary row.
- Track and artist metadata is as displayed on Kworb; titles may include
  feature notation (`(feat. X)`) inconsistent across releases.

## How to load

```python
import pandas as pd
entries = pd.read_parquet("chart_entries.parquet")
tracks  = pd.read_parquet("tracks.parquet")
totals  = pd.read_parquet("track_country_totals.parquet")

# UK Top 50 for one week:
week = "2024-01-07"
top50 = (entries.query("country == 'GB' and week_date == @week and position <= 50")
                .merge(tracks, on="track_id")
                .sort_values("position"))

# Tracks that hit #1 in at least 3 countries:
multi_no1 = (totals.query("peak == 1")
                   .groupby("track_id").size()
                   .loc[lambda s: s >= 3]
                   .index)
```

## Citation

If you use this dataset, please cite:

> [Your name]. *{DATASET_TITLE}*, version {DATASET_VERSION}, {datetime.now().year}.
> Derived from kworb.net.
"""
    (OUT_DIR / "README.md").write_text(md, encoding="utf-8")


def write_datapackage(
    entries: pd.DataFrame,
    tracks: pd.DataFrame,
    country_totals: pd.DataFrame,
) -> None:
    pkg = {
        "name": "spotify-weekly-charts-multicountry",
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
                    ],
                },
            },
            {
                "name": "track_country_totals",
                "path": "track_country_totals.csv",
                "format": "csv",
                "mediatype": "text/csv",
                "encoding": "utf-8",
                "schema": {
                    "primaryKey": ["track_id", "country"],
                    "foreignKeys": [{
                        "fields": "track_id",
                        "reference": {"resource": "tracks", "fields": "track_id"},
                    }],
                    "fields": [
                        {"name": "track_id", "type": "string"},
                        {"name": "country", "type": "string"},
                        {"name": "weeks_on", "type": "integer"},
                        {"name": "peak", "type": "integer"},
                        {"name": "total_streams", "type": "integer"},
                        {"name": "last_chart_week", "type": "date"},
                    ],
                },
            },
        ],
    }
    (OUT_DIR / "datapackage.json").write_text(
        json.dumps(pkg, indent=2), encoding="utf-8"
    )


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
        entries, tracks, country_totals = load_frames(conn)
    write_files(entries, tracks, country_totals)
    write_readme(entries, tracks, country_totals)
    write_datapackage(entries, tracks, country_totals)
    print(f"Wrote dataset to {OUT_DIR.resolve()}")
    print(f"  {len(entries):,} chart entries, {len(tracks):,} tracks, "
          f"{len(country_totals):,} country-totals rows")
    print(f"  Coverage: {entries['week_date'].min().date()} → "
          f"{entries['week_date'].max().date()}")


if __name__ == "__main__":
    main()
