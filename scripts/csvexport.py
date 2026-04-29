#!/usr/bin/env python3
"""
export_csv.py — Export tracks DB to CSV, one file per country.

Row shape: one row per (track × chart appearance in that country).
Row gate:  track must have track_analysis.status='ok' (Essentia data present).
           Reccobeats data is included when available, blank when not.

Output:    exports/<timestamp>/charts_<country>.csv
           (one file per country found in chart_entries, unless --country narrows)

Examples:
  # All countries, default output dir
  python export_csv.py --db ./db/charts.sqlite

  # Just IT, gzipped
  python export_csv.py --db ./db/charts.sqlite --country IT --gzip

  # Latest chart appearance per track only (one row per track per country)
  python export_csv.py --db ./db/charts.sqlite --latest-only

  # Custom output, restrict to recent weeks
  python export_csv.py --db ./db/charts.sqlite --since 2025-01-01 --out ./my_export

Notes on the row gate:
  By default a track is exported if track_analysis.status='ok'. This is the
  "taxonomy is filled" condition. Use --require-reccobeats to additionally
  require track_reccobeats.status='ok', or --no-essentia-gate to drop the gate
  entirely (export every chart row, regardless of enrichment).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


# ----------------------------------------------------------------------------
# Column projections. Explicit so the header is stable and we never leak raw_json.
# Each tuple: (table_alias, db_column, output_alias)
# ----------------------------------------------------------------------------

CHART_COLS = [
    ("ce", "week_date", "week_date"),
    ("ce", "country",   "country"),
    ("ce", "position",  "position"),
    ("ce", "streams",   "streams"),
]

TRACK_COLS = [
    ("t", "track_id",      "track_id"),
    ("t", "title",         "title"),
    ("t", "artist",        "artist"),
    ("t", "artist_id",     "artist_id"),
    ("t", "total_streams", "total_streams"),
    ("t", "weeks_on_it",   "weeks_on_it"),
    ("t", "peak_it",       "peak_it"),
]

RECCOBEATS_COLS = [
    ("rb", "reccobeats_id",       "rb_id"),
    ("rb", "isrc",                "isrc"),
    ("rb", "ean",                 "ean"),
    ("rb", "upc",                 "upc"),
    ("rb", "duration_ms",         "duration_ms"),
    ("rb", "acousticness",        "rb_acousticness"),
    ("rb", "danceability",        "rb_danceability"),
    ("rb", "energy",              "rb_energy"),
    ("rb", "instrumentalness",    "rb_instrumentalness"),
    ("rb", "liveness",            "rb_liveness"),
    ("rb", "loudness",            "rb_loudness"),
    ("rb", "speechiness",         "rb_speechiness"),
    ("rb", "tempo",               "rb_tempo"),
    ("rb", "valence",             "rb_valence"),
    ("rb", "status",              "rb_status"),
]

ANALYSIS_COLS = [
    ("a", "bpm",                 "es_bpm"),
    ("a", "bpm_confidence",      "es_bpm_confidence"),
    ("a", "danceability_raw",    "es_danceability_raw"),
    ("a", "loudness_ebu128",     "es_loudness_ebu128"),
    ("a", "average_loudness",    "es_average_loudness"),
    ("a", "dynamic_complexity",  "es_dynamic_complexity"),
    ("a", "key_key",             "es_key_key"),
    ("a", "key_scale",           "es_key_scale"),
    ("a", "key_strength",        "es_key_strength"),
    ("a", "chords_changes_rate", "es_chords_changes_rate"),
    ("a", "tuning_frequency",    "es_tuning_frequency"),
    ("a", "onset_rate",          "es_onset_rate"),
    ("a", "duration_sec",        "es_duration_sec"),
    ("a", "extractor_version",   "es_extractor_version"),
    ("a", "models_version",      "es_models_version"),
    ("a", "analyzed_at",         "es_analyzed_at"),
]

# Pivoted from the long-format high-level tables.
HLB_CLASSIFIERS = [
    "timbre", "tonal_atonal", "danceability", "voice_instrumental",
    "gender", "mood_acoustic", "mood_electronic", "mood_relaxed",
    "mood_sad", "mood_party", "mood_happy", "mood_aggressive",
]
HLC_CLASSIFIERS = [
    "moods_mirex", "genre_electronic", "genre_tzanetakis",
    "genre_dortmund", "genre_rosamerica", "rhythm_ismir04",
]


# ----------------------------------------------------------------------------
# SQL builder
# ----------------------------------------------------------------------------
def build_select_sql(args: argparse.Namespace) -> tuple[str, list[str]]:
    """
    Returns (sql, header_columns).

    Joins:
        chart_entries -> tracks -> track_reccobeats -> track_analysis
        + pivoted high_level_binary (P(positive)) and categorical (winning class).
    """
    select_parts: list[str] = []
    header: list[str] = []

    for tbl, col, alias in CHART_COLS + TRACK_COLS + RECCOBEATS_COLS + ANALYSIS_COLS:
        select_parts.append(f"{tbl}.{col} AS {alias}")
        header.append(alias)

    # Binary classifiers: one column per classifier, value = P(positive class).
    for clf in HLB_CLASSIFIERS:
        col = f"p_{clf}"
        select_parts.append(
            f"MAX(CASE WHEN hb.classifier='{clf}' THEN hb.prob_positive END) AS {col}"
        )
        header.append(col)

    # Categorical classifiers: winning class label + its probability.
    for clf in HLC_CLASSIFIERS:
        col_v = f"cat_{clf}"
        col_p = f"p_cat_{clf}"
        select_parts.append(
            f"MAX(CASE WHEN hc.classifier='{clf}' THEN hc.value END) AS {col_v}"
        )
        select_parts.append(
            f"MAX(CASE WHEN hc.classifier='{clf}' THEN hc.probability END) AS {col_p}"
        )
        header.extend([col_v, col_p])

    select_sql = ",\n           ".join(select_parts)

    where: list[str] = ["ce.country = ?"]
    if not args.no_essentia_gate:
        where.append("a.status = 'ok'")
    if args.require_reccobeats:
        where.append("rb.status = 'ok'")
    if args.since:
        where.append("ce.week_date >= ?")
    if args.min_streams is not None:
        where.append("t.total_streams >= ?")
    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT {select_sql}
        FROM chart_entries ce
        JOIN tracks t                              ON t.track_id  = ce.track_id
        LEFT JOIN track_reccobeats rb              ON rb.track_id = ce.track_id
        LEFT JOIN track_analysis a                 ON a.track_id  = ce.track_id
        LEFT JOIN track_high_level_binary hb       ON hb.track_id = ce.track_id
        LEFT JOIN track_high_level_categorical hc  ON hc.track_id = ce.track_id
        {where_sql}
        GROUP BY ce.track_id, ce.week_date, ce.country
        ORDER BY ce.week_date DESC, ce.position ASC
    """
    return sql, header


def build_latest_only_sql(args: argparse.Namespace) -> tuple[str, list[str]]:
    """One row per (track, country): the most recent chart_entries appearance."""
    inner_sql, header = build_select_sql(args)
    wrapped = f"""
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY track_id ORDER BY week_date DESC, position ASC
                   ) AS rn
            FROM ({inner_sql})
        ) WHERE rn = 1
    """
    return wrapped, header


# ----------------------------------------------------------------------------
# I/O helpers
# ----------------------------------------------------------------------------
def open_db(db_path: Path) -> sqlite3.Connection:
    """Open SQLite read-only via URI so the export never blocks worker writes."""
    if not db_path.exists():
        sys.exit(f"DB not found: {db_path}")
    uri = f"file:{db_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def list_countries(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT DISTINCT country FROM chart_entries ORDER BY country"
    )
    return [r[0] for r in cur.fetchall()]


def open_writer(path: Path, gzip_output: bool):
    if gzip_output:
        path = path.with_suffix(path.suffix + ".gz")
        fh = gzip.open(path, "wt", newline="", encoding="utf-8")
    else:
        fh = open(path, "w", newline="", encoding="utf-8")
    return fh, csv.writer(fh, quoting=csv.QUOTE_MINIMAL), path


def export_country(
    conn: sqlite3.Connection,
    country: str,
    out_dir: Path,
    args: argparse.Namespace,
) -> tuple[Path, int]:
    if args.latest_only:
        sql, header = build_latest_only_sql(args)
    else:
        sql, header = build_select_sql(args)

    params: list = [country]
    if args.since:
        params.append(args.since)
    if args.min_streams is not None:
        params.append(args.min_streams)

    cur = conn.execute(sql, tuple(params))
    actual_cols = [d[0] for d in cur.description]
    # In --latest-only the wrapped query exposes 'rn'; keep only declared header cols.
    keep_idx = [i for i, c in enumerate(actual_cols) if c in header]
    keep_header = [actual_cols[i] for i in keep_idx]

    out = out_dir / f"charts_{country}.csv"
    fh, writer, final_path = open_writer(out, args.gzip)
    rows = 0
    try:
        writer.writerow(keep_header)
        for row in cur:
            writer.writerow([row[i] for i in keep_idx])
            rows += 1
    finally:
        fh.close()
    return final_path, rows


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--db", required=True, type=Path, help="Path to SQLite DB")
    p.add_argument("--out", type=Path, default=None,
                   help="Output dir (default: ./exports/<timestamp>/)")
    p.add_argument("--gzip", action="store_true", help="gzip the CSVs")

    p.add_argument("--country", action="append", default=[],
                   help="Restrict to country code (repeatable). Default: all countries.")

    p.add_argument("--latest-only", action="store_true",
                   help="One row per (track, country): the most recent chart appearance.")

    p.add_argument("--no-essentia-gate", action="store_true",
                   help="Don't require Essentia analysis; export every chart row.")
    p.add_argument("--require-reccobeats", action="store_true",
                   help="Also require track_reccobeats.status='ok'.")

    p.add_argument("--since", type=str, default=None,
                   help="Filter chart_entries.week_date >= ISO date (YYYY-MM-DD).")
    p.add_argument("--min-streams", type=int, default=None,
                   help="Filter tracks.total_streams >= N.")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.out is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.out = Path("exports") / ts
    args.out.mkdir(parents=True, exist_ok=True)

    conn = open_db(args.db)

    countries = args.country or list_countries(conn)
    if not countries:
        sys.exit("No countries found in chart_entries (and none specified).")

    gate = "none" if args.no_essentia_gate else "essentia ok"
    if args.require_reccobeats:
        gate += " + reccobeats ok"
    shape = "latest per (track,country)" if args.latest_only else "one row per chart entry"

    print(f"DB:        {args.db}")
    print(f"Out:       {args.out}")
    print(f"Countries: {', '.join(countries)}")
    print(f"Gate:      {gate}")
    print(f"Shape:     {shape}")
    print()

    total = 0
    for country in countries:
        path, n = export_country(conn, country, args.out, args)
        total += n
        print(f"  {path.name}: {n} rows")
    print(f"\nTotal: {total} rows across {len(countries)} file(s).")


if __name__ == "__main__":
    main()