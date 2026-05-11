#!/usr/bin/env python3
"""
export_csv.py — Export tracks DB to CSV, one file per country.

Row shape: one row per (track × chart appearance in that country).
Row gate:  track must have track_analysis.status='ok' (Essentia data present).
           Reccobeats data is included when available, blank when not.

Output:    exports/<timestamp>/charts_<country>.csv
           (one file per country found in chart_entries, unless --country narrows)
           With --combined, a single exports/<timestamp>/charts_all.csv is
           produced instead, containing every selected country in one file
           (the country column distinguishes the rows).

Backends: SQLite (default) or PostgreSQL. Pass --database-url
postgresql://user:pass@host/db (or set DATABASE_URL) to read from Postgres;
otherwise pass --db <path-to-sqlite-file>.

Examples:
  # All countries, default output dir (SQLite)
  python export_csv.py --db ./db/charts.sqlite

  # Read from Postgres
  python export_csv.py --database-url postgresql://user:pass@host/charts

  # Or via env var
  DATABASE_URL=postgresql://user:pass@host/charts python export_csv.py

  # Just IT, gzipped
  python export_csv.py --db ./db/charts.sqlite --country IT --gzip

  # Latest chart appearance per track only (one row per track per country)
  python export_csv.py --db ./db/charts.sqlite --latest-only

  # Custom output, restrict to recent weeks
  python export_csv.py --db ./db/charts.sqlite --since 2025-01-01 --out ./my_export

Notes on the row gate:
  By default a track is exported if track_analysis.status='ok'. This is the
  "taxonomy is filled" condition. Use --require-reccobeats to additionally
  require track_reccobeats.status='ok', --require-audio-features to require
  audio scalars from any source (Reccobeats OR the external CSV fallback),
  or --no-essentia-gate to drop the gate entirely (export every chart row,
  regardless of enrichment).

Audio-feature columns:
  The nine audio scalars (af_acousticness, af_danceability, ...) come from
  v_track_audio_features_merged: Reccobeats values first, falling back to
  track_audio_features_external when Reccobeats failed for that track. The
  af_source column records which source actually populated the row
  ('reccobeats', the external source string, or NULL if neither has data).
  duration_ms remains Reccobeats-only since the external sources don't
  carry it.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


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
]

# Per-country chart summary, joined on (track_id, country) of the chart entry
# so each row gets the totals for the country it appeared in. NULL for chart
# entries from countries not present in track_country_totals (e.g. countries
# that surfaced via a track-page scrape but were never explicitly discovered
# via {cc}_weekly_totals).
COUNTRY_TOTALS_COLS = [
    ("tct", "weeks_on",      "weeks_on_chart"),
    ("tct", "peak",          "peak_position"),
    ("tct", "total_streams", "total_streams_country"),
]

# Reccobeats-only metadata. duration_ms isn't an audio scalar — only Reccobeats
# carries it, so it stays sourced from track_reccobeats directly.
RECCOBEATS_COLS = [
    ("rb", "duration_ms",         "duration_ms"),
]

# Audio-feature scalars come from the merged view, which prefers Reccobeats
# values and falls back to track_audio_features_external. The af_source column
# records which one populated each row.
AUDIO_FEATURE_COLS = [
    ("vam", "acousticness",          "af_acousticness"),
    ("vam", "danceability",          "af_danceability"),
    ("vam", "energy",                "af_energy"),
    ("vam", "instrumentalness",      "af_instrumentalness"),
    ("vam", "liveness",              "af_liveness"),
    ("vam", "loudness",              "af_loudness"),
    ("vam", "speechiness",           "af_speechiness"),
    ("vam", "tempo",                 "af_tempo"),
    ("vam", "valence",               "af_valence"),
    ("vam", "audio_features_source", "af_source"),
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
    "moods_mirex", "genre_rosamerica",
]


# ----------------------------------------------------------------------------
# SQL builder
# ----------------------------------------------------------------------------
def build_select_sql(args: argparse.Namespace, country_count: int = 1) -> tuple[str, list[str]]:
    """
    Returns (sql, header_columns).

    `country_count` controls the WHERE clause: 1 → `ce.country = ?` (per-country
    export), N>1 → `ce.country IN (?, ?, ...)` (combined export across the
    selected countries). The caller passes the matching number of country
    placeholders in the params tuple.

    Joins:
        chart_entries -> tracks -> track_reccobeats (duration_ms only)
                                -> v_track_audio_features_merged (audio scalars)
                                -> track_analysis
        + pivoted high_level_binary (P(positive)) and categorical (winning class).
    """
    select_parts: list[str] = []
    header: list[str] = []

    for tbl, col, alias in (
        CHART_COLS + TRACK_COLS + COUNTRY_TOTALS_COLS
        + RECCOBEATS_COLS + AUDIO_FEATURE_COLS + ANALYSIS_COLS
    ):
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

    if country_count <= 1:
        country_clause = "ce.country = ?"
    else:
        placeholders = ", ".join(["?"] * country_count)
        country_clause = f"ce.country IN ({placeholders})"
    where: list[str] = [country_clause]
    if not args.no_essentia_gate:
        where.append("a.status = 'ok'")
    if args.require_reccobeats:
        where.append("rb.status = 'ok'")
    if args.require_audio_features:
        where.append("vam.audio_features_source IS NOT NULL")
    if args.since:
        where.append("ce.week_date >= ?")
    if args.min_streams is not None:
        # Per-country threshold against the country totals row that matches
        # ce.country. Tracks from countries with no totals row are excluded.
        where.append("tct.total_streams >= ?")
    where_sql = "WHERE " + " AND ".join(where)

    # Postgres requires every non-aggregated select column to appear in
    # GROUP BY (SQLite is permissive). List them all so the same query runs
    # on both backends.
    group_by_parts = [
        f"{tbl}.{col}"
        for tbl, col, _ in (
            CHART_COLS + TRACK_COLS + COUNTRY_TOTALS_COLS
            + RECCOBEATS_COLS + AUDIO_FEATURE_COLS + ANALYSIS_COLS
        )
    ]
    group_by_sql = ", ".join(group_by_parts)

    sql = f"""
        SELECT {select_sql}
        FROM chart_entries ce
        JOIN tracks t                                 ON t.track_id   = ce.track_id
        LEFT JOIN track_country_totals tct            ON tct.track_id = ce.track_id
                                                     AND tct.country  = ce.country
        LEFT JOIN track_reccobeats rb                 ON rb.track_id  = ce.track_id
        LEFT JOIN v_track_audio_features_merged vam   ON vam.track_id = ce.track_id
        LEFT JOIN track_analysis a                    ON a.track_id   = ce.track_id
        LEFT JOIN track_high_level_binary hb          ON hb.track_id  = ce.track_id
        LEFT JOIN track_high_level_categorical hc     ON hc.track_id  = ce.track_id
        {where_sql}
        GROUP BY {group_by_sql}
        ORDER BY ce.week_date DESC, ce.position ASC
    """
    return sql, header


def build_latest_only_sql(args: argparse.Namespace, country_count: int = 1) -> tuple[str, list[str]]:
    """One row per (track, country): the most recent chart_entries appearance.
    Partitioning by (track_id, country) keeps the semantics correct in both
    per-country exports (only one country in scope anyway) and combined
    exports (one latest row per country). The outer SELECT projects the
    header columns explicitly so the cursor returns exactly those columns
    in the right order — no Python-side filtering needed."""
    inner_sql, header = build_select_sql(args, country_count=country_count)
    cols = ", ".join(header)
    # Subquery aliases (AS base / AS ranked) are required by Postgres.
    wrapped = f"""
        SELECT {cols} FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY track_id, country
                       ORDER BY week_date DESC, position ASC
                   ) AS rn
            FROM ({inner_sql}) AS base
        ) AS ranked WHERE rn = 1
    """
    return wrapped, header


# ----------------------------------------------------------------------------
# I/O helpers
# ----------------------------------------------------------------------------
class DbHandle:
    """Minimal uniform handle over sqlite3 / psycopg2 connections.

    For Postgres, `execute()` returns a server-side (named) cursor so that
    large result sets stream from the server in chunks rather than being
    buffered in client memory. The combined export can produce millions of
    rows; a regular client-side cursor would OOM the process."""

    def __init__(self, raw: Any, backend: str) -> None:
        self.raw = raw
        self.backend = backend  # "sqlite" | "postgres"
        self._cursor_seq = 0

    def execute(self, sql: str, params: tuple = ()) -> Any:
        if self.backend == "postgres":
            # Unique cursor name lets the same connection open multiple
            # named cursors (e.g. list_countries + export) without clash.
            self._cursor_seq += 1
            cur = self.raw.cursor(name=f"csvexport_{self._cursor_seq}")
            cur.itersize = 5000  # rows per server fetch
            cur.execute(sql.replace("?", "%s"), params)
            return cur
        return self.raw.execute(sql, params)

    def close(self) -> None:
        if self.backend == "postgres":
            # End the read-only transaction cleanly before closing.
            try:
                self.raw.rollback()
            except Exception:
                pass
        self.raw.close()


def open_db(args: argparse.Namespace) -> DbHandle:
    """Open SQLite (read-only) or PostgreSQL based on flags / env."""
    db_url = args.database_url or os.environ.get("DATABASE_URL")
    if db_url:
        if not db_url.startswith(("postgresql://", "postgres://")):
            sys.exit(f"Unsupported database URL scheme: {db_url}")
        try:
            import psycopg2
        except ImportError:
            sys.exit("psycopg2 is required for Postgres. pip install psycopg2-binary")
        # Read-only session prevents accidental writes from this script.
        # autocommit=False is required: named (server-side) cursors only
        # work inside a real transaction. The transaction is rolled back
        # on close.
        conn = psycopg2.connect(db_url)
        conn.set_session(readonly=True, autocommit=False)
        return DbHandle(conn, "postgres")

    if args.db is None:
        sys.exit("Provide --db <sqlite-path> or --database-url / DATABASE_URL")
    db_path: Path = args.db
    if not db_path.exists():
        sys.exit(f"DB not found: {db_path}")
    # Open SQLite read-only via URI so the export never blocks worker writes.
    uri = f"file:{db_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return DbHandle(conn, "sqlite")


def list_countries(conn: DbHandle) -> list[str]:
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


def _run_query_to_csv(
    conn: DbHandle,
    sql: str,
    header: list[str],
    params: tuple,
    out_path: Path,
    gzip_output: bool,
) -> tuple[Path, int]:
    """Stream a query result into a CSV at `out_path`. Both build_select_sql
    and build_latest_only_sql project exactly the `header` columns in order,
    so we can write the header straight from `header` and each row
    positionally — no need to introspect `cur.description` (which a psycopg2
    server-side cursor doesn't populate until after the first fetch)."""
    cur = conn.execute(sql, params)
    fh, writer, final_path = open_writer(out_path, gzip_output)
    rows = 0
    try:
        writer.writerow(header)
        for row in cur:
            writer.writerow(list(row))
            rows += 1
    finally:
        fh.close()
        try:
            cur.close()
        except Exception:
            pass
    return final_path, rows


def export_country(
    conn: DbHandle,
    country: str,
    out_dir: Path,
    args: argparse.Namespace,
) -> tuple[Path, int]:
    if args.latest_only:
        sql, header = build_latest_only_sql(args, country_count=1)
    else:
        sql, header = build_select_sql(args, country_count=1)

    params: list = [country]
    if args.since:
        params.append(args.since)
    if args.min_streams is not None:
        params.append(args.min_streams)

    return _run_query_to_csv(
        conn, sql, header, tuple(params),
        out_dir / f"charts_{country}.csv", args.gzip,
    )


def export_combined(
    conn: DbHandle,
    countries: list[str],
    out_dir: Path,
    args: argparse.Namespace,
) -> tuple[Path, int]:
    """Export every selected country into a single `charts_all.csv`.
    Uses one SQL query with `ce.country IN (...)` so the result is streamed
    out in one pass — no per-country buffering or repeated queries."""
    if args.latest_only:
        sql, header = build_latest_only_sql(args, country_count=len(countries))
    else:
        sql, header = build_select_sql(args, country_count=len(countries))

    params: list = list(countries)
    if args.since:
        params.append(args.since)
    if args.min_streams is not None:
        params.append(args.min_streams)

    return _run_query_to_csv(
        conn, sql, header, tuple(params),
        out_dir / "charts_all.csv", args.gzip,
    )


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--db", type=Path, default=None,
                   help="Path to SQLite DB (omit when using --database-url).")
    p.add_argument("--database-url", type=str, default=None,
                   help="PostgreSQL URL (postgresql://user:pass@host/db). "
                        "Falls back to the DATABASE_URL env var.")
    p.add_argument("--out", type=Path, default=None,
                   help="Output dir (default: ./exports/<timestamp>/)")
    p.add_argument("--gzip", action="store_true", help="gzip the CSVs")

    p.add_argument("--country", action="append", default=[],
                   help="Restrict to country code (repeatable). Default: all countries.")

    p.add_argument("--combined", action="store_true",
                   help="Produce a single charts_all.csv across the selected "
                        "countries instead of one file per country. The country "
                        "column distinguishes the rows.")

    p.add_argument("--latest-only", action="store_true",
                   help="One row per (track, country): the most recent chart appearance.")

    p.add_argument("--no-essentia-gate", action="store_true",
                   help="Don't require Essentia analysis; export every chart row.")
    p.add_argument("--require-reccobeats", action="store_true",
                   help="Also require track_reccobeats.status='ok' (Reccobeats specifically).")
    p.add_argument("--require-audio-features", action="store_true",
                   help="Also require audio scalars from any source "
                        "(Reccobeats OR the external CSV fallback).")

    p.add_argument("--since", type=str, default=None,
                   help="Filter chart_entries.week_date >= ISO date (YYYY-MM-DD).")
    p.add_argument("--min-streams", type=int, default=None,
                   help="Filter track_country_totals.total_streams >= N for "
                        "the export's country (excludes tracks with no totals "
                        "row for that country).")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.out is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.out = Path("exports") / ts
    args.out.mkdir(parents=True, exist_ok=True)

    conn = open_db(args)

    countries = args.country or list_countries(conn)
    if not countries:
        sys.exit("No countries found in chart_entries (and none specified).")

    gate = "none" if args.no_essentia_gate else "essentia ok"
    if args.require_reccobeats:
        gate += " + reccobeats ok"
    if args.require_audio_features:
        gate += " + audio features (any source)"
    shape = "latest per (track,country)" if args.latest_only else "one row per chart entry"
    layout = "single combined file" if args.combined else "one file per country"

    if conn.backend == "postgres":
        # Don't echo the URL — it may contain credentials.
        source = "postgres (DATABASE_URL)" if not args.database_url else "postgres (--database-url)"
    else:
        source = str(args.db)

    print(f"DB:        {source}")
    print(f"Out:       {args.out}")
    print(f"Countries: {', '.join(countries)}")
    print(f"Gate:      {gate}")
    print(f"Shape:     {shape}")
    print(f"Layout:    {layout}")
    print()

    if args.combined:
        path, total = export_combined(conn, countries, args.out, args)
        print(f"  {path.name}: {total} rows ({len(countries)} countries)")
        print(f"\nTotal: {total} rows in 1 file.")
    else:
        total = 0
        for country in countries:
            path, n = export_country(conn, country, args.out, args)
            total += n
            print(f"  {path.name}: {n} rows")
        print(f"\nTotal: {total} rows across {len(countries)} file(s).")
    conn.close()


if __name__ == "__main__":
    main()