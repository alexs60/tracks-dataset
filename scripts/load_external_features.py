"""
Load a Spotify-features CSV into the external_audio_features_raw staging table.

The defaults match the column names in
maharshipandya/-spotify-tracks-dataset on Kaggle. Override --*-col flags
when loading a CSV with a different schema.

Examples:
    # Maharshipandya dataset (default column names)
    python scripts/load_external_features.py \\
        --csv ~/Downloads/spotify_tracks.csv \\
        --source kaggle_maharshipandya

    # Custom column names + ISRC available
    python scripts/load_external_features.py \\
        --csv ~/Downloads/other_dump.csv \\
        --source kaggle_other \\
        --spotify-id-col spotify_track_id \\
        --isrc-col isrc

The script is idempotent: re-running with the same --source upserts rows.
Pass --replace to clear that source before loading (useful when the CSV
schema changed).
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect, transaction


load_repo_env(PROJECT_ROOT)

FEATURE_COLS = (
    "acousticness", "danceability", "energy",
    "instrumentalness", "liveness", "loudness",
    "speechiness", "tempo", "valence",
)

INSERT_SQL = """
INSERT INTO external_audio_features_raw (
    source, spotify_id, isrc,
    acousticness, danceability, energy,
    instrumentalness, liveness, loudness,
    speechiness, tempo, valence
) VALUES (
    :source, :spotify_id, :isrc,
    :acousticness, :danceability, :energy,
    :instrumentalness, :liveness, :loudness,
    :speechiness, :tempo, :valence
)
ON CONFLICT (source, spotify_id) DO UPDATE SET
    isrc             = excluded.isrc,
    acousticness     = excluded.acousticness,
    danceability     = excluded.danceability,
    energy           = excluded.energy,
    instrumentalness = excluded.instrumentalness,
    liveness         = excluded.liveness,
    loudness         = excluded.loudness,
    speechiness      = excluded.speechiness,
    tempo            = excluded.tempo,
    valence          = excluded.valence
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--csv", type=Path, required=True, help="Path to the CSV file.")
    p.add_argument("--source", required=True,
                   help="Source identifier stored in the table (e.g. 'kaggle_maharshipandya').")
    p.add_argument("--spotify-id-col", default="track_id",
                   help="CSV column containing the Spotify track ID (default: track_id).")
    p.add_argument("--isrc-col", default=None,
                   help="Optional CSV column containing the ISRC.")
    p.add_argument("--batch-size", type=int, default=1000, help="Insert batch size.")
    p.add_argument("--replace", action="store_true",
                   help="Delete existing rows for this source before loading.")
    for col in FEATURE_COLS:
        p.add_argument(f"--{col}-col", default=col,
                       help=f"CSV column for {col} (default: {col}).")
    return p.parse_args()


def coerce_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def feature_col_map(args: argparse.Namespace) -> dict[str, str]:
    return {col: getattr(args, f"{col.replace('-', '_')}_col") for col in FEATURE_COLS}


def flush(conn, batch: list[dict]) -> int:
    if not batch:
        return 0
    with transaction(conn):
        conn.executemany(INSERT_SQL, batch)
    return len(batch)


def main() -> None:
    args = parse_args()
    if not args.csv.exists():
        sys.exit(f"CSV not found: {args.csv}")

    col_map = feature_col_map(args)

    with connect() as conn:
        if args.replace:
            with transaction(conn):
                conn.execute(
                    "DELETE FROM external_audio_features_raw WHERE source = ?",
                    (args.source,),
                )
            print(f"Deleted existing rows for source={args.source}")

        with args.csv.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            field_set = set(reader.fieldnames or [])
            required = [args.spotify_id_col, *col_map.values()]
            missing = [c for c in required if c not in field_set]
            if missing:
                sys.exit(f"Missing columns in CSV: {missing}\nAvailable: {reader.fieldnames}")
            if args.isrc_col and args.isrc_col not in field_set:
                sys.exit(f"Missing ISRC column '{args.isrc_col}'\nAvailable: {reader.fieldnames}")

            loaded = 0
            skipped = 0
            batch: list[dict] = []

            for row in reader:
                spotify_id = (row.get(args.spotify_id_col) or "").strip()
                if not spotify_id:
                    skipped += 1
                    continue
                isrc = None
                if args.isrc_col:
                    raw_isrc = (row.get(args.isrc_col) or "").strip()
                    isrc = raw_isrc or None
                payload = {
                    "source": args.source,
                    "spotify_id": spotify_id,
                    "isrc": isrc,
                }
                for col, src_col in col_map.items():
                    payload[col] = coerce_float(row.get(src_col))
                batch.append(payload)
                if len(batch) >= args.batch_size:
                    loaded += flush(conn, batch)
                    batch.clear()
                    print(f"  loaded {loaded:,} rows")

            loaded += flush(conn, batch)

    print(f"Done. source={args.source}: {loaded:,} rows loaded ({skipped} blank IDs skipped)")


if __name__ == "__main__":
    main()
