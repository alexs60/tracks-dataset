"""
Fill track_audio_features_external from external_audio_features_raw.

Targets tracks where Reccobeats does NOT have usable audio features:
  - track_reccobeats.status IN ('not_found', 'no_features', 'failed')
  - or track_reccobeats row missing entirely

Match strategy (in order, each track filled at most once):
  1. Spotify track_id == external_audio_features_raw.spotify_id
  2. (with --by-isrc) track_reccobeats.isrc == external_audio_features_raw.isrc

Re-runnable. ON CONFLICT (track_id) DO UPDATE keeps the latest values.

Examples:
    python scripts/fill_external_features.py --source kaggle_maharshipandya
    python scripts/fill_external_features.py --source kaggle_other --by-isrc
    python scripts/fill_external_features.py --source kaggle_x --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect, transaction, utc_now_iso


load_repo_env(PROJECT_ROOT)


# Eligible = Reccobeats failed to provide features (any non-'ok' status, or row missing).
ELIGIBLE_PREDICATE = "(rb.status IS NULL OR rb.status != 'ok')"

INSERT_BY_ID_SQL = f"""
INSERT INTO track_audio_features_external (
    track_id, source, matched_by,
    acousticness, danceability, energy,
    instrumentalness, liveness, loudness,
    speechiness, tempo, valence,
    fetched_at
)
SELECT
    t.track_id, ?, 'spotify_id',
    e.acousticness, e.danceability, e.energy,
    e.instrumentalness, e.liveness, e.loudness,
    e.speechiness, e.tempo, e.valence,
    ?
FROM tracks t
LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
JOIN external_audio_features_raw e
    ON e.spotify_id = t.track_id AND e.source = ?
WHERE {ELIGIBLE_PREDICATE}
ON CONFLICT (track_id) DO UPDATE SET
    source           = excluded.source,
    matched_by       = excluded.matched_by,
    acousticness     = excluded.acousticness,
    danceability     = excluded.danceability,
    energy           = excluded.energy,
    instrumentalness = excluded.instrumentalness,
    liveness         = excluded.liveness,
    loudness         = excluded.loudness,
    speechiness      = excluded.speechiness,
    tempo            = excluded.tempo,
    valence          = excluded.valence,
    fetched_at       = excluded.fetched_at
"""

# ISRC fallback: only fills tracks not already filled by spotify_id, and only
# when Reccobeats returned an ISRC (status='no_features' or 'failed' with metadata).
INSERT_BY_ISRC_SQL = """
INSERT INTO track_audio_features_external (
    track_id, source, matched_by,
    acousticness, danceability, energy,
    instrumentalness, liveness, loudness,
    speechiness, tempo, valence,
    fetched_at
)
SELECT
    rb.track_id, ?, 'isrc',
    e.acousticness, e.danceability, e.energy,
    e.instrumentalness, e.liveness, e.loudness,
    e.speechiness, e.tempo, e.valence,
    ?
FROM track_reccobeats rb
JOIN external_audio_features_raw e
    ON e.isrc = rb.isrc AND e.source = ?
LEFT JOIN track_audio_features_external x
    ON x.track_id = rb.track_id
WHERE rb.status != 'ok'
  AND rb.isrc IS NOT NULL
  AND x.track_id IS NULL
ON CONFLICT (track_id) DO NOTHING
"""

COUNT_BY_ID_SQL = f"""
SELECT COUNT(*)
FROM tracks t
LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
JOIN external_audio_features_raw e
    ON e.spotify_id = t.track_id AND e.source = ?
WHERE {ELIGIBLE_PREDICATE}
"""

COUNT_BY_ISRC_SQL = """
SELECT COUNT(*)
FROM track_reccobeats rb
JOIN external_audio_features_raw e
    ON e.isrc = rb.isrc AND e.source = ?
LEFT JOIN track_audio_features_external x
    ON x.track_id = rb.track_id
WHERE rb.status != 'ok'
  AND rb.isrc IS NOT NULL
  AND x.track_id IS NULL
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--source", required=True,
                   help="Source identifier as loaded into external_audio_features_raw.")
    p.add_argument("--by-isrc", action="store_true",
                   help="Also try ISRC matching for any track not yet filled by spotify_id.")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't write — only report match counts.")
    return p.parse_args()


def scalar(conn, sql: str, params: tuple) -> int:
    return int(conn.execute(sql, params).fetchone()[0])


def main() -> None:
    args = parse_args()
    fetched_at = utc_now_iso()

    with connect() as conn:
        n_id = scalar(conn, COUNT_BY_ID_SQL, (args.source,))
        print(f"Tracks matchable by spotify_id: {n_id:,}")

        if args.dry_run:
            if args.by_isrc:
                # ISRC count after spotify_id fill is what matters in real runs;
                # in dry-run we report it as "additional if id-fill happened first"
                # which is the same SQL — good enough for sizing.
                n_isrc = scalar(conn, COUNT_BY_ISRC_SQL, (args.source,))
                print(f"Tracks matchable by ISRC (additional, before any fill): {n_isrc:,}")
            print("Dry-run: no rows written.")
            return

        with transaction(conn):
            conn.execute(INSERT_BY_ID_SQL, (args.source, fetched_at, args.source))
        print(f"Filled by spotify_id: ~{n_id:,} rows (re-runs may upsert)")

        if args.by_isrc:
            n_isrc_remaining = scalar(conn, COUNT_BY_ISRC_SQL, (args.source,))
            print(f"Tracks remaining matchable by ISRC: {n_isrc_remaining:,}")
            with transaction(conn):
                conn.execute(INSERT_BY_ISRC_SQL, (args.source, fetched_at, args.source))
            print(f"Filled by ISRC: ~{n_isrc_remaining:,} rows")

        total = scalar(
            conn,
            "SELECT COUNT(*) FROM track_audio_features_external WHERE source = ?",
            (args.source,),
        )
        print(f"track_audio_features_external rows for source={args.source}: {total:,}")


if __name__ == "__main__":
    main()
