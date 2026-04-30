"""Stage 2.2: derive Spotify-style audio scalars from Essentia output.

Fills `track_audio_features_external` with `source='essentia_derived'` for
tracks where:
  - `track_analysis.status = 'ok'` (Stage 3 completed successfully)
  - Reccobeats did NOT return 'ok' (Stage 2 missed the track), and
  - no other external fallback row exists yet.

Derivation formulas (all values bounded to Spotify's 0..1 range where applicable):
  acousticness     = mood_acoustic.prob_positive
  danceability     = danceability.prob_positive            (Essentia binary classifier)
  energy           = mean( clip((loudness_ebu128 + 60) / 60, 0, 1),
                            1 - mood_relaxed.prob_positive )
  instrumentalness = 1 - voice_instrumental.prob_positive
  liveness         = NULL                                  (no clean Essentia proxy)
  loudness         = loudness_ebu128                       (dB, direct)
  speechiness      = NULL                                  (no clean Essentia proxy)
  tempo            = bpm                                   (BPM, direct)
  valence          = (mood_happy.prob_positive - mood_sad.prob_positive + 1) / 2

Idempotent: `ON CONFLICT (track_id) DO NOTHING` — never overwrites a Kaggle CSV
fill or any other prior external row. Reset with `reset_stage.py --stage 2.2`.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect, transaction, utc_now_iso
from workers.lib.env import load_repo_env
from workers.lib.logging_utils import configure_json_logger, log_event


load_repo_env(PROJECT_ROOT)


SOURCE = "essentia_derived"
MATCHED_BY = "essentia"


INSERT_SQL = """
INSERT INTO track_audio_features_external (
    track_id, source, matched_by,
    acousticness, danceability, energy,
    instrumentalness, liveness, loudness,
    speechiness, tempo, valence,
    fetched_at
)
SELECT
    a.track_id,
    ?, ?,
    p.p_acoustic                                                AS acousticness,
    p.p_danceable                                               AS danceability,
    (
        (CASE
            WHEN a.loudness_ebu128 IS NULL THEN NULL
            WHEN (a.loudness_ebu128 + 60.0) / 60.0 < 0 THEN 0.0
            WHEN (a.loudness_ebu128 + 60.0) / 60.0 > 1 THEN 1.0
            ELSE (a.loudness_ebu128 + 60.0) / 60.0
         END
         + (1.0 - p.p_relaxed)
        ) / 2.0
    )                                                           AS energy,
    (1.0 - p.p_voice)                                           AS instrumentalness,
    NULL                                                        AS liveness,
    a.loudness_ebu128                                           AS loudness,
    NULL                                                        AS speechiness,
    a.bpm                                                       AS tempo,
    ((p.p_happy - p.p_sad + 1.0) / 2.0)                         AS valence,
    ?                                                           AS fetched_at
FROM track_analysis a
LEFT JOIN track_reccobeats rb           ON rb.track_id  = a.track_id
LEFT JOIN track_audio_features_external ext ON ext.track_id = a.track_id
JOIN (
    SELECT
        track_id,
        MAX(CASE WHEN classifier='mood_acoustic'      THEN prob_positive END) AS p_acoustic,
        MAX(CASE WHEN classifier='mood_happy'         THEN prob_positive END) AS p_happy,
        MAX(CASE WHEN classifier='mood_sad'           THEN prob_positive END) AS p_sad,
        MAX(CASE WHEN classifier='mood_relaxed'       THEN prob_positive END) AS p_relaxed,
        MAX(CASE WHEN classifier='voice_instrumental' THEN prob_positive END) AS p_voice,
        MAX(CASE WHEN classifier='danceability'       THEN prob_positive END) AS p_danceable
    FROM track_high_level_binary
    GROUP BY track_id
) p ON p.track_id = a.track_id
WHERE a.status = 'ok'
  AND (rb.status IS NULL OR rb.status != 'ok')
  AND ext.track_id IS NULL
  AND p.p_acoustic  IS NOT NULL
  AND p.p_happy     IS NOT NULL
  AND p.p_sad       IS NOT NULL
  AND p.p_relaxed   IS NOT NULL
  AND p.p_voice     IS NOT NULL
  AND p.p_danceable IS NOT NULL
ORDER BY a.track_id
LIMIT ?
ON CONFLICT (track_id) DO NOTHING
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-size", type=int,
                        default=int(os.environ.get("WORKER_BATCH_SIZE", "50")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    return parser.parse_args()


def process_once(batch_size: int, logger) -> int:
    fetched_at = utc_now_iso()
    with connect() as conn:
        with transaction(conn):
            cur = conn.execute(
                INSERT_SQL,
                (SOURCE, MATCHED_BY, fetched_at, batch_size),
            )
            raw = cur.rowcount
            inserted = int(raw) if raw is not None and raw >= 0 else 0
    log_event(
        logger,
        ts=fetched_at,
        stage="2.2",
        event="batch",
        inserted=inserted,
        batch_size=batch_size,
    )
    return inserted


def main() -> None:
    args = parse_args()
    logger = configure_json_logger("stage2_2", Path("logs/stage2_2.log"))
    while True:
        processed = process_once(args.batch_size, logger)
        if not args.loop or processed == 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
