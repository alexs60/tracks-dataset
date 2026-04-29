from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect, transaction, utc_now_iso
from workers.lib.env import load_repo_env
from workers.lib.logging_utils import configure_json_logger, log_event
from workers.lib.rate_limit import SimpleRateLimiter
from workers.lib.reccobeats import fetch_audio_features, fetch_tracks


load_repo_env(PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("WORKER_BATCH_SIZE", "50")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=3600)
    return parser.parse_args()


def claim_batch(conn, batch_size: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT t.track_id
        FROM tracks t
        LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
        WHERE rb.track_id IS NULL
        ORDER BY t.track_id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()
    return [row[0] for row in rows]


def upsert_row(conn, payload: dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO track_reccobeats (
            track_id, reccobeats_id, isrc, ean, upc, duration_ms,
            acousticness, danceability, energy,
            instrumentalness, liveness, loudness, speechiness, tempo,
            valence, fetched_at, status
        ) VALUES (
            :track_id, :reccobeats_id, :isrc, :ean, :upc, :duration_ms,
            :acousticness, :danceability, :energy,
            :instrumentalness, :liveness, :loudness, :speechiness, :tempo,
            :valence, :fetched_at, :status
        )
        ON CONFLICT (track_id) DO UPDATE SET
            reccobeats_id       = excluded.reccobeats_id,
            isrc                = excluded.isrc,
            ean                 = excluded.ean,
            upc                 = excluded.upc,
            duration_ms         = excluded.duration_ms,
            acousticness        = excluded.acousticness,
            danceability        = excluded.danceability,
            energy              = excluded.energy,
            instrumentalness    = excluded.instrumentalness,
            liveness            = excluded.liveness,
            loudness            = excluded.loudness,
            speechiness         = excluded.speechiness,
            tempo               = excluded.tempo,
            valence             = excluded.valence,
            fetched_at          = excluded.fetched_at,
            status              = excluded.status
        """,
        payload,
    )


def process_once(batch_size: int, logger) -> int:
    qps = float(os.environ.get("STAGE2_RATE_LIMIT_QPS", os.environ.get("WORKER_RATE_LIMIT_QPS", "1")))
    limiter = SimpleRateLimiter(qps=qps)
    client = httpx.Client(timeout=30.0)
    try:
        with connect() as conn:
            batch = claim_batch(conn, batch_size)
            if not batch:
                return 0

            limiter.wait()
            try:
                lookup = fetch_tracks(client, batch)
            except Exception as exc:
                fetched_at = utc_now_iso()
                for track_id in batch:
                    with transaction(conn):
                        upsert_row(conn, {
                            "track_id": track_id,
                            "reccobeats_id": None,
                            "isrc": None,
                            "ean": None,
                            "upc": None,
                            "duration_ms": None,
                            "acousticness": None,
                            "danceability": None,
                            "energy": None,
                            "instrumentalness": None,
                            "liveness": None,
                            "loudness": None,
                            "speechiness": None,
                            "tempo": None,
                            "valence": None,
                            "fetched_at": fetched_at,
                            "status": "failed",
                        })
                    log_event(logger, ts=fetched_at, stage=2, track_id=track_id, event="failed", duration_ms=0, error=str(exc))
                return len(batch)
            fetched_at = utc_now_iso()
            for track_id in batch:
                started = time.monotonic()
                log_event(logger, ts=fetched_at, stage=2, track_id=track_id, event="started")
                metadata = lookup.get(track_id)
                if metadata is None:
                    with transaction(conn):
                        upsert_row(conn, {
                            "track_id": track_id,
                            "reccobeats_id": None,
                            "isrc": None,
                            "ean": None,
                            "upc": None,
                            "duration_ms": None,
                            "acousticness": None,
                            "danceability": None,
                            "energy": None,
                            "instrumentalness": None,
                            "liveness": None,
                            "loudness": None,
                            "speechiness": None,
                            "tempo": None,
                            "valence": None,
                            "fetched_at": fetched_at,
                            "status": "not_found",
                        })
                    log_event(logger, ts=utc_now_iso(), stage=2, track_id=track_id, event="not_found", duration_ms=round((time.monotonic() - started) * 1000), error=None)
                    continue

                features: dict[str, object]
                status = "ok"
                try:
                    if metadata.reccobeats_id is None:
                        raise FileNotFoundError(track_id)
                    limiter.wait()
                    features = fetch_audio_features(client, metadata.reccobeats_id)
                except FileNotFoundError:
                    features = {
                        "acousticness": None,
                        "danceability": None,
                        "energy": None,
                        "instrumentalness": None,
                        "liveness": None,
                        "loudness": None,
                        "speechiness": None,
                        "tempo": None,
                        "valence": None,
                    }
                    status = "no_features"
                except Exception as exc:
                    features = {
                        "acousticness": None,
                        "danceability": None,
                        "energy": None,
                        "instrumentalness": None,
                        "liveness": None,
                        "loudness": None,
                        "speechiness": None,
                        "tempo": None,
                        "valence": None,
                    }
                    status = "failed"
                    with transaction(conn):
                        upsert_row(conn, {
                            "track_id": track_id,
                            "reccobeats_id": metadata.reccobeats_id,
                            "isrc": metadata.isrc,
                            "ean": metadata.ean,
                            "upc": metadata.upc,
                            "duration_ms": metadata.duration_ms,
                            **features,
                            "fetched_at": fetched_at,
                            "status": status,
                        })
                    log_event(logger, ts=utc_now_iso(), stage=2, track_id=track_id, event="failed", duration_ms=round((time.monotonic() - started) * 1000), error=str(exc))
                    continue

                with transaction(conn):
                    upsert_row(conn, {
                        "track_id": track_id,
                        "reccobeats_id": metadata.reccobeats_id,
                        "isrc": metadata.isrc,
                        "ean": metadata.ean,
                        "upc": metadata.upc,
                        "duration_ms": metadata.duration_ms,
                        **features,
                        "fetched_at": fetched_at,
                        "status": status,
                    })
                log_event(logger, ts=utc_now_iso(), stage=2, track_id=track_id, event="ok", duration_ms=round((time.monotonic() - started) * 1000), error=None)
            return len(batch)
    finally:
        client.close()


def main() -> None:
    args = parse_args()
    logger = configure_json_logger("stage2", Path("logs/stage2.log"))
    while True:
        processed = process_once(args.batch_size, logger)
        if not args.loop or processed == 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()