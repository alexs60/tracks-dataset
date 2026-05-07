from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect, transaction, utc_now_iso
from workers.lib.env import load_repo_env
from workers.lib.logging_utils import configure_json_logger, log_event
from workers.lib.rate_limit import SimpleRateLimiter
from workers.lib.spotify_embed import fetch_preview, get_embed_user_agent, maybe_dump_html


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
        SELECT track_id
        FROM tracks
        WHERE preview_status IS NULL OR preview_status = 'failed'
        ORDER BY preview_fetched IS NOT NULL, preview_fetched, track_id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()
    return [row[0] for row in rows]


def _process_track(track_id: str, limiter: SimpleRateLimiter, logger) -> None:
    """Fetch the Spotify embed preview for one track and persist the result.
    Opens its own DB connection and httpx client so it is safe to run from
    a thread pool. The shared `limiter` enforces the global qps cap across
    all workers."""
    started = time.monotonic()
    log_event(logger, ts=utc_now_iso(), stage=1, track_id=track_id, event="started")
    client = httpx.Client(headers={"User-Agent": get_embed_user_agent()}, timeout=30.0)
    try:
        with connect() as conn:
            limiter.wait()
            result = fetch_preview(client, track_id)
            fetched_at = utc_now_iso()
            dump_path = None
            if result.status in {"failed", "no_preview"}:
                dump_path = maybe_dump_html(track_id, result.html)

            with transaction(conn):
                conn.execute(
                    """
                    UPDATE tracks
                    SET preview_url = ?, preview_fetched = ?, preview_status = ?
                    WHERE track_id = ?
                    """,
                    (result.preview_url, fetched_at, result.status, track_id),
                )

        duration_ms = round((time.monotonic() - started) * 1000)
        payload = {
            "ts": fetched_at,
            "stage": 1,
            "track_id": track_id,
            "event": "ok" if result.status in {"ok", "no_preview"} else "failed",
            "duration_ms": duration_ms,
            "error": result.error,
        }
        if dump_path is not None:
            payload["html_dump"] = str(dump_path)
        log_event(logger, **payload)
    finally:
        client.close()


def process_once(batch_size: int, logger) -> int:
    with connect() as conn:
        batch = claim_batch(conn, batch_size)
    if not batch:
        return 0

    qps = float(os.environ.get("STAGE1_RATE_LIMIT_QPS", os.environ.get("WORKER_RATE_LIMIT_QPS", "1")))
    # Shared limiter enforces the global qps cap across all worker threads.
    limiter = SimpleRateLimiter(qps=qps)
    workers = max(1, int(os.environ.get("STAGE1_WORKERS", "1")))

    if workers == 1:
        for track_id in batch:
            _process_track(track_id, limiter, logger)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_process_track, tid, limiter, logger)
                       for tid in batch]
            for fut in futures:
                fut.result()
    return len(batch)


def main() -> None:
    args = parse_args()
    logger = configure_json_logger("stage1", Path("logs/stage1.log"))
    while True:
        processed = process_once(args.batch_size, logger)
        if not args.loop or processed == 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()