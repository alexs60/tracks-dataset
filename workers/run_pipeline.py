from __future__ import annotations

import argparse
import os
import shutil
import signal
import sys
import tempfile
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect
from workers.lib.env import load_repo_env
from workers.lib.logging_utils import configure_json_logger, log_event

load_repo_env(PROJECT_ROOT)

import workers.stage1_spotify_previews as s1
import workers.stage2_reccobeats as s2
import workers.stage2_2_essentia_derived as s2_2
import workers.stage3_essentia as s3


_shutdown = threading.Event()


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all enrichment stages, looping forever. "
                    "Stage 1+2 run in the main thread; Stage 3 (and gated "
                    "Stage 2.2) run in a background thread so Essentia "
                    "analysis cannot throttle Spotify preview fetching. "
                    "Stage 2.2 runs only when Stage 2 (Reccobeats) has no "
                    "pending tracks left, leaving room for the operator-run "
                    "Stage 2.1 Kaggle fallback to land first. Optionally "
                    "runs the kworb scraper once at startup as Stage 0."
    )
    parser.add_argument("--batch-size", type=int,
                        default=int(os.environ.get("WORKER_BATCH_SIZE", "10")))
    parser.add_argument("--interval", type=int, default=15,
                        help="Seconds to sleep between passes (default: 15). "
                             "Per-stage qps caps already throttle the work, "
                             "so this only governs idle time when a stage "
                             "has nothing to do.")
    parser.add_argument(
        "--run-scraper", action="store_true",
        default=_env_truthy("PIPELINE_RUN_SCRAPER"),
        help="Run the kworb scraper once at startup as Stage 0, before the "
             "enrichment loop. Default off; can be enabled via the "
             "PIPELINE_RUN_SCRAPER env var.",
    )
    parser.add_argument(
        "--scraper-max-age-days", type=int,
        default=int(os.environ.get("SCRAPER_MAX_AGE_DAYS", "90")),
        help="Track-page cache window for Stage 0 (default: 90).",
    )
    parser.add_argument(
        "--scraper-force", action="store_true",
        default=_env_truthy("SCRAPER_FORCE"),
        help="Stage 0 ignores cache and re-scrapes every track.",
    )
    return parser.parse_args()


def cleanup_orphan_temp_dirs(pipeline_logger) -> None:
    """Remove any `essentia_*` temp dirs left in the system tempdir by
    crashed past runs. Stage 3 normally cleans these via TemporaryDirectory's
    context manager, but a SIGKILL or container kill leaves them behind."""
    tmp_root = Path(tempfile.gettempdir())
    removed = 0
    for entry in tmp_root.glob("essentia_*"):
        if not entry.is_dir():
            continue
        try:
            shutil.rmtree(entry, ignore_errors=True)
            removed += 1
        except Exception:
            pass
    if removed:
        log_event(pipeline_logger, event="cleanup_temp_dirs", removed=removed)


def stage2_has_pending() -> bool:
    """True iff at least one track still needs a Reccobeats decision.
    Used to gate Stage 2.2 — we only derive Spotify-style scalars from
    Essentia after every track has been tried against Reccobeats."""
    with connect() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM tracks t
            LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
            WHERE rb.track_id IS NULL
            LIMIT 1
            """
        ).fetchone()
        return row is not None


def run_stage0(pipeline_logger, *, max_age_days: int, force: bool) -> None:
    """Invoke the kworb scraper once. Errors are logged but never raised —
    the enrichment loop must still start even if discovery fails."""
    log_event(pipeline_logger, event="stage0_start",
              max_age_days=max_age_days, force=force)
    try:
        from scraper.kworb_scraper import DEFAULT_COUNTRIES, run as run_scraper
        countries_env = os.environ.get("SCRAPER_COUNTRIES", "").strip()
        countries = countries_env.split() if countries_env else DEFAULT_COUNTRIES
        summary = run_scraper(
            countries=countries,
            max_age_days=max_age_days,
            force=force,
        )
        log_event(pipeline_logger, event="stage0_done", **summary)
    except Exception as exc:
        log_event(pipeline_logger, event="stage0_error", error=str(exc))


def main_loop(args, logger1, logger2, pipeline_logger) -> None:
    """Stage 1 + Stage 2. Independent of Stage 3 throughput."""
    while not _shutdown.is_set():
        n1 = s1.process_once(args.batch_size, logger1)
        n2 = s2.process_once(args.batch_size, logger2)
        log_event(pipeline_logger, event="pass_main", n1=n1, n2=n2)
        if _shutdown.wait(args.interval):
            return


def stage3_loop(args, logger3, logger2_2, pipeline_logger) -> None:
    """Stage 3 (Essentia) + gated Stage 2.2 (Essentia-derived fallback).
    Runs in its own thread so Essentia analysis cannot block Stage 1+2.
    Stage 2.2 is skipped on every pass where Reccobeats still has work
    pending, so the Kaggle fallback (Stage 2.1) and Reccobeats both get
    their shot before we synthesise scalars from Essentia output."""
    try:
        while not _shutdown.is_set():
            n3 = s3.process_once(args.batch_size, logger3)
            n2_2 = 0
            gated = stage2_has_pending()
            if not gated:
                n2_2 = s2_2.process_once(args.batch_size, logger2_2)
            log_event(pipeline_logger, event="pass_bg",
                      n3=n3, n2_2=n2_2, stage2_2_gated=gated)
            if _shutdown.wait(args.interval):
                return
    except Exception as exc:
        # Surface the failure and trigger a clean shutdown so docker can
        # restart the whole pipeline rather than silently losing Stage 3.
        log_event(pipeline_logger, event="bg_thread_crashed", error=str(exc))
        _shutdown.set()
        raise


def main() -> None:
    args = parse_args()

    logger1 = configure_json_logger("stage1", Path("logs/stage1.log"))
    logger2 = configure_json_logger("stage2", Path("logs/stage2.log"))
    logger2_2 = configure_json_logger("stage2_2", Path("logs/stage2_2.log"))
    logger3 = configure_json_logger("stage3", Path("logs/stage3.log"))
    pipeline_logger = configure_json_logger("pipeline", Path("logs/pipeline.log"))

    log_event(pipeline_logger, event="startup",
              batch_size=args.batch_size, interval=args.interval,
              run_scraper=args.run_scraper)

    cleanup_orphan_temp_dirs(pipeline_logger)

    if args.run_scraper:
        run_stage0(pipeline_logger,
                   max_age_days=args.scraper_max_age_days,
                   force=args.scraper_force)

    def _on_signal(signum, _frame):
        log_event(pipeline_logger, event="shutdown_signal", signal=signum)
        _shutdown.set()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    bg = threading.Thread(
        target=stage3_loop,
        args=(args, logger3, logger2_2, pipeline_logger),
        name="stage3-bg",
        daemon=True,
    )
    bg.start()

    try:
        main_loop(args, logger1, logger2, pipeline_logger)
    finally:
        _shutdown.set()
        # Daemon thread will be killed on process exit; we still try to
        # join briefly so an in-flight track has a chance to commit.
        bg.join(timeout=300.0)
        log_event(pipeline_logger, event="shutdown_done")


if __name__ == "__main__":
    main()
