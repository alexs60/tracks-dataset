from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.logging_utils import configure_json_logger, log_event

load_repo_env(PROJECT_ROOT)

import workers.stage1_spotify_previews as s1
import workers.stage2_reccobeats as s2
import workers.stage2_2_essentia_derived as s2_2
import workers.stage3_essentia as s3


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all enrichment stages in sequence, looping forever. "
                    "Optionally runs the kworb scraper once at startup as Stage 0."
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

    if args.run_scraper:
        run_stage0(pipeline_logger,
                   max_age_days=args.scraper_max_age_days,
                   force=args.scraper_force)

    while True:
        n1 = s1.process_once(args.batch_size, logger1)
        n2 = s2.process_once(args.batch_size, logger2)
        n3 = s3.process_once(args.batch_size, logger3)
        # Stage 2.2 must run after Stage 3: it derives Spotify-style scalars
        # from the Essentia output for tracks Reccobeats missed.
        n2_2 = s2_2.process_once(args.batch_size, logger2_2)
        log_event(pipeline_logger, event="pass", n1=n1, n2=n2, n3=n3, n2_2=n2_2)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
