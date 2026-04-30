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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all three enrichment stages in sequence, looping forever.")
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("WORKER_BATCH_SIZE", "10")))
    parser.add_argument("--interval", type=int, default=60, help="Seconds to sleep between passes (default: 60)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger1 = configure_json_logger("stage1", Path("logs/stage1.log"))
    logger2 = configure_json_logger("stage2", Path("logs/stage2.log"))
    logger2_2 = configure_json_logger("stage2_2", Path("logs/stage2_2.log"))
    logger3 = configure_json_logger("stage3", Path("logs/stage3.log"))
    pipeline_logger = configure_json_logger("pipeline", Path("logs/pipeline.log"))

    log_event(pipeline_logger, event="startup", batch_size=args.batch_size, interval=args.interval)

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
