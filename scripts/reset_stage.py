from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect, transaction


load_repo_env(PROJECT_ROOT)


STAGE_CHOICES = ["1", "2", "2.1", "3"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=STAGE_CHOICES,
                        help="Stage to reset. '2.1' clears track_audio_features_external (the external CSV fallback).")
    parser.add_argument("--track-id")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if bool(args.track_id) == bool(args.all):
        parser.error("provide exactly one of --track-id or --all")
    return args


def where_clause(args: argparse.Namespace) -> tuple[str, tuple[object, ...]]:
    if args.all:
        return "", ()
    return " WHERE track_id = ?", (args.track_id,)


def reset_stage_1(conn, args: argparse.Namespace) -> None:
    clause, params = where_clause(args)
    conn.execute(
        f"UPDATE tracks SET preview_url = NULL, preview_fetched = NULL, preview_status = NULL{clause}",
        params,
    )


def reset_stage_2(conn, args: argparse.Namespace) -> None:
    clause, params = where_clause(args)
    conn.execute(f"DELETE FROM track_reccobeats{clause}", params)


def reset_stage_2_1(conn, args: argparse.Namespace) -> None:
    """Clear the external-CSV fallback fill. Leaves external_audio_features_raw intact
    so a re-fill (`fill_external_features.py`) can run without re-loading the CSV."""
    clause, params = where_clause(args)
    conn.execute(f"DELETE FROM track_audio_features_external{clause}", params)


def reset_stage_3(conn, args: argparse.Namespace) -> None:
    clause, params = where_clause(args)
    conn.execute(f"DELETE FROM track_high_level_class_probs{clause}", params)
    conn.execute(f"DELETE FROM track_high_level_categorical{clause}", params)
    conn.execute(f"DELETE FROM track_high_level_binary{clause}", params)
    conn.execute(f"DELETE FROM track_analysis{clause}", params)
    if not args.all:
        conn.execute("UPDATE tracks SET preview_status = 'failed' WHERE track_id = ? AND preview_status = 'ok'", params)


STAGE_HANDLERS = {
    "1": reset_stage_1,
    "2": reset_stage_2,
    "2.1": reset_stage_2_1,
    "3": reset_stage_3,
}


def main() -> None:
    args = parse_args()
    with connect() as conn:
        with transaction(conn):
            STAGE_HANDLERS[args.stage](conn, args)
    target = "all tracks" if args.all else args.track_id
    print(f"Reset stage {args.stage} for {target}")


if __name__ == "__main__":
    main()