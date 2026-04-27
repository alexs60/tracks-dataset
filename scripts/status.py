from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect


load_repo_env(PROJECT_ROOT)


def scalar(conn, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def main() -> None:
    with connect() as conn:
        total = scalar(conn, "SELECT COUNT(*) FROM tracks")
        stage1_ok = scalar(conn, "SELECT COUNT(*) FROM tracks WHERE preview_status = 'ok'")
        stage1_no_preview = scalar(conn, "SELECT COUNT(*) FROM tracks WHERE preview_status = 'no_preview'")
        stage1_failed = scalar(conn, "SELECT COUNT(*) FROM tracks WHERE preview_status = 'failed'")
        stage1_pending = scalar(conn, "SELECT COUNT(*) FROM tracks WHERE preview_status IS NULL")

        stage2_ok = scalar(conn, "SELECT COUNT(*) FROM track_reccobeats WHERE status = 'ok'")
        stage2_not_found = scalar(conn, "SELECT COUNT(*) FROM track_reccobeats WHERE status = 'not_found'")
        stage2_no_features = scalar(conn, "SELECT COUNT(*) FROM track_reccobeats WHERE status = 'no_features'")
        stage2_failed = scalar(conn, "SELECT COUNT(*) FROM track_reccobeats WHERE status = 'failed'")
        stage2_pending = max(total - (stage2_ok + stage2_not_found + stage2_no_features + stage2_failed), 0)

        stage3_ok = scalar(conn, "SELECT COUNT(*) FROM track_analysis WHERE status = 'ok'")
        stage3_failed = scalar(conn, "SELECT COUNT(*) FROM track_analysis WHERE status = 'failed'")
        stage3_pending = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM tracks t
            LEFT JOIN track_analysis a ON a.track_id = t.track_id AND a.status = 'ok'
            WHERE t.preview_status = 'ok' AND a.track_id IS NULL
            """,
        )

        fully_enriched = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM tracks t
            JOIN track_reccobeats rb ON rb.track_id = t.track_id AND rb.status = 'ok'
            JOIN track_analysis a ON a.track_id = t.track_id AND a.status = 'ok'
            WHERE t.preview_status = 'ok'
            """,
        )

    pct = (fully_enriched / total * 100.0) if total else 0.0
    print(f"Tracks total:                  {total:,}")
    print(
        "Stage 1 (Spotify preview):     "
        f"{stage1_ok:,} ok | {stage1_no_preview:,} no_preview | "
        f"{stage1_failed:,} failed | {stage1_pending:,} pending"
    )
    print(
        "Stage 2 (Reccobeats):          "
        f"{stage2_ok:,} ok | {stage2_not_found:,} not_found | "
        f"{stage2_no_features:,} no_features | {stage2_failed:,} failed | "
        f"{stage2_pending:,} pending"
    )
    print(
        "Stage 3 (Essentia):            "
        f"{stage3_ok:,} ok | {stage3_failed:,} failed | {stage3_pending:,} pending"
    )
    print(f"Fully enriched:                {fully_enriched:,} ({pct:.1f}%)")


if __name__ == "__main__":
    main()