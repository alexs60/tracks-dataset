from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.env import load_repo_env
from workers.lib.db import connect


load_repo_env(PROJECT_ROOT)


def scalar(conn, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def _recent_5m_threshold() -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds")


def _recent(conn, sql: str, threshold: str) -> int:
    return int(conn.execute(sql, (threshold,)).fetchone()[0])


def _eta_suffix(pending: int, recent_5m: int) -> str:
    """Returns ' | rate X t/s | eta Y' (or ' | done' / ' | idle')."""
    if pending <= 0:
        return " | done"
    if recent_5m <= 0:
        return " | idle"
    rate = recent_5m / 300.0
    eta_h = pending / rate / 3600.0
    return f" | rate {rate:.2f} t/s | eta {eta_h:.1f}h"


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

        # Fallback universe = tracks where Reccobeats did NOT return 'ok' (so a
        # fallback is the only way these tracks get audio scalars). Of that
        # universe: 'filled' = has any external row, 'pending' = still missing.
        fallback_filled = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM track_audio_features_external ext
            LEFT JOIN track_reccobeats rb ON rb.track_id = ext.track_id
            WHERE rb.track_id IS NULL OR rb.status != 'ok'
            """,
        )
        fallback_universe = stage2_not_found + stage2_no_features + stage2_failed + stage2_pending
        fallback_pending = max(fallback_universe - fallback_filled, 0)
        source_rows = conn.execute(
            "SELECT source, COUNT(*) FROM track_audio_features_external GROUP BY source ORDER BY source"
        ).fetchall()
        fallback_sources = [(row[0], int(row[1])) for row in source_rows]

        # Stage 2.2 split: how many of the fallback-filled rows came from the
        # automatic Essentia derivation, and how many tracks are still waiting
        # on Stage 3 before Stage 2.2 can fill them.
        stage2_2_filled = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM track_audio_features_external ext
            LEFT JOIN track_reccobeats rb ON rb.track_id = ext.track_id
            WHERE ext.source = 'essentia_derived'
              AND (rb.status IS NULL OR rb.status != 'ok')
            """,
        )
        stage2_2_awaiting_stage3 = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM tracks t
            LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
            LEFT JOIN track_audio_features_external ext ON ext.track_id = t.track_id
            LEFT JOIN track_analysis a ON a.track_id = t.track_id AND a.status = 'ok'
            WHERE (rb.status IS NULL OR rb.status != 'ok')
              AND ext.track_id IS NULL
              AND a.track_id IS NULL
            """,
        )

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

        # End-to-end success: track has audio scalars (Reccobeats 'ok' OR an
        # external-fallback row) AND a successful Essentia analysis.
        fully_enriched = scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM tracks t
            JOIN track_analysis a ON a.track_id = t.track_id AND a.status = 'ok'
            WHERE t.preview_status = 'ok'
              AND (
                EXISTS (
                    SELECT 1 FROM track_reccobeats rb
                    WHERE rb.track_id = t.track_id AND rb.status = 'ok'
                )
                OR EXISTS (
                    SELECT 1 FROM track_audio_features_external ext
                    WHERE ext.track_id = t.track_id
                )
              )
            """,
        )

        threshold = _recent_5m_threshold()
        stage1_recent = _recent(
            conn,
            "SELECT COUNT(*) FROM tracks WHERE preview_fetched >= ?",
            threshold,
        )
        stage2_recent = _recent(
            conn,
            "SELECT COUNT(*) FROM track_reccobeats WHERE fetched_at >= ?",
            threshold,
        )
        fallback_recent = _recent(
            conn,
            "SELECT COUNT(*) FROM track_audio_features_external WHERE fetched_at >= ?",
            threshold,
        )
        stage2_2_recent = _recent(
            conn,
            "SELECT COUNT(*) FROM track_audio_features_external "
            "WHERE source = 'essentia_derived' AND fetched_at >= ?",
            threshold,
        )
        stage3_recent = _recent(
            conn,
            "SELECT COUNT(*) FROM track_analysis WHERE analyzed_at >= ?",
            threshold,
        )

        stage0 = stage0_summary(conn)
        per_country = per_country_breakdown(conn)

    pct = (fully_enriched / total * 100.0) if total else 0.0
    print(f"Tracks total:                  {total:,}")
    if stage0 is not None:
        latest_week_str = stage0["latest_week"] or "—"
        last_scraped_str = stage0["latest_scrape"] or "—"
        scraped_pct = (
            stage0["scraped"] / stage0["total"] * 100.0
            if stage0["total"] else 0.0
        )
        if stage0["rate_per_sec"] > 0:
            rate_str = f"{stage0['rate_per_sec']:.2f} t/s"
            eta_str = (
                f"{stage0['eta_hours']:.1f}h"
                if stage0["eta_hours"] is not None else "—"
            )
        else:
            rate_str = "idle"
            eta_str = "—"
        print(
            "Stage 0 (Scraper):             "
            f"{stage0['n_countries']:,} countries | "
            f"{stage0['scraped']:,}/{stage0['total']:,} scraped "
            f"({scraped_pct:.1f}%) | "
            f"rate {rate_str} (last 5m: {stage0['recent_5min']:,}) | "
            f"eta {eta_str}"
        )
        print(
            "                               "
            f"latest chart week: {latest_week_str} | "
            f"last track scrape: {last_scraped_str}"
        )
    print(
        "Stage 1 (Spotify preview):     "
        f"{stage1_ok:,} ok | {stage1_no_preview:,} no_preview | "
        f"{stage1_failed:,} failed | {stage1_pending:,} pending"
        f"{_eta_suffix(stage1_pending, stage1_recent)}"
    )
    print(
        "Stage 2 (Reccobeats):          "
        f"{stage2_ok:,} ok | {stage2_not_found:,} not_found | "
        f"{stage2_no_features:,} no_features | {stage2_failed:,} failed | "
        f"{stage2_pending:,} pending"
        f"{_eta_suffix(stage2_pending, stage2_recent)}"
    )
    sources_suffix = (
        f" ({', '.join(f'{src}={n:,}' for src, n in fallback_sources)})"
        if fallback_sources else ""
    )
    print(
        "Stage 2.1+2.2 (Fallback fill): "
        f"{fallback_filled:,} filled | {fallback_pending:,} pending"
        f"{_eta_suffix(fallback_pending, fallback_recent)}"
        f"{sources_suffix}"
    )
    print(
        "Stage 2.2 (Essentia derived):  "
        f"{stage2_2_filled:,} filled | {stage2_2_awaiting_stage3:,} awaiting Stage 3"
        f"{_eta_suffix(stage2_2_awaiting_stage3, stage2_2_recent)}"
    )
    print(
        "Stage 3 (Essentia):            "
        f"{stage3_ok:,} ok | {stage3_failed:,} failed | {stage3_pending:,} pending"
        f"{_eta_suffix(stage3_pending, stage3_recent)}"
    )
    print(f"Fully enriched:                {fully_enriched:,} ({pct:.1f}%)")

    if per_country:
        print()
        print("Per-country breakdown (from track_country_totals):")
        print(
            f"  {'country':<8} {'tracks':>8} {'stage1_ok':>10} "
            f"{'stage2_ok':>10} {'stage3_ok':>10}"
        )
        for row in per_country:
            cc, n_tracks, s1, s2, s3 = row
            print(
                f"  {cc:<8} {n_tracks:>8,} {s1:>10,} {s2:>10,} {s3:>10,}"
            )


def stage0_summary(conn) -> dict | None:
    """Returns a dict with Stage 0 progress fields, or None if
    track_country_totals doesn't exist yet. Timestamps are ISO 8601 strings
    written by the scraper. The 5-minute window approximates current scrape
    rate; rate_per_sec is 0.0 when the scraper is idle/finished."""
    try:
        n_countries = int(conn.execute(
            "SELECT COUNT(DISTINCT country) FROM track_country_totals"
        ).fetchone()[0])
    except Exception:
        return None
    latest_week = conn.execute(
        "SELECT MAX(week_date) FROM chart_entries"
    ).fetchone()[0]
    latest_scrape = conn.execute(
        "SELECT MAX(last_scraped) FROM tracks"
    ).fetchone()[0]
    total = int(conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0])
    scraped = int(conn.execute(
        "SELECT COUNT(*) FROM tracks WHERE last_scraped IS NOT NULL"
    ).fetchone()[0])
    pending = max(total - scraped, 0)

    window_minutes = 5
    threshold = (
        datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    ).isoformat(timespec="seconds")
    recent = int(conn.execute(
        "SELECT COUNT(*) FROM tracks WHERE last_scraped >= ?",
        (threshold,),
    ).fetchone()[0])
    rate_per_sec = recent / (window_minutes * 60.0) if recent > 0 else 0.0
    eta_hours = (pending / rate_per_sec / 3600.0) if rate_per_sec > 0 else None

    return {
        "n_countries": n_countries,
        "latest_week": str(latest_week) if latest_week else None,
        "latest_scrape": str(latest_scrape) if latest_scrape else None,
        "total": total,
        "scraped": scraped,
        "pending": pending,
        "recent_5min": recent,
        "rate_per_sec": rate_per_sec,
        "eta_hours": eta_hours,
    }


def per_country_breakdown(conn) -> list[tuple]:
    """Returns [(country, n_tracks, stage1_ok, stage2_ok, stage3_ok), ...]
    or [] if track_country_totals doesn't exist yet."""
    try:
        rows = conn.execute("""
            SELECT
                tct.country,
                COUNT(DISTINCT tct.track_id) AS n_tracks,
                COUNT(DISTINCT CASE WHEN t.preview_status = 'ok'
                                    THEN tct.track_id END) AS s1_ok,
                COUNT(DISTINCT CASE WHEN rb.status = 'ok'
                                    THEN tct.track_id END) AS s2_ok,
                COUNT(DISTINCT CASE WHEN a.status = 'ok'
                                    THEN tct.track_id END) AS s3_ok
            FROM track_country_totals tct
            JOIN tracks t                ON t.track_id = tct.track_id
            LEFT JOIN track_reccobeats rb ON rb.track_id = tct.track_id
            LEFT JOIN track_analysis a    ON a.track_id = tct.track_id
            GROUP BY tct.country
            ORDER BY tct.country
        """).fetchall()
    except Exception:
        return []
    return [
        (str(r[0]), int(r[1]), int(r[2] or 0), int(r[3] or 0), int(r[4] or 0))
        for r in rows
    ]


if __name__ == "__main__":
    main()