from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect, transaction, utc_now_iso
from workers.lib.env import load_repo_env
from workers.lib.essentia_parse import parse_essentia_json
from workers.lib.logging_utils import configure_json_logger, log_event


load_repo_env(PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("WORKER_BATCH_SIZE", "50")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    return parser.parse_args()


def claim_batch(conn, batch_size: int) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT t.track_id, t.preview_url
        FROM tracks t
        LEFT JOIN track_analysis a ON a.track_id = t.track_id AND a.status = 'ok'
        WHERE t.preview_status = 'ok'
          AND t.preview_url IS NOT NULL
          AND a.track_id IS NULL
        ORDER BY t.preview_fetched, t.track_id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def extractor_command(mp3_path: Path, json_path: Path) -> list[str]:
    binary = os.environ.get("ESSENTIA_EXTRACTOR_BIN", "essentia_streaming_extractor_music")
    profile = os.environ.get("ESSENTIA_PROFILE")
    command = [binary, str(mp3_path), str(json_path)]
    if profile:
        command.append(profile)
    return command


def essentia_api_url() -> str | None:
    return os.environ.get("ESSENTIA_API_URL")


def analyze_with_remote_api(client: httpx.Client, mp3_path: Path) -> dict[str, Any]:
    api_url = essentia_api_url()
    if not api_url:
        raise RuntimeError("ESSENTIA_API_URL is not configured")

    field_name = os.environ.get("ESSENTIA_API_AUDIO_FIELD", "audio")
    with mp3_path.open("rb") as audio_file:
        response = client.post(
            api_url,
            files={field_name: (mp3_path.name, audio_file, "audio/mpeg")},
            timeout=float(os.environ.get("ESSENTIA_API_TIMEOUT_SEC", "300")),
        )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Essentia API returned a non-object JSON payload")
    return payload


def analyze_with_local_extractor(mp3_path: Path, json_path: Path) -> dict[str, Any]:
    command = extractor_command(mp3_path, json_path)
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "essentia failed")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Local Essentia extractor returned a non-object JSON payload")
    return payload


def mark_analysis_failure(conn, track_id: str, error: str, analyzed_at: str) -> None:
    with transaction(conn):
        conn.execute("DELETE FROM track_high_level_binary WHERE track_id = ?", (track_id,))
        conn.execute("DELETE FROM track_high_level_categorical WHERE track_id = ?", (track_id,))
        conn.execute("DELETE FROM track_high_level_class_probs WHERE track_id = ?", (track_id,))
        conn.execute(
            """
            INSERT INTO track_analysis (
                track_id, analyzed_at, extractor_version, models_version,
                status, error, raw_json, bpm, bpm_confidence, danceability_raw,
                loudness_ebu128, average_loudness, dynamic_complexity,
                key_key, key_scale, key_strength, chords_changes_rate,
                tuning_frequency, onset_rate, duration_sec
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            ON CONFLICT (track_id) DO UPDATE SET
                analyzed_at         = excluded.analyzed_at,
                extractor_version   = excluded.extractor_version,
                models_version      = excluded.models_version,
                status              = excluded.status,
                error               = excluded.error,
                raw_json            = NULL,
                bpm                 = NULL,
                bpm_confidence      = NULL,
                danceability_raw    = NULL,
                loudness_ebu128     = NULL,
                average_loudness    = NULL,
                dynamic_complexity  = NULL,
                key_key             = NULL,
                key_scale           = NULL,
                key_strength        = NULL,
                chords_changes_rate = NULL,
                tuning_frequency    = NULL,
                onset_rate          = NULL,
                duration_sec        = NULL
            """,
            (track_id, analyzed_at, "unknown", "unknown", "failed", error),
        )


def replace_analysis_rows(conn, parsed) -> None:
    with transaction(conn):
        conn.execute("DELETE FROM track_high_level_binary WHERE track_id = ?", (parsed.analysis_row["track_id"],))
        conn.execute("DELETE FROM track_high_level_categorical WHERE track_id = ?", (parsed.analysis_row["track_id"],))
        conn.execute("DELETE FROM track_high_level_class_probs WHERE track_id = ?", (parsed.analysis_row["track_id"],))
        conn.execute("DELETE FROM track_analysis WHERE track_id = ?", (parsed.analysis_row["track_id"],))
        conn.execute(
            """
            INSERT INTO track_analysis (
                track_id, analyzed_at, extractor_version, models_version,
                status, error, raw_json, bpm, bpm_confidence, danceability_raw,
                loudness_ebu128, average_loudness, dynamic_complexity,
                key_key, key_scale, key_strength, chords_changes_rate,
                tuning_frequency, onset_rate, duration_sec
            ) VALUES (
                :track_id, :analyzed_at, :extractor_version, :models_version,
                :status, :error, :raw_json, :bpm, :bpm_confidence, :danceability_raw,
                :loudness_ebu128, :average_loudness, :dynamic_complexity,
                :key_key, :key_scale, :key_strength, :chords_changes_rate,
                :tuning_frequency, :onset_rate, :duration_sec
            )
            """,
            parsed.analysis_row,
        )
        conn.executemany(
            """
            INSERT INTO track_high_level_binary (
                track_id, classifier, value, probability, prob_positive
            ) VALUES (
                :track_id, :classifier, :value, :probability, :prob_positive
            )
            """,
            parsed.binary_rows,
        )
        conn.executemany(
            """
            INSERT INTO track_high_level_categorical (
                track_id, classifier, value, probability
            ) VALUES (
                :track_id, :classifier, :value, :probability
            )
            """,
            parsed.categorical_rows,
        )
        conn.executemany(
            """
            INSERT INTO track_high_level_class_probs (
                track_id, classifier, class_label, probability
            ) VALUES (
                :track_id, :classifier, :class_label, :probability
            )
            """,
            parsed.categorical_prob_rows,
        )


def process_once(batch_size: int, logger) -> int:
    client = httpx.Client(timeout=60.0)
    try:
        with connect() as conn:
            batch = claim_batch(conn, batch_size)
            if not batch:
                return 0

            for track_id, preview_url in batch:
                started = time.monotonic()
                analyzed_at = utc_now_iso()
                log_event(logger, ts=analyzed_at, stage=3, track_id=track_id, event="started")
                try:
                    with tempfile.TemporaryDirectory(prefix=f"essentia_{track_id}_") as tmp_dir:
                        tmp_path = Path(tmp_dir)
                        mp3_path = tmp_path / f"{track_id}.mp3"
                        json_path = tmp_path / f"{track_id}.json"

                        response = client.get(preview_url, follow_redirects=True)
                        if response.status_code in {403, 410}:
                            with transaction(conn):
                                conn.execute(
                                    "UPDATE tracks SET preview_status = 'failed' WHERE track_id = ?",
                                    (track_id,),
                                )
                            raise RuntimeError(f"preview url expired with status {response.status_code}")
                        response.raise_for_status()
                        mp3_path.write_bytes(response.content)

                        if essentia_api_url():
                            payload = analyze_with_remote_api(client, mp3_path)
                        else:
                            payload = analyze_with_local_extractor(mp3_path, json_path)
                        parsed = parse_essentia_json(track_id, payload, analyzed_at)
                        replace_analysis_rows(conn, parsed)

                    log_event(logger, ts=utc_now_iso(), stage=3, track_id=track_id, event="ok", duration_ms=round((time.monotonic() - started) * 1000), error=None)
                except Exception as exc:
                    mark_analysis_failure(conn, track_id, str(exc), analyzed_at)
                    log_event(logger, ts=utc_now_iso(), stage=3, track_id=track_id, event="failed", duration_ms=round((time.monotonic() - started) * 1000), error=str(exc))
            return len(batch)
    finally:
        client.close()


def main() -> None:
    args = parse_args()
    logger = configure_json_logger("stage3", Path("logs/stage3.log"))
    while True:
        processed = process_once(args.batch_size, logger)
        if not args.loop or processed == 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()