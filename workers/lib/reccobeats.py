from __future__ import annotations

import os
import re
from dataclasses import dataclass

import httpx


@dataclass
class ReccobeatsTrack:
    spotify_track_id: str
    reccobeats_id: str | None
    isrc: str | None
    ean: str | None
    upc: str | None
    duration_ms: int | None
    available_countries: str | None


def get_base_url() -> str:
    return os.environ.get("RECCOBEATS_BASE_URL", "https://api.reccobeats.com/v1")


SPOTIFY_ID_RE = re.compile(r"(?:track[:/])?([0-9A-Za-z]{22})")


def _extract_spotify_id(item: dict[str, object]) -> str | None:
    candidates = [
        item.get("spotifyId"),
        item.get("spotifyTrackId"),
        item.get("idSpotify"),
        item.get("href"),
        item.get("uri"),
        item.get("spotifyHref"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            candidate = candidate.get("id")
        if not isinstance(candidate, str) or not candidate:
            continue
        match = SPOTIFY_ID_RE.search(candidate)
        if match:
            return match.group(1)
        if len(candidate) == 22 and candidate.isalnum():
            return candidate
    return None


def fetch_tracks(client: httpx.Client, spotify_ids: list[str]) -> dict[str, ReccobeatsTrack]:
    if not spotify_ids:
        return {}
    response = client.get(
        f"{get_base_url().rstrip('/')}/track",
        params={"ids": ",".join(spotify_ids)},
    )
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict):
        items = payload.get("content") or payload.get("data") or payload.get("tracks") or payload.get("results") or []
        if isinstance(items, dict):
            items = list(items.values())
    else:
        items = payload

    results: dict[str, ReccobeatsTrack] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        spotify_id = _extract_spotify_id(item)
        if not spotify_id:
            continue
        available = item.get("availableCountries")
        if isinstance(available, list):
            available_countries = ",".join(available)
        else:
            available_countries = available
        results[spotify_id] = ReccobeatsTrack(
            spotify_track_id=spotify_id,
            reccobeats_id=item.get("id"),
            isrc=item.get("isrc"),
            ean=item.get("ean"),
            upc=item.get("upc"),
            duration_ms=item.get("durationMs"),
            available_countries=available_countries,
        )
    return results


def fetch_audio_features(client: httpx.Client, reccobeats_id: str) -> dict[str, float | None]:
    response = client.get(f"{get_base_url().rstrip('/')}/track/{reccobeats_id}/audio-features")
    if response.status_code == 404:
        raise FileNotFoundError(reccobeats_id)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "content" in payload and isinstance(payload["content"], dict):
        payload = payload["content"]
    return {
        "acousticness": payload.get("acousticness"),
        "danceability": payload.get("danceability"),
        "energy": payload.get("energy"),
        "instrumentalness": payload.get("instrumentalness"),
        "liveness": payload.get("liveness"),
        "loudness": payload.get("loudness"),
        "speechiness": payload.get("speechiness"),
        "tempo": payload.get("tempo"),
        "valence": payload.get("valence"),
    }