from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import httpx


AUDIO_PREVIEW_RE = re.compile(
    r'"audioPreview"\s*:\s*\{\s*"url"\s*:\s*"([^"]+)"\s*\}',
    re.IGNORECASE,
)


@dataclass
class PreviewResult:
    status: str
    preview_url: str | None
    html: str | None = None
    error: str | None = None


def get_embed_base_url() -> str:
    return os.environ.get("SPOTIFY_EMBED_BASE", "https://open.spotify.com/embed/track")


def get_embed_user_agent() -> str:
    return os.environ.get(
        "SPOTIFY_EMBED_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )


def build_embed_url(track_id: str) -> str:
    return f"{get_embed_base_url().rstrip('/')}/{track_id}"


def fetch_preview(client: httpx.Client, track_id: str) -> PreviewResult:
    url = build_embed_url(track_id)
    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return PreviewResult(status="failed", preview_url=None, error=str(exc))

    match = AUDIO_PREVIEW_RE.search(response.text)
    if not match:
        return PreviewResult(status="no_preview", preview_url=None, html=response.text)

    preview_url = match.group(1)
    if not preview_url.startswith("https://"):
        return PreviewResult(status="failed", preview_url=None, html=response.text)
    return PreviewResult(status="ok", preview_url=preview_url)


def maybe_dump_html(track_id: str, html: str | None) -> Path | None:
    dump_dir = os.environ.get("SPOTIFY_EMBED_SAMPLE_HTML_DIR")
    if not dump_dir or not html:
        return None
    target = Path(dump_dir)
    target.mkdir(parents=True, exist_ok=True)
    dump_path = target / f"embed_{track_id}.html"
    dump_path.write_text(html, encoding="utf-8")
    return dump_path