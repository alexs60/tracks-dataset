#!/usr/bin/env python3
"""
Probe the Reccobeats API with a handful of real track IDs from the DB
and print the raw response so we can verify the field mapping.

Usage:
  python3 scripts/probe_reccobeats.py          # random 5 tracks
  python3 scripts/probe_reccobeats.py 10        # random 10 tracks
  python3 scripts/probe_reccobeats.py --not-found  # test the not_found tracks specifically
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import connect
from workers.lib.env import load_repo_env
from workers.lib.reccobeats import get_base_url, _extract_spotify_id

load_repo_env(PROJECT_ROOT)

not_found_mode = "--not-found" in sys.argv
N = int(sys.argv[1]) if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else 5

with connect() as conn:
    if not_found_mode:
        rows = conn.execute(
            "SELECT t.track_id, t.title, t.artist FROM tracks t "
            "JOIN track_reccobeats rb ON rb.track_id = t.track_id "
            "WHERE rb.status = 'not_found' LIMIT 20"
        ).fetchall()
        print(f"Testing {len(rows)} not_found tracks")
    else:
        rows = conn.execute("SELECT track_id, title, artist FROM tracks LIMIT ?", (N,)).fetchall()

track_ids = [r[0] for r in rows]
print("Querying IDs:", track_ids)
print()

client = httpx.Client(timeout=30.0)
url = f"{get_base_url().rstrip('/')}/track"
response = client.get(url, params={"ids": ",".join(track_ids)})
print(f"GET {response.url}")
print(f"Status: {response.status_code}")
print()

payload = response.json()
items = payload.get("content") if isinstance(payload, dict) else payload
print(f"Items returned: {len(items) if items else 0} / {len(track_ids)} queried")
print()

# Show what _extract_spotify_id pulls from each item
print("ID mapping (extracted → matches queried?):")
extracted = {}
for item in (items or []):
    sid = _extract_spotify_id(item)
    match = "✓" if sid in track_ids else "✗ (MISMATCH)"
    print(f"  href={item.get('href')}  →  extracted={sid}  {match}")
    if sid:
        extracted[sid] = item

print()
missing = [t for t in track_ids if t not in extracted]
if missing:
    print(f"NOT returned by API ({len(missing)}):", missing)
else:
    print("All queried IDs returned by API")

if "--raw" in sys.argv:
    print()
    print("Raw response:")
    print(json.dumps(payload, indent=2))

