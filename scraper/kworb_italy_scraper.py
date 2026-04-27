"""
Kworb.net Italy Spotify Chart Scraper — v2

Fixes a parsing issue where the totals table wasn't being located
correctly. This version:
  - inspects ALL tables on the page and picks the one that contains
    the most track links (avoids matching layout/nav tables),
  - has a --debug mode that dumps the raw HTML and prints what each
    table on the page looks like,
  - has a --limit flag for smoke testing.

Usage:
    pip install httpx selectolax tenacity tqdm

    # First, sanity-check parsing on the totals page only:
    python kworb_italy_scraper.py --debug

    # Then a small smoke test:
    python kworb_italy_scraper.py --limit 5

    # Then the real run:
    python kworb_italy_scraper.py
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

BASE = "https://kworb.net"
TOTALS_URL = f"{BASE}/spotify/country/it_weekly_totals.html"
DB_PATH = Path("kworb_italy.db")
USER_AGENT = "kworb-italy-scraper/2.0 (personal research)"
REQUEST_DELAY = 1.0
FRESHNESS_DAYS = 90  # default ≈3 months; override with --max-age-days
DORMANT_DAYS = 60    # if a track hasn't charted in this long, skip even
                     # if its DB row is stale (its history won't change)

# Hrefs on it_weekly_totals.html are RELATIVE: '../track/{id}.html'.
# On other pages they may be absolute: '/spotify/track/{id}.html'.
# Match the trailing '/track/{id}.html' regardless of prefix.
TRACK_HREF_RE = re.compile(r"(?:^|/)track/([A-Za-z0-9]+)\.html")
ARTIST_HREF_RE = re.compile(r"(?:^|/)artist/([A-Za-z0-9]+)\.html")
DATE_RE = re.compile(r"^\d{4}/\d{2}/\d{2}$")
CELL_RE = re.compile(r"^\s*(\d+)\s*\(([\d,]+)\)\s*$")


# ---------- DB ----------

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tracks (
            track_id        TEXT PRIMARY KEY,
            title           TEXT NOT NULL,
            artist          TEXT NOT NULL,
            artist_id       TEXT,
            total_streams   INTEGER,
            weeks_on_it     INTEGER,
            peak_it         INTEGER,
            last_scraped    TEXT,
            last_chart_week TEXT  -- most recent week_date this track appeared
                                  -- on the IT chart (set after track scrape)
        );
        CREATE TABLE IF NOT EXISTS chart_entries (
            track_id   TEXT NOT NULL,
            week_date  TEXT NOT NULL,
            country    TEXT NOT NULL,
            position   INTEGER NOT NULL,
            streams    INTEGER,
            PRIMARY KEY (track_id, week_date, country)
        );
        CREATE INDEX IF NOT EXISTS idx_entries_week_country
            ON chart_entries(week_date, country, position);
        CREATE INDEX IF NOT EXISTS idx_entries_track
            ON chart_entries(track_id);
    """)
    # Backfill column for users upgrading from v2 schema.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tracks)")}
    if "last_chart_week" not in cols:
        conn.execute("ALTER TABLE tracks ADD COLUMN last_chart_week TEXT")
    conn.commit()


# ---------- HTTP ----------

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
def fetch(client: httpx.Client, url: str) -> str:
    r = client.get(url, timeout=30.0)
    r.raise_for_status()
    return r.text


# ---------- Parsing ----------

def find_data_table(tree: HTMLParser):
    """
    Pick the table containing the most track-history links. Layout/nav
    tables have zero, so this reliably finds the data table even if
    Kworb wraps it in other tables.
    """
    best, best_count = None, 0
    for table in tree.css("table"):
        count = sum(
            1 for a in table.css("a")
            if TRACK_HREF_RE.search(a.attributes.get("href", "") or "")
        )
        if count > best_count:
            best, best_count = table, count
    return best if best_count > 0 else None


def parse_totals(html: str, debug: bool = False) -> list[dict]:
    tree = HTMLParser(html)

    if debug:
        all_tables = tree.css("table")
        print(f"[debug] found {len(all_tables)} <table> elements", file=sys.stderr)
        for i, t in enumerate(all_tables):
            n_rows = len(t.css("tr"))
            n_links = sum(
                1 for a in t.css("a")
                if TRACK_HREF_RE.search(a.attributes.get("href", "") or "")
            )
            print(f"  table[{i}]: {n_rows} rows, {n_links} track links",
                  file=sys.stderr)

    table = find_data_table(tree)
    if table is None:
        if debug:
            print("[debug] no data table found", file=sys.stderr)
        return []

    rows: list[dict] = []
    seen: set[str] = set()

    for tr in table.css("tr"):
        cells = tr.css("td")
        if not cells:
            continue  # header row using <th>, or empty separator

        track_id = artist_id = None
        artist_name = track_title = None

        for a in cells[0].css("a"):
            href = a.attributes.get("href", "") or ""
            if (m := TRACK_HREF_RE.search(href)) and track_id is None:
                track_id = m.group(1)
                track_title = a.text(strip=True)
            elif (m := ARTIST_HREF_RE.search(href)) and artist_id is None:
                artist_id = m.group(1)
                artist_name = a.text(strip=True)

        if track_id is None or track_id in seen:
            continue
        seen.add(track_id)

        def cell_int(idx: int) -> int | None:
            if idx >= len(cells):
                return None
            txt = cells[idx].text(strip=True).replace(",", "")
            return int(txt) if txt.isdigit() else None

        # Column layout (from inspection of it_weekly_totals.html):
        # 0: Artist - Title | 1: Wks | 2: T10 | 3: Pk
        # 4: (x?) | 5: PkStreams | 6: Total
        rows.append({
            "track_id": track_id,
            "artist": artist_name or "",
            "artist_id": artist_id,
            "title": track_title or "",
            "weeks_on_it": cell_int(1),
            "peak_it": cell_int(3),
            "total_streams": cell_int(6),
        })

    if debug:
        print(f"[debug] parsed {len(rows)} track rows", file=sys.stderr)
        for r in rows[:3]:
            print(f"  sample: {r}", file=sys.stderr)
    return rows


def parse_track_page(html: str) -> list[dict]:
    """One row per (week_date, country) on the weekly table."""
    tree = HTMLParser(html)

    # Find the first table whose first row starts with "Date".
    weekly = None
    for t in tree.css("table"):
        first = t.css_first("tr")
        if first is None:
            continue
        headers = [c.text(strip=True) for c in first.css("td, th")]
        if headers and headers[0].lower() == "date":
            weekly = t
            break
    if weekly is None:
        return []

    headers = [c.text(strip=True) for c in weekly.css_first("tr").css("td, th")]
    countries = headers[1:]

    rows: list[dict] = []
    for tr in weekly.css("tr")[1:]:
        cells = [td.text(strip=True) for td in tr.css("td")]
        if len(cells) < 2 or not DATE_RE.match(cells[0]):
            continue  # skip Total/Peak summary rows
        week_date = datetime.strptime(cells[0], "%Y/%m/%d").date().isoformat()
        for country, cell in zip(countries, cells[1:]):
            if not cell or cell == "--":
                continue
            m = CELL_RE.match(cell)
            if not m:
                continue
            rows.append({
                "week_date": week_date,
                "country": country,
                "position": int(m.group(1)),
                "streams": int(m.group(2).replace(",", "")),
            })
    return rows


# ---------- Pipeline ----------

def upsert_tracks(conn: sqlite3.Connection, tracks: list[dict]) -> None:
    conn.executemany("""
        INSERT INTO tracks (track_id, title, artist, artist_id,
                            total_streams, weeks_on_it, peak_it, last_scraped)
        VALUES (:track_id, :title, :artist, :artist_id,
                :total_streams, :weeks_on_it, :peak_it, NULL)
        ON CONFLICT(track_id) DO UPDATE SET
            title=excluded.title, artist=excluded.artist,
            artist_id=excluded.artist_id,
            total_streams=excluded.total_streams,
            weeks_on_it=excluded.weeks_on_it,
            peak_it=excluded.peak_it
    """, tracks)
    conn.commit()


def needs_scrape(
    conn: sqlite3.Connection,
    track_id: str,
    *,
    max_age_days: int,
    force: bool = False,
) -> tuple[bool, str]:
    """
    Decide whether to scrape this track. Returns (should_scrape, reason).
    Skip rules, in order:
      1. Never scraped before  → scrape.
      2. --force flag          → scrape.
      3. Scraped recently      → skip ("fresh").
      4. Track is dormant      → skip ("dormant"): hasn't charted in
         IT for DORMANT_DAYS+, so its history is frozen.
      5. Otherwise             → scrape ("stale").
    """
    row = conn.execute(
        "SELECT last_scraped, last_chart_week FROM tracks WHERE track_id = ?",
        (track_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return True, "new"
    if force:
        return True, "forced"

    last_scraped = datetime.fromisoformat(row[0])
    age = datetime.utcnow() - last_scraped
    if age <= timedelta(days=max_age_days):
        return False, "fresh"

    last_chart_week = row[1]
    if last_chart_week:
        last_chart_dt = datetime.fromisoformat(last_chart_week)
        # If the track's most recent chart appearance is way in the past,
        # its weekly history is frozen — don't bother re-scraping.
        if (datetime.utcnow() - last_chart_dt) > timedelta(days=DORMANT_DAYS):
            return False, "dormant"

    return True, "stale"


def save_track_entries(conn: sqlite3.Connection, track_id: str, entries: list[dict]) -> None:
    conn.execute("DELETE FROM chart_entries WHERE track_id = ?", (track_id,))
    conn.executemany("""
        INSERT INTO chart_entries (track_id, week_date, country, position, streams)
        VALUES (?, ?, ?, ?, ?)
    """, [
        (track_id, e["week_date"], e["country"], e["position"], e["streams"])
        for e in entries
    ])
    # Capture the most recent IT chart appearance so future runs can mark
    # this track dormant once it stops charting.
    last_it_week = max(
        (e["week_date"] for e in entries if e["country"] == "IT"),
        default=None,
    )
    conn.execute(
        "UPDATE tracks SET last_scraped = ?, last_chart_week = ? WHERE track_id = ?",
        (datetime.utcnow().isoformat(), last_it_week, track_id),
    )
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--debug", action="store_true",
                    help="dump totals_debug.html and print table diagnostics, then exit")
    ap.add_argument("--limit", type=int, default=None,
                    help="only scrape the first N tracks (smoke test)")
    ap.add_argument("--max-age-days", type=int, default=FRESHNESS_DAYS,
                    help=f"skip tracks scraped within this many days "
                         f"(default: {FRESHNESS_DAYS})")
    ap.add_argument("--force", action="store_true",
                    help="re-scrape every track regardless of cache age")
    args = ap.parse_args()

    with httpx.Client(headers={"User-Agent": USER_AGENT},
                      follow_redirects=True) as client:
        print("Fetching totals page…")
        totals_html = fetch(client, TOTALS_URL)
        print(f"  received {len(totals_html):,} bytes")

        if args.debug:
            Path("totals_debug.html").write_text(totals_html, encoding="utf-8")
            print("  wrote totals_debug.html")

        tracks = parse_totals(totals_html, debug=args.debug)
        print(f"Found {len(tracks)} tracks in IT weekly totals.")

        if len(tracks) == 0:
            Path("totals_debug.html").write_text(totals_html, encoding="utf-8")
            print("  Parsing returned 0 — wrote totals_debug.html so we can")
            print("  inspect what the page actually looks like.")
            print("  Re-run with --debug for table-by-table diagnostics.")
            return

        if args.debug:
            return  # debug mode stops after parsing the index

        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        upsert_tracks(conn, tracks)

        # Decide which tracks need scraping; show a breakdown so it's
        # obvious what got skipped and why.
        from collections import Counter
        decisions = [
            (t, *needs_scrape(conn, t["track_id"],
                              max_age_days=args.max_age_days,
                              force=args.force))
            for t in tracks
        ]
        reason_counts = Counter(r for _, _, r in decisions)
        to_scrape = [t for t, should, _ in decisions if should]
        if args.limit is not None:
            to_scrape = to_scrape[: args.limit]

        print("Cache decision breakdown:")
        for reason in ("new", "stale", "forced", "fresh", "dormant"):
            n = reason_counts.get(reason, 0)
            if n:
                action = "scrape" if reason in ("new", "stale", "forced") else "skip"
                print(f"  {reason:8s} ({action}): {n:5d}")
        print(f"Will scrape {len(to_scrape)} tracks "
              f"(threshold: {args.max_age_days} days"
              f"{', forced' if args.force else ''}).")

        for t in tqdm(to_scrape, desc="tracks"):
            url = f"{BASE}/spotify/track/{t['track_id']}.html"
            try:
                html = fetch(client, url)
                entries = parse_track_page(html)
                save_track_entries(conn, t["track_id"], entries)
            except Exception as e:
                tqdm.write(f"  failed {t['track_id']}: {e}")
            time.sleep(REQUEST_DELAY)

        print("\nDone. Sanity check:")
        for label, q in [
            ("Total entries", "SELECT COUNT(*) FROM chart_entries"),
            ("Distinct weeks (IT)",
             "SELECT COUNT(DISTINCT week_date) FROM chart_entries WHERE country='IT'"),
            ("Date range (IT)",
             "SELECT MIN(week_date), MAX(week_date) FROM chart_entries WHERE country='IT'"),
        ]:
            print(f"  {label}: {conn.execute(q).fetchone()}")


if __name__ == "__main__":
    main()