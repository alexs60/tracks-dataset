"""
Kworb.net Spotify Chart Scraper — multi-country.

Discovers tracks from one or more country `_weekly_totals.html` pages, then
scrapes each track's history page (which contains weekly chart positions for
*all* countries kworb tracks). Writes to either SQLite (DB_PATH) or
PostgreSQL (DATABASE_URL) via workers.lib.db.connect().

Tables touched:
  - tracks                 (core: track_id, title, artist, artist_id, last_scraped)
                           Legacy IT-specific cols (weeks_on_it, peak_it,
                           last_chart_week, total_streams) are still updated
                           when scraping IT, for backward compat with existing
                           exports.
  - chart_entries          (per-week, per-country position+streams)
  - track_country_totals   (per-country summary: weeks_on, peak, total_streams,
                            last_chart_week, last_seen)

Usage:
    pip install -r requirements.txt   # httpx selectolax tenacity tqdm psycopg2-binary

    # Smoke test on IT only
    python scraper/kworb_scraper.py --country IT --limit 5 --debug

    # Full scrape across the default country set
    python scraper/kworb_scraper.py

    # Specific countries
    python scraper/kworb_scraper.py --country IT GB FR DE ES US PT NL
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.db import DbAdapter, connect
from workers.lib.env import load_repo_env

BASE = "https://kworb.net"
USER_AGENT = "kworb-scraper/3.0 (personal research)"
REQUEST_DELAY = 1.0
FRESHNESS_DAYS = 90
DORMANT_DAYS = 60

DEFAULT_COUNTRIES = ["IT", "ES", "FR", "DE", "GB", "US", "PT", "NL"]

TRACK_HREF_RE = re.compile(r"(?:^|/)track/([A-Za-z0-9]+)\.html")
ARTIST_HREF_RE = re.compile(r"(?:^|/)artist/([A-Za-z0-9]+)\.html")
DATE_RE = re.compile(r"^\d{4}/\d{2}/\d{2}$")
CELL_RE = re.compile(r"^\s*(\d+)\s*\(([\d,]+)\)\s*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- DB bootstrap ----------

def init_db(conn: DbAdapter) -> None:
    """Idempotent. Creates the chart-data tables if missing."""
    streams_type = "BIGINT" if conn.backend == "postgres" else "INTEGER"

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
            track_id        TEXT PRIMARY KEY,
            title           TEXT NOT NULL,
            artist          TEXT NOT NULL,
            artist_id       TEXT,
            total_streams   INTEGER,
            weeks_on_it     INTEGER,
            peak_it         INTEGER,
            last_scraped    TEXT,
            last_chart_week TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chart_entries (
            track_id   TEXT NOT NULL,
            week_date  TEXT NOT NULL,
            country    TEXT NOT NULL,
            position   INTEGER NOT NULL,
            streams    INTEGER,
            PRIMARY KEY (track_id, week_date, country)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_week_country "
        "ON chart_entries(week_date, country, position)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_track ON chart_entries(track_id)"
    )

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS track_country_totals (
            track_id        TEXT NOT NULL REFERENCES tracks(track_id),
            country         TEXT NOT NULL,
            weeks_on        INTEGER,
            peak            INTEGER,
            total_streams   {streams_type},
            last_chart_week TEXT,
            last_seen       TEXT NOT NULL,
            PRIMARY KEY (track_id, country)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tct_country ON track_country_totals(country)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tct_last_chart "
        "ON track_country_totals(last_chart_week)"
    )

    if conn.backend == "sqlite":
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
    best, best_count = None, 0
    for table in tree.css("table"):
        count = sum(
            1 for a in table.css("a")
            if TRACK_HREF_RE.search(a.attributes.get("href", "") or "")
        )
        if count > best_count:
            best, best_count = table, count
    return best if best_count > 0 else None


def parse_totals(html: str, *, debug: bool = False) -> list[dict]:
    """Parse a `{cc}_weekly_totals.html` page → per-track summary rows."""
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
        return []

    rows: list[dict] = []
    seen: set[str] = set()

    for tr in table.css("tr"):
        cells = tr.css("td")
        if not cells:
            continue

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

        # Column layout (from inspection of {cc}_weekly_totals.html):
        # 0: Artist - Title | 1: Wks | 2: T10 | 3: Pk
        # 4: (x?) | 5: PkStreams | 6: Total
        rows.append({
            "track_id": track_id,
            "artist": artist_name or "",
            "artist_id": artist_id,
            "title": track_title or "",
            "weeks_on": cell_int(1),
            "peak": cell_int(3),
            "total_streams": cell_int(6),
        })

    return rows


def parse_track_page(html: str) -> list[dict]:
    """One row per (week_date, country) on the weekly cross-country table."""
    tree = HTMLParser(html)
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
            continue
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


# ---------- Persistence ----------

def upsert_tracks_core(conn: DbAdapter, rows: list[dict]) -> None:
    """Upsert track identification only (title/artist/artist_id)."""
    if not rows:
        return
    conn.executemany("""
        INSERT INTO tracks (track_id, title, artist, artist_id)
        VALUES (:track_id, :title, :artist, :artist_id)
        ON CONFLICT (track_id) DO UPDATE SET
            title = excluded.title,
            artist = excluded.artist,
            artist_id = excluded.artist_id
    """, rows)
    conn.commit()


def update_tracks_legacy_it(conn: DbAdapter, it_rows: list[dict]) -> None:
    """Backward-compat: update IT-specific summary cols on `tracks` for IT rows."""
    if not it_rows:
        return
    conn.executemany("""
        UPDATE tracks SET
            total_streams = :total_streams,
            weeks_on_it = :weeks_on,
            peak_it = :peak
        WHERE track_id = :track_id
    """, it_rows)
    conn.commit()


def upsert_country_totals(conn: DbAdapter, rows: list[dict]) -> None:
    """Per-country summary upsert. Preserves last_chart_week (set later)."""
    if not rows:
        return
    conn.executemany("""
        INSERT INTO track_country_totals
            (track_id, country, weeks_on, peak, total_streams, last_seen)
        VALUES (:track_id, :country, :weeks_on, :peak, :total_streams, :last_seen)
        ON CONFLICT (track_id, country) DO UPDATE SET
            weeks_on = excluded.weeks_on,
            peak = excluded.peak,
            total_streams = excluded.total_streams,
            last_seen = excluded.last_seen
    """, rows)
    conn.commit()


def needs_scrape(
    conn: DbAdapter,
    track_id: str,
    *,
    max_age_days: int,
    force: bool = False,
) -> tuple[bool, str]:
    row = conn.execute(
        "SELECT last_scraped FROM tracks WHERE track_id = ?",
        (track_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return True, "new"
    if force:
        return True, "forced"

    last_scraped = datetime.fromisoformat(row[0])
    if last_scraped.tzinfo is not None:
        last_scraped = last_scraped.replace(tzinfo=None)
    age = datetime.utcnow() - last_scraped
    if age <= timedelta(days=max_age_days):
        return False, "fresh"

    max_row = conn.execute(
        "SELECT MAX(last_chart_week) FROM track_country_totals WHERE track_id = ?",
        (track_id,),
    ).fetchone()
    max_week = max_row[0] if max_row else None
    if max_week:
        last_chart_dt = datetime.fromisoformat(max_week)
        if (datetime.utcnow() - last_chart_dt) > timedelta(days=DORMANT_DAYS):
            return False, "dormant"
    return True, "stale"


def save_track_entries(conn: DbAdapter, track_id: str, entries: list[dict]) -> None:
    """Replace this track's chart_entries and refresh per-country last_chart_week."""
    conn.execute("DELETE FROM chart_entries WHERE track_id = ?", (track_id,))
    if entries:
        conn.executemany("""
            INSERT INTO chart_entries (track_id, week_date, country, position, streams)
            VALUES (?, ?, ?, ?, ?)
        """, [
            (track_id, e["week_date"], e["country"], e["position"], e["streams"])
            for e in entries
        ])

    by_country: dict[str, str] = {}
    for e in entries:
        cur = by_country.get(e["country"])
        if cur is None or e["week_date"] > cur:
            by_country[e["country"]] = e["week_date"]

    if by_country:
        # Only touch rows we've already created via a totals scrape; tracks may
        # also chart in countries we don't track explicitly (e.g. MX showing up
        # on a US track page) — those entries still land in chart_entries but
        # we don't manufacture a totals row from track-page data alone.
        conn.executemany("""
            UPDATE track_country_totals
            SET last_chart_week = ?
            WHERE track_id = ? AND country = ?
        """, [
            (week, track_id, country)
            for country, week in by_country.items()
        ])

    last_it_week = by_country.get("IT")
    conn.execute(
        "UPDATE tracks SET last_scraped = ?, last_chart_week = ? WHERE track_id = ?",
        (utc_now_iso(), last_it_week, track_id),
    )
    conn.commit()


# ---------- Pipeline ----------

def discover_country(client: httpx.Client, country: str, *, debug: bool) -> list[dict]:
    url = f"{BASE}/spotify/country/{country.lower()}_weekly_totals.html"
    print(f"  fetching {country}: {url}")
    html = fetch(client, url)
    if debug:
        Path(f"totals_{country.lower()}_debug.html").write_text(html, encoding="utf-8")
    rows = parse_totals(html, debug=debug)
    print(f"    parsed {len(rows)} tracks for {country}")
    return rows


def main() -> None:
    load_repo_env(PROJECT_ROOT)

    ap = argparse.ArgumentParser(
        description="Multi-country kworb.net Spotify chart scraper."
    )
    ap.add_argument(
        "--country", nargs="+", default=DEFAULT_COUNTRIES,
        metavar="CC",
        help=f"ISO-2 country codes to scrape (default: {' '.join(DEFAULT_COUNTRIES)})",
    )
    ap.add_argument("--debug", action="store_true",
                    help="dump totals_*_debug.html for each country and print "
                         "table diagnostics, then exit before track scraping")
    ap.add_argument("--limit", type=int, default=None,
                    help="only scrape the first N candidate tracks (smoke test)")
    ap.add_argument("--max-age-days", type=int, default=FRESHNESS_DAYS,
                    help=f"skip tracks scraped within this many days "
                         f"(default: {FRESHNESS_DAYS})")
    ap.add_argument("--force", action="store_true",
                    help="re-scrape every track regardless of cache age")
    args = ap.parse_args()

    countries = [c.upper() for c in args.country]
    now = utc_now_iso()

    with httpx.Client(headers={"User-Agent": USER_AGENT},
                      follow_redirects=True) as client:
        # ----- Phase 1: discovery -----
        print(f"Discovering tracks across {len(countries)} country totals pages…")
        core_by_id: dict[str, dict] = {}
        country_rows: list[dict] = []

        for cc in countries:
            try:
                rows = discover_country(client, cc, debug=args.debug)
            except Exception as exc:
                print(f"    ERROR fetching {cc}: {exc}", file=sys.stderr)
                continue
            for r in rows:
                core_by_id[r["track_id"]] = {
                    "track_id": r["track_id"],
                    "title": r["title"],
                    "artist": r["artist"],
                    "artist_id": r["artist_id"],
                }
                country_rows.append({
                    "track_id": r["track_id"],
                    "country": cc,
                    "weeks_on": r["weeks_on"],
                    "peak": r["peak"],
                    "total_streams": r["total_streams"],
                    "last_seen": now,
                })
            time.sleep(REQUEST_DELAY)

        print(f"Discovered {len(core_by_id)} unique tracks "
              f"across {len(countries)} countries.")

        if args.debug:
            return

        if not core_by_id:
            print("Nothing to do.")
            return

        # ----- Phase 2: persist totals -----
        with connect() as conn:
            init_db(conn)
            upsert_tracks_core(conn, list(core_by_id.values()))
            upsert_country_totals(conn, country_rows)
            if "IT" in countries:
                it_rows = [r for r in country_rows if r["country"] == "IT"]
                update_tracks_legacy_it(conn, it_rows)

            # ----- Phase 3: track-page scraping -----
            unique_ids = list(core_by_id.keys())
            decisions = [
                (tid, *needs_scrape(conn, tid,
                                    max_age_days=args.max_age_days,
                                    force=args.force))
                for tid in unique_ids
            ]
            reason_counts = Counter(r for _, _, r in decisions)
            to_scrape = [tid for tid, should, _ in decisions if should]
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

            for tid in tqdm(to_scrape, desc="tracks"):
                url = f"{BASE}/spotify/track/{tid}.html"
                try:
                    html = fetch(client, url)
                    entries = parse_track_page(html)
                    save_track_entries(conn, tid, entries)
                except Exception as e:
                    tqdm.write(f"  failed {tid}: {e}")
                time.sleep(REQUEST_DELAY)

            print("\nDone. Sanity check:")
            sanity = [
                ("Total tracks",
                 "SELECT COUNT(*) FROM tracks"),
                ("Total chart_entries",
                 "SELECT COUNT(*) FROM chart_entries"),
                ("Distinct countries in chart_entries",
                 "SELECT COUNT(DISTINCT country) FROM chart_entries"),
            ]
            for label, q in sanity:
                val = conn.execute(q).fetchone()[0]
                print(f"  {label}: {val:,}")

            for cc in countries:
                row = conn.execute(
                    "SELECT COUNT(*) FROM track_country_totals WHERE country = ?",
                    (cc,),
                ).fetchone()
                print(f"  totals {cc}: {row[0]:,} tracks")


if __name__ == "__main__":
    main()
