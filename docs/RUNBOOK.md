# Runbook

Step-by-step guide for operating the `tracks-dataset` pipeline from scratch through a full backfill.

---

## Prerequisites

- Python 3.9+ (3.12 recommended)
- Access to the network at `10.0.0.X` for the Essentia API and Postgres
- A copy of `kworb_italy.db` that you can use as the enrichment target (never use the live one)

### Python dependencies

```bash
pip install -r requirements.txt
```

Or if you use the venv:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` covers both the enrichment pipeline (`httpx`,
`psycopg2-binary`) and the kworb scraper (`selectolax`, `tenacity`, `tqdm`).

---

## Environment configuration

Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

### Key variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | For Postgres | `postgresql://user:password@10.0.0.25/dbname` |
| `DB_PATH` | For SQLite | Path to the SQLite enrichment DB. Ignored when `DATABASE_URL` is set. |
| `ESSENTIA_API_URL` | Yes | Full URL of the Essentia SVM API, e.g. `http://10.0.0.189:8000/analyze` |
| `ESSENTIA_API_AUDIO_FIELD` | No | Multipart field name for audio upload (default: `audio`) |
| `ESSENTIA_API_TIMEOUT_SEC` | No | HTTP timeout for Essentia calls in seconds (default: `300`) |
| `RECCOBEATS_BASE_URL` | No | Reccobeats API base (default: `https://api.reccobeats.com/v1`) |
| `WORKER_BATCH_SIZE` | No | Default batch size for standalone stage scripts (default: `50`) |
| `WORKER_RATE_LIMIT_QPS` | No | Requests/sec for rate-limited stages (default: `1`) |
| `SPOTIFY_EMBED_BASE` | No | Base URL for Spotify embed scraping (default: standard embed URL) |
| `SPOTIFY_EMBED_USER_AGENT` | No | User-agent header for Spotify embed requests |

### SQLite example

```dotenv
DB_PATH=/tmp/kworb_italy.db
ESSENTIA_API_URL=http://10.0.0.189:8000/analyze
RECCOBEATS_BASE_URL=https://api.reccobeats.com/v1
WORKER_BATCH_SIZE=50
WORKER_RATE_LIMIT_QPS=1
```

### PostgreSQL example

```dotenv
DATABASE_URL=postgresql://user:password@10.0.0.25/dbname
ESSENTIA_API_URL=http://10.0.0.189:8000/analyze
RECCOBEATS_BASE_URL=https://api.reccobeats.com/v1
WORKER_BATCH_SIZE=50
WORKER_RATE_LIMIT_QPS=1
```

`DB_PATH` is ignored when `DATABASE_URL` is set.

---

## Part 1 — Scraper (Stage 0): populate chart data

The scraper (`scraper/kworb_scraper.py`) fetches weekly chart data from kworb.net.
It writes to **either** SQLite (`DB_PATH`) **or** PostgreSQL (`DATABASE_URL`) —
the same env vars the enrichment pipeline uses, so you can scrape directly into
the production Postgres DB and skip pgloader entirely.

It supports multiple countries on a single invocation. Default country set
covers Western Europe, Iberia, Mediterranean, Nordics/Scandinavia, Baltics,
Central/Eastern Europe, Russia, and the United States (~27 countries). Each
`{cc}_weekly_totals.html` discovers tracks; then each track's per-track page
(`/spotify/track/{id}.html`) is fetched once — that page contains weekly chart
entries for *all* countries kworb tracks, not just the discovery country.

The scraper can be run **either**:
- Manually as a one-off (the typical workflow), **or**
- Automatically as **Stage 0** of the pipeline, before the enrichment loop —
  see [Part 4.1](#part-41--stage-0-pipeline-integrated-scraping-optional) below.
  Default is OFF (`PIPELINE_RUN_SCRAPER=false`) so the docker container does
  not re-scrape on every restart.

```bash
# Default: scrape all 8 countries into whatever DB env points at
python scraper/kworb_scraper.py

# Single country
python scraper/kworb_scraper.py --country IT

# Custom country list
python scraper/kworb_scraper.py --country IT GB FR DE ES US PT NL

# Shorter cache window (re-scrape more often)
python scraper/kworb_scraper.py --max-age-days 30

# Force a full re-scrape of all tracks
python scraper/kworb_scraper.py --force

# Smoke test
python scraper/kworb_scraper.py --country IT --limit 5 --force

# Inspect a country's totals page layout (writes totals_<cc>_debug.html, exits)
python scraper/kworb_scraper.py --country GB --debug
```

Tables written:
- `tracks` — core identification (`track_id`, `title`, `artist`, `artist_id`,
  `last_scraped`). Legacy IT-specific cols (`weeks_on_it`, `peak_it`,
  `last_chart_week`, `total_streams`) are still updated when scraping IT, so
  existing exports keep working.
- `chart_entries` — per-week, per-country chart positions and stream counts.
- `track_country_totals` — per-country summary
  (`weeks_on`, `peak`, `total_streams`, `last_chart_week`, `last_seen`),
  PK `(track_id, country)`.

The scraper bootstraps these tables idempotently on first run, so it works
against an empty DB. For an existing Postgres DB, `track_country_totals` is
also created by migration `003_track_country_totals.pg.sql` if you prefer to
manage schema centrally with `scripts/run_migrations.py`.

### Direct-to-Postgres scraping (recommended)

Set `DATABASE_URL` in `.env` (same value the pipeline uses) and run:

```bash
python scraper/kworb_scraper.py
```

No SQLite copy, no pgloader, no risk of `include drop` wiping enrichment data.

### SQLite scraping (legacy / development)

Unset `DATABASE_URL` (or leave it blank) and set `DB_PATH`:

```bash
DB_PATH=/tmp/kworb_charts.db python scraper/kworb_scraper.py
```

> The old `scraper/kworb_italy_scraper.py` (SQLite-only, IT-only) is kept for
> reference but is no longer the recommended entrypoint.

---

## Part 2 — Migrations: create enrichment schema

Applies the audio feature tables and preview columns to the target database.  
Run once before the first pipeline run, and again after any schema change.

```bash
python scripts/run_migrations.py
```

- **SQLite**: creates audio feature tables in `DB_PATH`; also adds `preview_url`, `preview_fetched`, `preview_status` columns to `tracks`
- **PostgreSQL**: creates audio feature tables only; adds preview columns with `ADD COLUMN IF NOT EXISTS` (safe to re-run)

---

## Part 3 — Docker deployment (Postgres, recommended for production)

This section covers the full deployment path for running the enrichment pipeline in Docker against the Postgres database at `10.0.0.25`.

### 3.0 — Prerequisites

- Docker and Docker Compose installed on the host machine
- The Postgres database at `10.0.0.25` is reachable from the host and from inside Docker containers (same LAN)
- The Essentia API at `10.0.0.189:8000` is reachable from inside Docker containers
- `kworb_italy.db` has been populated by the scraper (see Part 1)

---

### 3.1 — Configure `.env` for Postgres

Edit `.env` so `DATABASE_URL` points at your Postgres instance. Remove or comment out `DB_PATH` — it is ignored when `DATABASE_URL` is set.

```dotenv
DATABASE_URL=postgresql://user:password@10.0.0.25/dbname

ESSENTIA_API_URL=http://10.0.0.189:8000/analyze
ESSENTIA_API_AUDIO_FIELD=audio
ESSENTIA_API_TIMEOUT_SEC=300

RECCOBEATS_BASE_URL=https://api.reccobeats.com/v1
WORKER_BATCH_SIZE=50
WORKER_RATE_LIMIT_QPS=1
SPOTIFY_EMBED_USER_AGENT=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
```

---

### 3.2 — Seed Postgres with chart data (one-time)

The `tracks` and `chart_entries` tables must already exist in Postgres before running migrations. The easiest way to get the data across from `kworb_italy.db` is with **pgloader**.

**Install pgloader** (macOS):
```bash
brew install pgloader
```

**Edit `migrate.load`** — update the credentials and target DB to match your `.env`:
```
LOAD DATABASE
    FROM sqlite:///path/to/kworb_italy.db
    INTO postgresql://user:password@10.0.0.25:5432/dbname

WITH include drop, create tables, create indexes, reset sequences;
```

**Run it:**
```bash
pgloader migrate.load
```

This copies the full `tracks` and `chart_entries` tables into Postgres. It takes a few seconds for ~9k tracks. Re-running is safe — `include drop` recreates the tables from scratch.

> If you don't have pgloader, any other tool that can dump SQLite to Postgres works (DBeaver, DataGrip, `pg_loader` Python package, etc.). The target tables just need to exist before Step 3.3.

---

### 3.3 — Apply enrichment migrations to Postgres

This creates the audio feature tables (`track_analysis`, `track_reccobeats`, `track_high_level_*`) and adds the preview columns to `tracks`. Safe to re-run.

```bash
# Run from the repo root with DATABASE_URL in your environment (loaded from .env automatically)
python scripts/run_migrations.py
```

Expected output:
```
Applied migrations from migrations/001_audio_features.pg.sql
```

---

### 3.4 — Build the Docker image

```bash
docker compose build
```

The image is `python:3.12-slim` with `httpx` and `psycopg2-binary` installed. No Essentia binary is included — Stage 3 uses the remote API at `ESSENTIA_API_URL`.

---

### 3.5 — Start the container

```bash
# Start in the foreground (Ctrl+C to stop)
docker compose up

# Start in the background
docker compose up -d
```

The container reads `.env` via `env_file` in `docker-compose.yml`. It runs `workers/run_pipeline.py --batch-size 10 --interval 60` in a loop, processing 10 tracks per stage every 60 seconds until all tracks are enriched, then keeps polling for new ones.

`restart: unless-stopped` means Docker will restart the container automatically if it crashes or if the host reboots.

---

### 3.6 — Monitor progress

```bash
# Follow live logs from the container
docker compose logs -f pipeline

# In a separate terminal, check pipeline coverage
python scripts/status.py
```

The `logs/` directory is mounted from the host, so log files persist across container restarts:

```bash
# Tail Stage 3 failures
grep '"event": "failed"' logs/stage3.log | tail -20

# Watch pipeline pass summaries
tail -f logs/pipeline.log
```

---

### 3.7 — Adjust batch size or interval

Edit `docker-compose.yml` `command:` line and rebuild:

```yaml
command: python workers/run_pipeline.py --batch-size 50 --interval 30
```

```bash
docker compose up -d --build
```

Or override at runtime without editing the file:

```bash
docker compose run --rm pipeline python workers/run_pipeline.py --batch-size 100 --interval 10
```

---

### 3.8 — Stop and restart

```bash
# Stop the container (preserves the image)
docker compose down

# Restart without rebuilding
docker compose up -d

# Rebuild and restart (after code changes)
docker compose up -d --build
```

---

### 3.9 — Reset a stage inside the running Postgres

Use `scripts/reset_stage.py` locally (with `DATABASE_URL` in `.env`):

```bash
# Reset all Stage 3 results so they will be reprocessed
python scripts/reset_stage.py --stage 3 --all

# Reset one track across all stages
python scripts/reset_stage.py --stage 1 --track-id SPOTIFY_TRACK_ID
python scripts/reset_stage.py --stage 2 --track-id SPOTIFY_TRACK_ID
python scripts/reset_stage.py --stage 3 --track-id SPOTIFY_TRACK_ID
```

The running container will pick up the reset tracks on its next pass automatically.

---

## Part 4 — Enrichment pipeline (local / Python only)

### Option A — Python loop runner

Same behaviour as Docker but run directly, useful during development or when Docker is not available:

```bash
python workers/run_pipeline.py --batch-size 10 --interval 60
```

| Flag | Description |
|------|-------------|
| `--batch-size` | Tracks to claim per stage per pass (default: `10`) |
| `--interval` | Seconds to sleep between passes (default: `60`) |
| `--run-scraper` | Run the kworb scraper once at startup as Stage 0, before the loop (default: off; env: `PIPELINE_RUN_SCRAPER`) |
| `--scraper-max-age-days` | Track-page cache window for Stage 0 (default `90`; env: `SCRAPER_MAX_AGE_DAYS`) |
| `--scraper-force` | Stage 0 ignores the cache and re-scrapes every track (default off; env: `SCRAPER_FORCE`) |

### Stage 0: pipeline-integrated scraping (opt-in)

Stage 0 is the kworb scrape, run **once** at pipeline startup before any
enrichment stage. It is OFF by default — the scraper is normally an operator
task (`python scraper/kworb_scraper.py`). Enable it when you want the pipeline
container to also keep chart data fresh:

```bash
# Local
python workers/run_pipeline.py --run-scraper

# Docker — set PIPELINE_RUN_SCRAPER=true in .env, then:
docker compose up -d --build
```

Behavior:
- Runs once per process startup (until completion), then enters the enrichment
  loop. It does **not** repeat on every loop pass — kworb refreshes weekly.
- Caching applies (`--scraper-max-age-days`, default 90 days), so a container
  restart re-runs it cheaply: totals pages re-fetch, track pages skipped if
  fresh.
- Failures (network, parse) are logged to `logs/pipeline.log` as
  `event=stage0_error` but never crash the loop.
- Country list defaults to ~27 countries (Europe + Russia + USA + Scandinavia).
  Override with `SCRAPER_COUNTRIES` (whitespace-separated ISO-2 codes).

Logged events:

| Event | When |
|---|---|
| `stage0_start` | Stage 0 begins |
| `stage0_done` | Stage 0 finished — payload includes `discovered_tracks`, `scraped_tracks`, `track_errors`, `country_errors`, `skipped_fresh`, `skipped_dormant` |
| `stage0_error` | Stage 0 raised before completing |

### Option B — Individual stages

Run each stage independently. Useful for debugging, backfilling a single stage, or running at different batch sizes.

```bash
# Stage 1: resolve Spotify preview URLs
python workers/stage1_spotify_previews.py --batch-size 100

# Stage 2: fetch Reccobeats features
python workers/stage2_reccobeats.py --batch-size 100

# Stage 3: Essentia SVM analysis
python workers/stage3_essentia.py --batch-size 25
```

Common flags available on all three:

| Flag | Description |
|------|-------------|
| `--batch-size` | Tracks to claim per run |
| `--loop` | Keep running until no pending tracks |
| `--interval` | Seconds between loop iterations |

---

## Part 4 — Monitoring

### Pipeline status

```bash
python scripts/status.py
```

Example output:

```
Tracks total:                  9,150
Stage 1 (Spotify preview):     250 ok | 3 no_preview | 1 failed | 8,896 pending
Stage 2 (Reccobeats):          242 ok | 8 not_found | 0 no_features | 0 failed | 8,900 pending
Stage 2.1 (External fallback): 6 filled (kaggle_maharshipandya=6)
Stage 3 (Essentia):            230 ok | 2 failed | 10 pending
Audio features available:      248 (2.7%)
Fully enriched:                225 (2.5%)
```

`Audio features available` counts tracks with audio scalars from any source
(Reccobeats `ok` or external fallback row present). `Fully enriched` is that
set intersected with Essentia `ok`.

### Log files

Each stage writes structured JSON logs to `logs/`:

| File | Contains |
|------|---------|
| `logs/pipeline.log` | Per-pass summary when using `run_pipeline.py` |
| `logs/stage1.log` | Per-track events for Stage 1 |
| `logs/stage2.log` | Per-track events for Stage 2 |
| `logs/stage3.log` | Per-track events for Stage 3 |

To tail errors from Stage 3:

```bash
grep '"event": "failed"' logs/stage3.log | tail -20
```

---

## Part 4.1 — Stage 2.1: external CSV fallback (optional)

Stage 2.1 fills audio scalars for tracks where Reccobeats came back `not_found`,
`no_features`, or `failed` (or didn't return a row at all). It is **operator-run**
— not part of `run_pipeline.py` — and only useful when you have an external
Spotify-features CSV (e.g. a Kaggle dump) that overlaps your catalogue.

The flow is two steps: load the CSV into the staging table, then fill from it.
Both steps are idempotent.

### Step 1 — Load a CSV into the staging table

```bash
# Default column names (Maharshipandya / Kaggle layout)
python scripts/load_external_features.py \
    --csv ~/kaggle/maharshipandya/dataset.csv \
    --source kaggle_maharshipandya

# CSV with non-default column names + ISRC available
python scripts/load_external_features.py \
    --csv ~/dumps/other.csv \
    --source kaggle_other \
    --spotify-id-col spotify_track_id \
    --isrc-col isrc

# Reload after changing the CSV / column mapping
python scripts/load_external_features.py \
    --csv ~/dumps/other.csv \
    --source kaggle_other \
    --replace
```

Rows land in `external_audio_features_raw` keyed by `(source, spotify_id)`.
Re-running with the same `--source` upserts; pass `--replace` to clear that
source first.

### Step 2 — Fill `track_audio_features_external` from the staging table

```bash
# Match by Spotify track_id only (default)
python scripts/fill_external_features.py --source kaggle_maharshipandya

# Also try ISRC fallback for tracks that didn't match by Spotify ID,
# but where Reccobeats returned an ISRC for them.
python scripts/fill_external_features.py --source kaggle_other --by-isrc

# Sizing run — count matches without writing anything
python scripts/fill_external_features.py --source kaggle_x --dry-run
```

The fill is gated on `track_reccobeats.status` — it only writes rows for tracks
where Reccobeats did **not** return `ok`, so Reccobeats data is never overwritten.
Re-runnable: `ON CONFLICT (track_id) DO UPDATE` keeps the latest values.

### Verifying coverage

```bash
python scripts/status.py
```

Look for the `Stage 2.1 (External fallback)` line (counts and per-source
breakdown) and `Audio features available` (the union of Reccobeats `ok` and
external fallback). Exports include the merged values automatically (see Part
6.2 below); the `af_source` column records which source populated each row.

### Resetting

See Part 5. `--stage 2.1 --all` clears `track_audio_features_external` but
leaves the staging table intact, so a re-fill doesn't require re-loading the
CSV.

---

## Part 4.2 — Stage 2.2: Essentia-derived scalars (automatic)

Stage 2.2 fills the same `track_audio_features_external` table as Stage 2.1, but
the values come from the Stage 3 Essentia output rather than an external CSV.
It runs **automatically** as part of `run_pipeline.py`, immediately after Stage
3, and only writes rows for tracks where:

- `track_analysis.status = 'ok'` (Stage 3 completed)
- Reccobeats did **not** return `ok` (Stage 2 missed the track)
- No external row exists yet (Stage 2.1 fills win — see priority below)

The new rows are tagged `source='essentia_derived'`, `matched_by='essentia'`.

### Derivation formulas

| Spotify scalar | Source | Formula |
|---|---|---|
| `acousticness` (0..1) | binary classifier | `mood_acoustic.prob_positive` |
| `danceability` (0..1) | binary classifier | `danceability.prob_positive` |
| `energy` (0..1) | derived | mean of `clip((loudness_ebu128 + 60) / 60, 0, 1)` and `1 - mood_relaxed.prob_positive` |
| `instrumentalness` (0..1) | binary classifier | `1 - voice_instrumental.prob_positive` |
| `liveness` (0..1) | — | NULL (no clean Essentia proxy) |
| `loudness` (dB) | low-level | `loudness_ebu128` |
| `speechiness` (0..1) | — | NULL (no clean Essentia proxy) |
| `tempo` (BPM) | low-level | `bpm` |
| `valence` (0..1) | derived | `(mood_happy.prob_positive - mood_sad.prob_positive + 1) / 2` |

These are proxies, not Spotify's actual values. They're a reasonable substitute
for clustering/filtering on Italian charts, but not for cross-comparison with
public Spotify-features datasets — use the merged view's `audio_features_source`
column to tell which rows are Reccobeats / CSV / `essentia_derived`.

### Priority (when a track has multiple sources)

1. Reccobeats `ok` — preferred by `v_track_audio_features_merged`
2. CSV fallback (`kaggle_*` etc.) — fills only when Reccobeats is not `ok`
3. Essentia-derived — fills only when neither of the above produced a row

Stage 2.2 uses `ON CONFLICT (track_id) DO NOTHING`, so it never overwrites a
Stage 2.1 row. Conversely, `fill_external_features.py` uses `ON CONFLICT DO
UPDATE`, so loading a Kaggle dump *will* overwrite a previous `essentia_derived`
row for the same track — the more authoritative source wins.

### Running it manually

The pipeline runner (`workers/run_pipeline.py`) calls Stage 2.2 every pass.
For a one-shot drain or testing, run it directly:

```bash
# One pass (default 50 tracks)
python workers/stage2_2_essentia_derived.py

# Drain everything
python workers/stage2_2_essentia_derived.py --loop --batch-size 500
```

### Verifying

```bash
python scripts/status.py
```

The `Stage 2.1 (External fallback)` line shows the per-source breakdown,
including `essentia_derived=N`. That count plus any `kaggle_*` counts gives the
total filled-by-fallback. Tracks counted under `pending` are tracks Reccobeats
missed *and* Stage 2.2 hasn't been able to derive yet (typically because
Stage 3 hasn't completed on them).

### Resetting

See Part 5. `--stage 2.2 --all` clears only `essentia_derived` rows, so any
Kaggle CSV fills survive untouched.

---

## Part 5 — Administrative operations

### Reset a single track

Clears a track's stage data so it will be retried on the next pipeline pass.

```bash
# Reset Stage 1 for one track
python scripts/reset_stage.py --stage 1 --track-id SPOTIFY_TRACK_ID

# Reset Stage 2 for one track
python scripts/reset_stage.py --stage 2 --track-id SPOTIFY_TRACK_ID

# Reset Stage 2.1 (external fallback — clears ALL external rows for the track)
python scripts/reset_stage.py --stage 2.1 --track-id SPOTIFY_TRACK_ID

# Reset Stage 2.2 (only essentia_derived rows — preserves Kaggle CSV fills)
python scripts/reset_stage.py --stage 2.2 --track-id SPOTIFY_TRACK_ID

# Reset Stage 3 for one track
python scripts/reset_stage.py --stage 3 --track-id SPOTIFY_TRACK_ID
```

### Reset all tracks for a stage

Wipes all results for the given stage (e.g. after a schema change or parser fix).

```bash
python scripts/reset_stage.py --stage 3 --all

# Stage 2.1: clears all rows in track_audio_features_external (CSV fills AND
# essentia_derived). Leaves the external_audio_features_raw staging table
# intact, so re-running fill_external_features.py is enough — no need to
# reload the CSV.
python scripts/reset_stage.py --stage 2.1 --all

# Stage 2.2: clears only rows with source='essentia_derived'. Useful after
# changing the derivation formulas in stage2_2_essentia_derived.py — Kaggle
# CSV fills survive.
python scripts/reset_stage.py --stage 2.2 --all
```

### Stage status meanings

| Stage | Status | Meaning |
|-------|--------|---------|
| 1 | `ok` | Preview URL resolved |
| 1 | `no_preview` | Track has no 30-second preview on Spotify |
| 1 | `failed` | Network error or parsing failure |
| 2 | `ok` | Reccobeats features fetched |
| 2 | `not_found` | Track not in Reccobeats catalogue |
| 2 | `no_features` | Track in catalogue but no audio features |
| 2 | `failed` | API error |
| 2.1 | (row present) | Audio features filled from external CSV (only for tracks where Stage 2 didn't return `ok`). The `source` column records which CSV; `matched_by` records `spotify_id` or `isrc`. |
| 2.2 | (row with `source='essentia_derived'`) | Spotify-style scalars derived from the Stage 3 Essentia output. Only written when Reccobeats != `ok` AND no other external row exists. `liveness` and `speechiness` are always NULL — no clean Essentia proxy. |
| 3 | `ok` | Essentia SVM descriptors stored |
| 3 | `failed` | Download or API error |

---

## Part 6 — Export dataset

Two export scripts, both supporting **SQLite or PostgreSQL** as the source.

Backend selection (same for both scripts):

- Pass `--database-url postgresql://user:pass@host/db`, **or**
- set `DATABASE_URL` in the environment (auto-loaded from `.env`), **or**
- fall back to SQLite (`--db <path>` for `csvexport.py`, or `DB_PATH` env var / default `kworb_italy.db` for `export_dataset.py`).

### 6.1 — `export_dataset.py` (publishable Parquet/CSV bundle)

Exports the bare `chart_entries` + `tracks` tables to Parquet and CSV plus a Frictionless `datapackage.json`.

```bash
pip install pandas pyarrow

# SQLite (uses DB_PATH env var or ./kworb_italy.db)
python scripts/export_dataset.py

# PostgreSQL via env var (loaded from .env)
python scripts/export_dataset.py

# PostgreSQL via flag (overrides DATABASE_URL)
python scripts/export_dataset.py --database-url postgresql://user:pass@10.0.0.25/dbname
```

Output in `dataset/`:

```
dataset/
  chart_entries.parquet
  chart_entries.csv
  tracks.parquet
  tracks.csv
  README.md
  datapackage.json
```

### 6.2 — `csvexport.py` (enriched per-country CSV export)

Joins `chart_entries` with the enrichment tables (`track_reccobeats`, `track_analysis`, `track_high_level_*`) and writes one CSV per country. By default a track is included only if `track_analysis.status='ok'` (the "Essentia data present" gate).

```bash
# SQLite, all countries, default output dir
python scripts/csvexport.py --db /tmp/kworb_italy.db

# Postgres via env var (DATABASE_URL from .env)
python scripts/csvexport.py

# Postgres via flag, IT only, gzipped
python scripts/csvexport.py \
    --database-url postgresql://user:pass@10.0.0.25/dbname \
    --country IT --gzip

# One row per track per country (most recent chart appearance)
python scripts/csvexport.py --latest-only

# Drop the Essentia gate (export every chart row, regardless of enrichment)
python scripts/csvexport.py --no-essentia-gate

# Also require Reccobeats data
python scripts/csvexport.py --require-reccobeats

# Date / streams filters
python scripts/csvexport.py --since 2024-01-01 --min-streams 10000
```

Output: `exports/<timestamp>/charts_<country>.csv` (one file per country in `chart_entries`, unless `--country` narrows). Use `--out` to override the directory.

Postgres connections from this script are opened in **read-only** mode, so the export can never block or interfere with the running enrichment pipeline.

---

## Typical full-backfill sequence

### With Postgres + Docker (production)

```bash
# 1. Apply enrichment + chart-data schema to Postgres
python scripts/run_migrations.py

# 2. Scrape latest chart data DIRECTLY into Postgres (DATABASE_URL from .env)
python scraper/kworb_scraper.py

# 3. Check initial status
python scripts/status.py

# 4. Build and start the pipeline container
docker compose up -d --build

# 5. Follow logs
docker compose logs -f pipeline

# 6. Periodically check coverage in another terminal
python scripts/status.py

# 7. (optional) Once Stage 2 has settled, fill the gaps from an external CSV.
#    See Part 4.1 for details.
python scripts/load_external_features.py --csv ~/kaggle/dataset.csv --source kaggle_maharshipandya
python scripts/fill_external_features.py --source kaggle_maharshipandya
```

> The legacy pgloader path (Part 3.2) is no longer required: the scraper now
> writes directly to Postgres and bootstraps the chart-data tables itself, so
> `pgloader` would only overwrite enrichment data with `include drop`. Keep
> `migrate.load` only if you need a one-shot SQLite → Postgres seed.

### With SQLite (development / local)

```bash
# 1. Apply migrations (DB_PATH in .env, DATABASE_URL unset)
python scripts/run_migrations.py

# 2. Scrape
python scraper/kworb_scraper.py

# 3. Run the pipeline
python workers/run_pipeline.py --batch-size 50 --interval 30

# 4. Monitor
python scripts/status.py

# 5. (optional) Fill remaining gaps from an external CSV. See Part 4.1.
python scripts/load_external_features.py --csv ~/kaggle/dataset.csv --source kaggle_maharshipandya
python scripts/fill_external_features.py --source kaggle_maharshipandya
```
