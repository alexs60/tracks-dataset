# Runbook

Step-by-step guide for operating the `tracks-dataset` pipeline from scratch through a full backfill.

---

## Prerequisites

- Python 3.9+ (3.12 recommended)
- Access to the network at `10.0.0.X` for the Essentia API and Postgres
- A copy of `kworb_italy.db` that you can use as the enrichment target (never use the live one)

### Python dependencies

```bash
pip install httpx psycopg2-binary
```

Or if you use the venv:

```bash
python -m venv venv
source venv/bin/activate
pip install httpx psycopg2-binary
```

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

## Part 1 — Scraper: populate chart data

The scraper fetches weekly chart data from kworb.net and writes it into `kworb_italy.db`.

```bash
# Normal run — skip tracks scraped within the last 90 days
python scraper/kworb_italy_scraper.py

# Shorter cache window (re-scrape more often)
python scraper/kworb_italy_scraper.py --max-age-days 30

# Force a full re-scrape of all tracks
python scraper/kworb_italy_scraper.py --force

# Smoke test (5 tracks only)
python scraper/kworb_italy_scraper.py --limit 5 --force
```

Output: `kworb_italy.db` containing `tracks` and `chart_entries` tables.

> **Do not use `kworb_italy.db` as the enrichment target.** Copy it first:
> ```bash
> cp kworb_italy.db /tmp/kworb_italy.db
> ```

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
Stage 3 (Essentia):            230 ok | 2 failed | 10 pending
Fully enriched:                221 (2.4%)
```

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

## Part 5 — Administrative operations

### Reset a single track

Clears a track's stage data so it will be retried on the next pipeline pass.

```bash
# Reset Stage 1 for one track
python scripts/reset_stage.py --stage 1 --track-id SPOTIFY_TRACK_ID

# Reset Stage 2 for one track
python scripts/reset_stage.py --stage 2 --track-id SPOTIFY_TRACK_ID

# Reset Stage 3 for one track
python scripts/reset_stage.py --stage 3 --track-id SPOTIFY_TRACK_ID
```

### Reset all tracks for a stage

Wipes all results for the given stage (e.g. after a schema change or parser fix).

```bash
python scripts/reset_stage.py --stage 3 --all
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
# 1. Scrape latest chart data into kworb_italy.db
python scraper/kworb_italy_scraper.py

# 2. Seed Postgres with tracks and chart_entries (edit migrate.load credentials first)
pgloader migrate.load

# 3. Apply audio feature schema to Postgres
python scripts/run_migrations.py

# 4. Check initial status
python scripts/status.py

# 5. Build and start the pipeline container
docker compose up -d --build

# 6. Follow logs
docker compose logs -f pipeline

# 7. Periodically check coverage in another terminal
python scripts/status.py
```

### With SQLite (development / local)

```bash
# 1. Scrape
python scraper/kworb_italy_scraper.py

# 2. Copy live DB to a safe enrichment target
cp kworb_italy.db /tmp/kworb_italy.db

# 3. Apply migrations (DB_PATH must be set in .env)
python scripts/run_migrations.py

# 4. Run the pipeline
python workers/run_pipeline.py --batch-size 50 --interval 30

# 5. Monitor
python scripts/status.py
```
