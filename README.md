# tracks-dataset

A research pipeline for building an enriched Spotify chart dataset for Italy.

Two independent workflows:

1. **Scraper** — collects weekly chart data from kworb.net into `kworb_italy.db`
2. **Enrichment pipeline** — adds Spotify preview URLs, Reccobeats audio features, and Essentia SVM descriptors to each track

---

## Project structure

```
scraper/
  kworb_italy_scraper.py      # kworb.net chart scraper

workers/
  run_pipeline.py             # loop runner: executes all three stages in sequence
  stage1_spotify_previews.py  # Stage 1: resolve Spotify preview CDN URLs
  stage2_reccobeats.py        # Stage 2: fetch Reccobeats metadata & scalars
  stage3_essentia.py          # Stage 3: Essentia SVM analysis via remote API
  lib/                        # shared library (db, env, logging, http helpers)

migrations/
  001_audio_features.sql      # SQLite schema for enrichment tables
  001_audio_features.pg.sql   # PostgreSQL equivalent (audio tables only)

scripts/
  run_migrations.py           # apply migrations to the target database
  status.py                   # print per-stage coverage counts
  reset_stage.py              # reset one stage for one or all tracks
  export_dataset.py           # export chart data to Parquet/CSV for analysis
  csvexport.py                # export enriched tracks to per-country CSV files

docs/
  RUNBOOK.md                  # step-by-step operating guide
  audio_features_spike.md     # original spike notes
  pipeline_plan.md            # implementation plan archive

Dockerfile                    # single-container pipeline image (Python 3.12-slim)
docker-compose.yml            # runs the enrichment pipeline as one service
requirements.txt              # httpx, psycopg2-binary
.env.example                  # all supported env vars with documentation
```

---

## Quick start

See [docs/RUNBOOK.md](docs/RUNBOOK.md) for the full operating guide.

```bash
# 1. Copy and fill in environment variables
cp .env.example .env

# 2. Apply migrations
python scripts/run_migrations.py

# 3. Run the pipeline (batch of 10, polls every 60 s)
python workers/run_pipeline.py --batch-size 10

# 4. Check progress
python scripts/status.py
```

---

## Databases

| File | Purpose |
|------|---------|
| `kworb_italy.db` | Live chart data written by the scraper — do not use as enrichment target |
| `/tmp/kworb_italy.db` | Default SQLite enrichment target (set via `DB_PATH`) |
| `DATABASE_URL` | Set to a `postgresql://` URL to target Postgres instead |

The enrichment pipeline never touches `kworb_italy.db` directly. Always set `DB_PATH` or `DATABASE_URL` to a separate database.

---

## Enrichment stages

| Stage | Script | What it does |
|-------|--------|-------------|
| 1 | `stage1_spotify_previews.py` | Scrapes Spotify embed page to resolve the 30-second preview CDN URL |
| 2 | `stage2_reccobeats.py` | Fetches Reccobeats track metadata and audio feature scalars |
| 3 | `stage3_essentia.py` | Downloads the preview mp3, POSTs it to the remote Essentia SVM API, stores descriptors |

Each stage claims only tracks that the previous stage completed. Stages are independent and idempotent — re-running a stage skips already-processed tracks.

---

## Docker

```bash
docker compose up --build
```

Runs the enrichment pipeline as a single always-on container. Batch size and poll interval are configured in `docker-compose.yml`. All configuration is read from `.env`.