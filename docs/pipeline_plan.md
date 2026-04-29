# Audio Features Enrichment Pipeline — Implementation Plan

**Project:** Italian charts scraper, audio features enrichment
**Owner:** Alessandro
**Target environment:** Mac Mini home server (Ubuntu, 16 GB RAM), Docker / Docker Compose
**Existing storage:** SQLite (single file, schema below)
**Status:** Spec — ready for analysis pass, then implementation

> **Addendum (added later):** an additional **Stage 2.1** was introduced after the
> initial implementation to cover tracks where Reccobeats does not have audio
> features. It is operator-run, not part of the loop pipeline: load a Spotify-
> features CSV (e.g. a Kaggle dump) into a staging table, then fill
> `track_audio_features_external` for tracks where Reccobeats came back
> non-`ok`. Schema lives in `migrations/002_external_audio_features.{sql,pg.sql}`;
> tooling is `scripts/load_external_features.py` + `scripts/fill_external_features.py`;
> the merged values are exposed via `v_track_audio_features_merged`. See the
> README and `docs/RUNBOOK.md` (Part 4.1) for usage. The §3–§9 schema and stage
> design below are otherwise unchanged.

---

## 1. Goal

Enrich the existing `tracks` table with two complementary feature sets:

1. **Reccobeats metadata + Spotify-style scalars** — fast, free, no audio download needed.
2. **Essentia high-level + low-level descriptors** — self-hosted analysis on Spotify 30-second previews, gives the full mood/genre/timbre taxonomy that Reccobeats does not expose.

The two sources are **complementary, not redundant**. Reccobeats fills cross-database identifiers (ISRC, EAN, UPC), licensing geography, and Spotify-comparable scalars. Essentia fills the 12 binary classifiers (timbre, tonal, danceable, voice, gender, 7 moods) and 6 categorical classifiers (MIREX clusters, 4 genre models, ISMIR04 rhythm).

The pipeline must be:

- **Idempotent.** Re-running any stage on a track that's already done is a no-op.
- **Stage-isolated.** A failure in one stage doesn't poison the others.
- **Restartable.** Killing the worker mid-batch and restarting picks up where it left off.
- **Latest-overwrites.** No version history of analyses kept (per design decision).

---

## 2. Existing schema (do not modify)

```sql
CREATE TABLE IF NOT EXISTS tracks (
    track_id        TEXT PRIMARY KEY,   -- assumed Spotify track ID
    title           TEXT NOT NULL,
    artist          TEXT NOT NULL,
    artist_id       TEXT,
    total_streams   INTEGER,
    weeks_on_it     INTEGER,
    peak_it         INTEGER,
    last_scraped    TEXT,
    last_chart_week TEXT
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
```

**Assumption:** `tracks.track_id` is the Spotify track ID. If it isn't, the agent must surface this before implementation — every downstream stage depends on it.

---

## 3. New schema

All new tables. Run the DDL in a single `executescript()` call after the existing one. Everything is `IF NOT EXISTS` so it's safe to re-run.

### 3.1. Preview tracking — added to `tracks`

The `preview_url` is resolved by scraping Spotify's public embed page (see §4.1 for why the Web API path is dead). The columns themselves are source-agnostic; the worker that populates them changes.

```sql
ALTER TABLE tracks ADD COLUMN preview_url     TEXT;
ALTER TABLE tracks ADD COLUMN preview_fetched TEXT;   -- ISO8601, NULL = not tried
ALTER TABLE tracks ADD COLUMN preview_status  TEXT;   -- 'ok' | 'no_preview' | 'failed'
```

> **Note for the agent:** SQLite does not support `IF NOT EXISTS` on `ALTER TABLE`. Wrap each `ALTER` in a try/except and ignore `duplicate column name` errors so the migration is idempotent.

> **CDN expiry caveat:** the `p.scdn.co/mp3-preview/...` URLs returned by the embed page carry a signed query string with a finite TTL (typically days to weeks). Stage 3 must download the mp3 reasonably soon after Stage 1 resolves it. If Stage 3 hits HTTP 403/410 on the cached `preview_url`, it should mark the track `preview_status='failed'` so Stage 1 re-resolves it on the next pass.

### 3.2. Reccobeats metadata + scalars

One row per track. Combines identifier metadata (from `GET /v1/track`) and audio scalars (from `GET /v1/track/{id}/audio-features`) — a single fetch stage produces both.

```sql
CREATE TABLE IF NOT EXISTS track_reccobeats (
    track_id            TEXT PRIMARY KEY REFERENCES tracks(track_id),
    reccobeats_id       TEXT,            -- their UUID, useful for /recommendation seeds
    isrc                TEXT,            -- international recording code
    ean                 TEXT,
    upc                 TEXT,
    duration_ms         INTEGER,
    available_countries TEXT,            -- raw CSV string from API ("AR,AU,AT,BE,...")

    -- Spotify-style audio features (continuous 0..1 unless noted)
    acousticness        REAL,
    danceability        REAL,
    energy              REAL,
    instrumentalness    REAL,
    liveness            REAL,
    loudness            REAL,            -- dB, typically -60..0
    speechiness         REAL,
    tempo               REAL,            -- BPM
    valence             REAL,

    fetched_at          TEXT NOT NULL,
    status              TEXT NOT NULL    -- 'ok' | 'not_found' | 'no_features' | 'failed'
);
CREATE INDEX IF NOT EXISTS idx_reccobeats_isrc ON track_reccobeats(isrc);
```

### 3.3. Essentia analysis — main row

```sql
CREATE TABLE IF NOT EXISTS track_analysis (
    track_id            TEXT PRIMARY KEY REFERENCES tracks(track_id),
    analyzed_at         TEXT NOT NULL,
    extractor_version   TEXT NOT NULL,   -- e.g. 'essentia-2.1_beta5'
    models_version      TEXT NOT NULL,   -- e.g. 'svm_models-2017-03'
    status              TEXT NOT NULL,   -- 'ok' | 'failed'
    error               TEXT,

    -- Full Essentia output. Use SQLite JSON1 to extract anything not projected below.
    raw_json            TEXT,

    -- Flat low-level columns (the ones you'll actually filter on).
    bpm                 REAL,
    bpm_confidence      REAL,
    danceability_raw    REAL,            -- Essentia's continuous value, ~0..3
    loudness_ebu128     REAL,
    average_loudness    REAL,
    dynamic_complexity  REAL,
    key_key             TEXT,            -- 'C', 'C#', ...
    key_scale           TEXT,            -- 'major' | 'minor'
    key_strength        REAL,
    chords_changes_rate REAL,
    tuning_frequency    REAL,
    onset_rate          REAL,
    duration_sec        REAL
);
CREATE INDEX IF NOT EXISTS idx_ta_bpm ON track_analysis(bpm);
CREATE INDEX IF NOT EXISTS idx_ta_key ON track_analysis(key_key, key_scale);
```

### 3.4. Essentia high-level — binary classifiers (long format)

```sql
CREATE TABLE IF NOT EXISTS track_high_level_binary (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,
        -- Allowed values (12):
        -- 'timbre', 'tonal_atonal', 'danceability', 'voice_instrumental',
        -- 'gender', 'mood_acoustic', 'mood_electronic', 'mood_relaxed',
        -- 'mood_sad', 'mood_party', 'mood_happy', 'mood_aggressive'
    value           TEXT NOT NULL,        -- winning class label (e.g. 'dark', 'danceable')
    probability     REAL NOT NULL,        -- prob of winning class
    prob_positive   REAL NOT NULL,        -- prob of canonical positive class — see §3.6
    PRIMARY KEY (track_id, classifier)
);
CREATE INDEX IF NOT EXISTS idx_thlb_clf_pos
    ON track_high_level_binary(classifier, prob_positive DESC);
```

### 3.5. Essentia high-level — categorical classifiers (long format)

```sql
CREATE TABLE IF NOT EXISTS track_high_level_categorical (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,
        -- Allowed values (6):
        -- 'moods_mirex', 'genre_electronic', 'genre_tzanetakis',
        -- 'genre_dortmund', 'genre_rosamerica', 'rhythm_ismir04'
    value           TEXT NOT NULL,        -- winning class
    probability     REAL NOT NULL,
    PRIMARY KEY (track_id, classifier)
);

CREATE TABLE IF NOT EXISTS track_high_level_class_probs (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,        -- same set as track_high_level_categorical
    class_label     TEXT NOT NULL,        -- e.g. 'cluster3', 'jazz', 'cha cha'
    probability     REAL NOT NULL,
    PRIMARY KEY (track_id, classifier, class_label)
);
CREATE INDEX IF NOT EXISTS idx_thlcp_lookup
    ON track_high_level_class_probs(classifier, class_label, probability DESC);
```

### 3.6. Canonical "positive class" mapping for binary classifiers

The `prob_positive` column stores P(positive class), regardless of which class won. This makes ranking queries trivial.

| `classifier`           | Positive class    | Negative class       |
|------------------------|-------------------|----------------------|
| `timbre`               | `dark`            | `bright`             |
| `tonal_atonal`         | `tonal`           | `atonal`             |
| `danceability`         | `danceable`       | `not_danceable`      |
| `voice_instrumental`   | `voice`           | `instrumental`       |
| `gender`               | `female`          | `male`               |
| `mood_acoustic`        | `acoustic`        | `not_acoustic`       |
| `mood_electronic`      | `electronic`      | `not_electronic`     |
| `mood_relaxed`         | `relaxed`         | `not_relaxed`        |
| `mood_sad`             | `sad`             | `not_sad`            |
| `mood_party`           | `party`           | `not_party`          |
| `mood_happy`           | `happy`           | `not_happy`          |
| `mood_aggressive`      | `aggressive`      | `not_aggressive`     |

> **Implementation note:** the actual class labels emitted by Essentia models may differ in punctuation/casing (e.g. `not_danceable` vs `not danceable`). The agent must verify against a real Essentia output during the analysis spike (§5) and adjust this table accordingly.

### 3.7. Convenience view (wide format)

```sql
CREATE VIEW IF NOT EXISTS v_track_features AS
SELECT
    t.track_id, t.title, t.artist,

    -- Reccobeats scalars
    rb.acousticness, rb.danceability AS rb_danceability, rb.energy,
    rb.instrumentalness, rb.liveness, rb.loudness AS rb_loudness,
    rb.speechiness, rb.tempo AS rb_tempo, rb.valence,
    rb.isrc, rb.duration_ms,

    -- Essentia low-level
    a.bpm, a.key_key, a.key_scale, a.loudness_ebu128,
    a.danceability_raw, a.duration_sec,

    -- Essentia binary classifiers (P(positive))
    MAX(CASE WHEN b.classifier='danceability'        THEN b.prob_positive END) AS p_danceable,
    MAX(CASE WHEN b.classifier='mood_happy'          THEN b.prob_positive END) AS p_happy,
    MAX(CASE WHEN b.classifier='mood_sad'            THEN b.prob_positive END) AS p_sad,
    MAX(CASE WHEN b.classifier='mood_party'          THEN b.prob_positive END) AS p_party,
    MAX(CASE WHEN b.classifier='mood_aggressive'     THEN b.prob_positive END) AS p_aggressive,
    MAX(CASE WHEN b.classifier='mood_relaxed'        THEN b.prob_positive END) AS p_relaxed,
    MAX(CASE WHEN b.classifier='mood_acoustic'       THEN b.prob_positive END) AS p_acoustic,
    MAX(CASE WHEN b.classifier='mood_electronic'     THEN b.prob_positive END) AS p_electronic,
    MAX(CASE WHEN b.classifier='timbre'              THEN b.prob_positive END) AS p_dark,
    MAX(CASE WHEN b.classifier='tonal_atonal'        THEN b.prob_positive END) AS p_tonal,
    MAX(CASE WHEN b.classifier='voice_instrumental'  THEN b.prob_positive END) AS p_voice,
    MAX(CASE WHEN b.classifier='gender'              THEN b.prob_positive END) AS p_female,

    -- Essentia categorical classifiers (winning class)
    MAX(CASE WHEN c.classifier='genre_dortmund'      THEN c.value END) AS genre_dortmund,
    MAX(CASE WHEN c.classifier='genre_tzanetakis'    THEN c.value END) AS genre_tzanetakis,
    MAX(CASE WHEN c.classifier='genre_rosamerica'    THEN c.value END) AS genre_rosamerica,
    MAX(CASE WHEN c.classifier='genre_electronic'    THEN c.value END) AS genre_electronic,
    MAX(CASE WHEN c.classifier='moods_mirex'         THEN c.value END) AS mirex_cluster,
    MAX(CASE WHEN c.classifier='rhythm_ismir04'      THEN c.value END) AS rhythm
FROM tracks t
LEFT JOIN track_reccobeats              rb ON rb.track_id = t.track_id
LEFT JOIN track_analysis                a  ON a.track_id  = t.track_id
LEFT JOIN track_high_level_binary       b  ON b.track_id  = t.track_id
LEFT JOIN track_high_level_categorical  c  ON c.track_id  = t.track_id
GROUP BY t.track_id;
```

---

## 4. Pipeline architecture

Three independent, idempotent stages. Each runs as its own worker process / cron job / one-shot script.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        tracks (existing)                                │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
            ┌──────────────────────┼──────────────────────┐
            │                      │                      │
            ▼                      ▼                      ▼
   ┌────────────────┐    ┌────────────────┐    ┌────────────────────┐
   │ Stage 1:       │    │ Stage 2:       │    │ Stage 3:           │
   │ Spotify embed  │    │ Reccobeats     │    │ Essentia analysis  │
   │ scrape →       │    │ metadata +     │    │ (depends on        │
   │ preview URL    │    │ scalars        │    │  Stage 1 success)  │
   └───────┬────────┘    └───────┬────────┘    └─────────┬──────────┘
           │                     │                       │
           ▼                     ▼                       ▼
    tracks.preview_url    track_reccobeats        track_analysis
    tracks.preview_status                         track_high_level_binary
                                                  track_high_level_categorical
                                                  track_high_level_class_probs
```

### 4.1. Stage 1 — Spotify preview URL resolver (embed scrape)

**Why not the Web API.** The November 27, 2024 Spotify Web API change restricted `preview_url` in track responses for newly-registered apps and apps not already in extended mode. This was confirmed empirically against this project's credentials: `GET /v1/tracks/{id}` returns the new simplified Track object **with no `preview_url` key at all** (not `null`, omitted), and `GET /v1/audio-features` returns HTTP 403. The Web API path is therefore unviable and Stage 1 instead scrapes Spotify's own public embed page — the same source `open.spotify.com` uses to render its iframe player.

**Input:** rows in `tracks` where `preview_status IS NULL` OR (`preview_status='failed'` AND a retry is desired).
**Output:** updated `preview_url`, `preview_fetched`, `preview_status` columns.

**Logic per track:**
1. `GET https://open.spotify.com/embed/track/{track_id}` with a realistic browser-like `User-Agent` and `follow_redirects=True`. No auth required.
2. Match the response body against the regex:
   ```
   "audioPreview"\s*:\s*\{\s*"url"\s*:\s*"([^"]+)"\s*\}
   ```
   This is the same approach used by the popular `spotify-audio-previews` library — robust against bundle restructuring because it doesn't depend on the surrounding `__NEXT_DATA__` shape.
3. The captured URL is a `https://p.scdn.co/mp3-preview/...` link to a 30-second mp3, downloadable without auth.
4. Update SQLite:
   - `preview_url` set if matched (and starts with `https://`), else NULL.
   - `preview_status`:
     - `'ok'` if URL captured.
     - `'no_preview'` if the embed page returned 200 but no `audioPreview` block matched (regional licensing, or Spotify simply doesn't have a preview for that track).
     - `'failed'` on HTTP non-200, network error, or unexpected parse failure.
   - `preview_fetched` = current ISO8601 timestamp.

**Notes:**
- **Zero Spotify auth.** Stage 1 has no dependency on `SPOTIFY_CLIENT_ID/SECRET`. Embed pages are public.
- **Coverage.** For Italian charting tracks (mostly major-label catalog) expect 5–15% `no_preview`. If the rate is much higher, the regex or the embed structure has shifted — re-run the spike (§5).
- **Surface fragility.** This is a non-API surface and Spotify can change the embed bundle at any time. Mitigations:
  - Isolate parsing in `lib/spotify_embed.py` so a bundle change is a one-file fix.
  - Keep the regex permissive (whitespace-tolerant, no anchoring around the `"audioPreview"` token).
  - Log a sample of the raw HTML on parse-failure for one in N tracks so future regressions are diagnosable from logs alone.
- **Rate limiting.** No documented limit on the embed endpoint. Throttle to ~1 request/second with small jitter, exponential backoff on HTTP 429 / 503. Batching is not possible — one HTTP request per track.
- **CDN URL freshness.** See §3.1 caveat: `preview_url` strings are signed and expire. Don't let Stage 1 results sit unused for weeks before Stage 3 picks them up.
- **Throughput math.** At ~1 req/sec, 12 K tracks ≈ 3.5 hours wall-clock for a full backfill. Acceptable for a one-shot run; run nightly thereafter only against new chart entries.

### 4.2. Stage 2 — Reccobeats metadata + scalars

**Input:** rows in `tracks` where there's no row in `track_reccobeats` (or `status != 'ok'` and retry is desired).
**Output:** one row per track in `track_reccobeats`.

**API endpoints:**
- Track detail by Spotify ID: `GET https://api.reccobeats.com/v1/track?ids={spotify_id}` (supports multiple IDs).
- Audio features by Reccobeats ID: `GET https://api.reccobeats.com/v1/track/{reccobeats_id}/audio-features`.

**Logic:**
1. For each batch of unprocessed Spotify IDs, call the track detail endpoint.
2. From the response, extract: `id` (Reccobeats UUID), `isrc`, `ean`, `upc`, `durationMs`, `availableCountries`.
3. For each Reccobeats ID returned, call `/audio-features` to get the 9 scalars.
4. `INSERT OR REPLACE` into `track_reccobeats` with `status='ok'`.
5. If track-detail returns 404, write `status='not_found'`. If audio-features returns 404, keep metadata but write `status='no_features'`.

**Notes:**
- Reccobeats requires no API key. There is a rate limit; the worker should backoff on HTTP 429.
- A track can be in Reccobeats' DB for metadata but not have features computed yet — handle that case explicitly (`'no_features'`).
- The `availableCountries` field is a comma-separated string of ISO country codes. Store as-is; parse at query time if ever needed.

### 4.3. Stage 3 — Essentia analysis worker

**Input:** rows in `tracks` where `preview_status='ok'` AND no `track_analysis` row exists with `status='ok'`.
**Output:** one row per track in `track_analysis` + N rows in the three high-level tables.

**Logic per track:**
1. Read `preview_url` from `tracks`.
2. Download mp3 to `/tmp/{track_id}.mp3` (preview is ~30s, ~500 KB — trivial).
3. Run `essentia_streaming_extractor_music /tmp/{track_id}.mp3 /tmp/{track_id}.json` with a profile that enables `highlevel.compute=1` and points at all 18 SVM model files (12 binary + 6 categorical).
4. Parse the JSON output.
5. In a single SQLite transaction:
   - Delete existing rows for this `track_id` from all four analysis tables (cleanup before re-insert).
   - `INSERT` into `track_analysis` with full JSON in `raw_json` plus extracted flat columns.
   - For each of 12 binary classifiers: `INSERT` into `track_high_level_binary` with `value`, `probability`, and the computed `prob_positive`.
   - For each of 6 categorical classifiers: `INSERT` into `track_high_level_categorical` with the winning class, plus N rows into `track_high_level_class_probs` for all class probabilities.
6. Commit transaction.
7. Delete the temp mp3 and JSON files.

**Notes:**
- The 30-second preview is shorter than what AcousticBrainz historically used (full tracks). Confidence values will be noisier — many P(positive) values will sit in the 0.45–0.65 range. This is expected, not a bug. Use thresholds, not equality, when querying.
- Essentia's `essentia_streaming_extractor_music` is the prebuilt static binary. Use the official Docker image if available, or build from the static binaries page on the Essentia site.
- High-level SVM model files are distributed separately from Essentia. They must be downloaded and the profile YAML must list them explicitly.

---

## 5. Pre-implementation analysis spike (BEFORE writing the worker)

Before writing the full pipeline, the agent must do a one-track manual run end-to-end and confirm assumptions. This is the highest-risk part of the project — if the Essentia output shape doesn't match expectations, the schema needs adjustment.

**Spike checklist:**

1. **Verify `tracks.track_id` is a Spotify ID.** Pull 5 random rows and confirm shape (`^[0-9A-Za-z]{22}$`).
2. **Reccobeats sanity check.** Pick one track ID. Hit `/v1/track?ids=X` and `/v1/track/{rb_id}/audio-features`. Confirm field names match §3.2.
3. **Spotify embed preview scrape.** For the same track, `GET https://open.spotify.com/embed/track/X` with a browser-like User-Agent. Apply the §4.1 regex and confirm a `https://p.scdn.co/mp3-preview/...` URL comes back. Then `GET` that URL and confirm it returns a ~500 KB mp3 with `Content-Type: audio/mpeg`. If parsing fails, save the raw HTML to `spike/embed_<track_id>.html` and adjust the regex / parsing logic in §4.1 before proceeding.
4. **Run Essentia on the preview.** Use the mp3 from step 3, run the extractor with high-level models enabled, dump the JSON.
5. **Inspect the JSON.** Specifically:
   - Confirm path to BPM: `rhythm.bpm`.
   - Confirm path to key: `tonal.key_key` and `tonal.key_scale`.
   - Confirm path to high-level outputs: `highlevel.{classifier_name}.value` and `highlevel.{classifier_name}.all` (dict of class→prob).
   - **Verify exact class label spellings** (e.g. is it `not_danceable`, `not danceable`, or `notdanceable`?). Update §3.6 if needed.
   - Confirm the `version` and `models` fields for `extractor_version` / `models_version` columns.
6. **Document any deviations** from this plan before implementing.

Output of the spike: a short markdown note with confirmed JSON paths and any schema corrections.

---

## 6. Project layout

```
charts-enrichment/
├── docker-compose.yml
├── .env                          # DB_PATH, RECCOBEATS_BASE_URL, ESSENTIA_*, SPOTIFY_EMBED_*
├── db/
│   └── charts.sqlite             # existing DB (mounted into containers)
├── migrations/
│   └── 001_audio_features.sql    # all DDL from §3
├── workers/
│   ├── stage1_spotify_previews.py
│   ├── stage2_reccobeats.py
│   ├── stage3_essentia.py
│   └── lib/
│       ├── db.py                 # connection, transaction helpers
│       ├── spotify_embed.py      # embed page fetch + audioPreview regex
│       ├── reccobeats.py         # API client
│       └── essentia_parse.py     # JSON → flat columns + classifier rows
├── essentia/
│   ├── Dockerfile                # Essentia + SVM models
│   ├── profile.yaml              # extractor profile (highlevel enabled)
│   └── svm_models/               # downloaded model files
├── scripts/
│   ├── run_migrations.py
│   ├── reset_stage.py            # clear stage N data for a track or all tracks
│   └── status.py                 # report progress: % tracks per stage
└── README.md
```

### 6.1. `.env` template

```
DB_PATH=/data/charts.sqlite
RECCOBEATS_BASE_URL=https://api.reccobeats.com/v1
ESSENTIA_PROFILE=/app/essentia/profile.yaml
ESSENTIA_MODELS_DIR=/app/essentia/svm_models
SPOTIFY_EMBED_BASE=https://open.spotify.com/embed/track
SPOTIFY_EMBED_USER_AGENT=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
WORKER_BATCH_SIZE=50
WORKER_RATE_LIMIT_QPS=1
```

> Stage 1 no longer requires Spotify Web API credentials; the embed page is public. If a future stage needs Spotify metadata (album, ISRC, release date — see §9), add `SPOTIFY_CLIENT_ID/SECRET` back at that point.

### 6.2. Docker Compose sketch

```yaml
services:
  essentia:
    build: ./essentia
    volumes:
      - ./db:/data
      - ./essentia/svm_models:/app/essentia/svm_models:ro
      - ./essentia/profile.yaml:/app/essentia/profile.yaml:ro
    # Long-lived service, polls for unprocessed tracks
    command: python /app/workers/stage3_essentia.py --loop --interval 30

  stage1:
    build: ./workers
    env_file: .env
    volumes:
      - ./db:/data
    command: python stage1_spotify_previews.py --loop --interval 3600

  stage2:
    build: ./workers
    env_file: .env
    volumes:
      - ./db:/data
    command: python stage2_reccobeats.py --loop --interval 3600
```

The three workers can run concurrently; SQLite's WAL mode handles the concurrency safely.

---

## 7. Operational details

### 7.1. SQLite concurrency

Enable WAL mode once at DB init:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
```

This is required because Stage 2 and Stage 3 will write concurrently.

### 7.2. Rate limiting

- **Spotify embed scrape (Stage 1):** non-API surface, no documented limit. Throttle to ~1 req/sec with small jitter; exponential backoff on HTTP 429 / 503. One HTTP request per track (no batching possible).
- **Reccobeats:** undocumented, but free service → throttle to ~1 req/sec, exponential backoff on 429.
- **Essentia:** local, CPU-bound. Concurrency = `min(cores - 1, 4)` to leave headroom.

### 7.3. Logging

Structured JSON logs per stage to `./logs/stage{1,2,3}.log`. Required fields:
- `ts`, `stage`, `track_id`, `event` (`'started'`, `'ok'`, `'failed'`, `'skipped'`), `duration_ms`, `error`.

### 7.4. Status / progress reporting

`scripts/status.py` should emit something like:

```
Tracks total:                  12,847
Stage 1 (Spotify preview):     11,203 ok | 1,402 no_preview | 242 failed |   0 pending
Stage 2 (Reccobeats):          12,491 ok |   289 not_found  | 67 failed  |   0 pending
Stage 3 (Essentia):             9,876 ok |             45 failed         | 1,282 pending
Fully enriched:                 9,801 (76.3%)
```

### 7.5. Re-running a stage for a single track

`scripts/reset_stage.py --track-id X --stage 3` should delete all rows for that track in the stage's tables and clear status flags. Useful when models are upgraded or data looks wrong for a specific track.

---

## 8. Quality checks (post-backfill)

Once the pipeline has run on the full catalog, the agent must verify:

1. **Coverage:** ≥ 80% of tracks fully enriched (all three stages `ok`). The embed scrape used in Stage 1 typically yields higher preview coverage than the (now-defunct) Web API path, so this target is achievable for an Italian-charts catalog. If much below, investigate Stage 1 first.
2. **No-preview rate:** `preview_status='no_preview'` should be 5–15% on a major-label-heavy catalog. If much higher, the embed regex or page structure has likely shifted — pull a few `failed`/`no_preview` track IDs, fetch their embed HTML manually, and re-validate the §4.1 regex. Note: there is no `market` parameter for the embed endpoint; preview availability is purely a function of what Spotify chose to expose for that track globally.
3. **Essentia confidence sanity:** for fully-enriched tracks, the median `prob_positive` for `danceability` should differ meaningfully between tracks Reccobeats labels high-danceability vs low-danceability. If Essentia's output is uncorrelated with Reccobeats', something is wrong with the model loading.
4. **Cross-source consistency spot check:** pick 20 random tracks. Compare:
   - Reccobeats `tempo` vs Essentia `bpm` — should match within ~2 BPM (or be 2× / 0.5× due to half-time/double-time).
   - Reccobeats `loudness` vs Essentia `loudness_ebu128` — same ballpark, may differ by a few dB.
   - Genre classifiers should be roughly plausible (Italian pop chart tracks shouldn't all classify as `classical`).

---

## 9. Out of scope (explicitly)

- **Spotify Web API as the preview source.** Considered and rejected. The Nov 27, 2024 Web API changes restricted `preview_url` in track responses for newly-registered apps and apps not in pre-existing extended mode. Empirically confirmed against this project's credentials: `GET /v1/tracks/{id}` returns the simplified Track object with `preview_url` omitted entirely, and `GET /v1/audio-features` returns HTTP 403. Stage 1 therefore uses the public embed scrape (§4.1). The Web API path is not a viable fallback and applying for "extended quota mode" is not pursued here. Document this in the README so it doesn't get re-litigated.
- **Spotify Web API as a metadata source.** Even with `preview_url` gone, `GET /v1/tracks/{id}` still returns `external_ids.isrc`, the full `artists[]` list, `album.release_date`, `explicit`, etc. Currently out of scope to keep Stage 1 auth-free, but cheap to add later as a fourth optional stage; it would populate a new `track_metadata` table without touching anything in §3.
- **AcousticBrainz integration.** Considered and rejected: data collection stopped 2022, coverage on recent Italian charts will be near-zero, requires an extra Spotify→ISRC→MBID resolution step. Document the decision in the README so it doesn't get re-litigated.
- **Model versioning / re-analysis history.** Latest overwrites by design. If versioning is needed later, it can be retrofitted via an `analysis_runs` table without touching the existing schema beyond renaming PKs.
- **Newer Essentia TensorFlow models** (Discogs-EffNet, MTG-Jamendo). The SVM-based high-level set is the documented taxonomy. TF models can be added later as additional classifier rows in the same long-format tables — the schema already supports it.
- **Embedding-based similarity.** Not needed for the chart-enrichment use case.

---

## 10. Acceptance criteria

The implementation is done when:

- [ ] All DDL from §3 applied; existing data untouched.
- [ ] All three stages run end-to-end on a 100-track sample with logs and `status.py` output captured.
- [ ] Spike (§5) findings documented; any schema deviations reflected in code.
- [ ] `v_track_features` returns a row per track in the sample with non-null fields from all three sources where applicable.
- [ ] Idempotency proven: running each stage twice on the same input is a no-op (no duplicate rows, no errors).
- [ ] Quality checks (§8) pass on the sample.
- [ ] README documents: how to run migrations, how to start each stage, how to inspect status, how to reset a track.

---

## 11. Suggested implementation order

1. **Spike — preview path (§5 step 3 only).** Hit `open.spotify.com/embed/track/{id}` for one known-good track, confirm the §4.1 regex captures a `p.scdn.co/mp3-preview/...` URL, download the mp3, confirm size/`Content-Type`. This validates the entire foundation Stage 3 stands on. **Do this before writing any DDL.**
2. **Spike — Essentia toolchain.** Pull the Essentia Docker image (or build with `--with-gaia`), run `streaming_extractor_music_svm` on the mp3 from step 1 with a profile enabling `highlevel.compute=1`, dump the JSON, and confirm it contains the expected `highlevel.*` keys. If the SVM toolchain is unworkable, this is the moment to pivot to Essentia TensorFlow models — the long-format §3 schema accommodates either.
3. **Spike — Reccobeats sanity check (§5 step 2).** Confirm field names and that the same track resolves.
4. **Migration script + DDL (§3).** Now informed by the actual JSON shape from step 2.
5. **Stage 1 worker** (embed scrape) — small surface area, no auth, easiest.
6. **Stage 2 worker (Reccobeats)** — straightforward HTTP, populates the most fields per row.
7. **Stage 3 worker (Essentia)** — most complex; needs the spike output to be solid first.
8. Status / reset scripts.
9. Compose file and README.
10. Run on 100-track sample, verify acceptance criteria (§10), then full backfill.
