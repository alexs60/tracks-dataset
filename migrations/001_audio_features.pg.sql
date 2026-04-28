-- PostgreSQL migration: audio feature tables
-- Assumes the `tracks` table already exists and is populated.
-- Safe to run multiple times (idempotent).

-- Add preview columns to the existing tracks table
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS preview_url     TEXT;
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS preview_fetched TEXT;
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS preview_status  TEXT;

CREATE TABLE IF NOT EXISTS track_reccobeats (
    track_id            TEXT PRIMARY KEY REFERENCES tracks(track_id),
    reccobeats_id       TEXT,
    isrc                TEXT,
    ean                 TEXT,
    upc                 TEXT,
    duration_ms         INTEGER,
    available_countries TEXT,
    acousticness        REAL,
    danceability        REAL,
    energy              REAL,
    instrumentalness    REAL,
    liveness            REAL,
    loudness            REAL,
    speechiness         REAL,
    tempo               REAL,
    valence             REAL,
    fetched_at          TEXT NOT NULL,
    status              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reccobeats_isrc ON track_reccobeats(isrc);

CREATE TABLE IF NOT EXISTS track_analysis (
    track_id            TEXT PRIMARY KEY REFERENCES tracks(track_id),
    analyzed_at         TEXT NOT NULL,
    extractor_version   TEXT NOT NULL,
    models_version      TEXT NOT NULL,
    status              TEXT NOT NULL,
    error               TEXT,
    raw_json            TEXT,
    bpm                 REAL,
    bpm_confidence      REAL,
    danceability_raw    REAL,
    loudness_ebu128     REAL,
    average_loudness    REAL,
    dynamic_complexity  REAL,
    key_key             TEXT,
    key_scale           TEXT,
    key_strength        REAL,
    chords_changes_rate REAL,
    tuning_frequency    REAL,
    onset_rate          REAL,
    duration_sec        REAL
);
CREATE INDEX IF NOT EXISTS idx_ta_bpm ON track_analysis(bpm);
CREATE INDEX IF NOT EXISTS idx_ta_key ON track_analysis(key_key, key_scale);

CREATE TABLE IF NOT EXISTS track_high_level_binary (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,
    value           TEXT NOT NULL,
    probability     REAL NOT NULL,
    prob_positive   REAL NOT NULL,
    PRIMARY KEY (track_id, classifier)
);
CREATE INDEX IF NOT EXISTS idx_thlb_clf_pos
    ON track_high_level_binary(classifier, prob_positive DESC);

CREATE TABLE IF NOT EXISTS track_high_level_categorical (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,
    value           TEXT NOT NULL,
    probability     REAL NOT NULL,
    PRIMARY KEY (track_id, classifier)
);

CREATE TABLE IF NOT EXISTS track_high_level_class_probs (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    classifier      TEXT NOT NULL,
    class_label     TEXT NOT NULL,
    probability     REAL NOT NULL,
    PRIMARY KEY (track_id, classifier, class_label)
);
CREATE INDEX IF NOT EXISTS idx_thlcp_lookup
    ON track_high_level_class_probs(classifier, class_label, probability DESC);

CREATE OR REPLACE VIEW v_track_features AS
SELECT
    t.track_id, t.title, t.artist,
    rb.acousticness, rb.danceability AS rb_danceability, rb.energy,
    rb.instrumentalness, rb.liveness, rb.loudness AS rb_loudness,
    rb.speechiness, rb.tempo AS rb_tempo, rb.valence,
    rb.isrc, rb.duration_ms,
    a.bpm, a.key_key, a.key_scale, a.loudness_ebu128,
    a.danceability_raw, a.duration_sec,
    MAX(CASE WHEN b.classifier='danceability' THEN b.prob_positive END) AS p_danceable,
    MAX(CASE WHEN b.classifier='mood_happy' THEN b.prob_positive END) AS p_happy,
    MAX(CASE WHEN b.classifier='mood_sad' THEN b.prob_positive END) AS p_sad,
    MAX(CASE WHEN b.classifier='mood_party' THEN b.prob_positive END) AS p_party,
    MAX(CASE WHEN b.classifier='mood_aggressive' THEN b.prob_positive END) AS p_aggressive,
    MAX(CASE WHEN b.classifier='mood_relaxed' THEN b.prob_positive END) AS p_relaxed,
    MAX(CASE WHEN b.classifier='mood_acoustic' THEN b.prob_positive END) AS p_acoustic,
    MAX(CASE WHEN b.classifier='mood_electronic' THEN b.prob_positive END) AS p_electronic,
    MAX(CASE WHEN b.classifier='timbre' THEN b.prob_positive END) AS p_dark,
    MAX(CASE WHEN b.classifier='tonal_atonal' THEN b.prob_positive END) AS p_tonal,
    MAX(CASE WHEN b.classifier='voice_instrumental' THEN b.prob_positive END) AS p_voice,
    MAX(CASE WHEN b.classifier='gender' THEN b.prob_positive END) AS p_female,
    MAX(CASE WHEN c.classifier='genre_dortmund' THEN c.value END) AS genre_dortmund,
    MAX(CASE WHEN c.classifier='genre_tzanetakis' THEN c.value END) AS genre_tzanetakis,
    MAX(CASE WHEN c.classifier='genre_rosamerica' THEN c.value END) AS genre_rosamerica,
    MAX(CASE WHEN c.classifier='genre_electronic' THEN c.value END) AS genre_electronic,
    MAX(CASE WHEN c.classifier='moods_mirex' THEN c.value END) AS mirex_cluster,
    MAX(CASE WHEN c.classifier='rhythm_ismir04' THEN c.value END) AS rhythm
FROM tracks t
LEFT JOIN track_reccobeats rb ON rb.track_id = t.track_id
LEFT JOIN track_analysis a ON a.track_id = t.track_id
LEFT JOIN track_high_level_binary b ON b.track_id = t.track_id
LEFT JOIN track_high_level_categorical c ON c.track_id = t.track_id
GROUP BY t.track_id;
