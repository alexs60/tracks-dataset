-- PostgreSQL migration: external (non-Reccobeats) audio-feature fill
-- Idempotent. Run after 001_audio_features.pg.sql.

CREATE TABLE IF NOT EXISTS external_audio_features_raw (
    source            TEXT NOT NULL,
    spotify_id        TEXT NOT NULL,
    isrc              TEXT,
    acousticness      REAL,
    danceability      REAL,
    energy            REAL,
    instrumentalness  REAL,
    liveness          REAL,
    loudness          REAL,
    speechiness       REAL,
    tempo             REAL,
    valence           REAL,
    PRIMARY KEY (source, spotify_id)
);
CREATE INDEX IF NOT EXISTS idx_eafr_isrc ON external_audio_features_raw(source, isrc);

CREATE TABLE IF NOT EXISTS track_audio_features_external (
    track_id          TEXT PRIMARY KEY REFERENCES tracks(track_id),
    source            TEXT NOT NULL,
    matched_by        TEXT NOT NULL,
    acousticness      REAL,
    danceability      REAL,
    energy            REAL,
    instrumentalness  REAL,
    liveness          REAL,
    loudness          REAL,
    speechiness       REAL,
    tempo             REAL,
    valence           REAL,
    fetched_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tafe_source ON track_audio_features_external(source);

CREATE OR REPLACE VIEW v_track_audio_features_merged AS
SELECT
    t.track_id,
    COALESCE(rb.acousticness,     ext.acousticness)     AS acousticness,
    COALESCE(rb.danceability,     ext.danceability)     AS danceability,
    COALESCE(rb.energy,           ext.energy)           AS energy,
    COALESCE(rb.instrumentalness, ext.instrumentalness) AS instrumentalness,
    COALESCE(rb.liveness,         ext.liveness)         AS liveness,
    COALESCE(rb.loudness,         ext.loudness)         AS loudness,
    COALESCE(rb.speechiness,      ext.speechiness)      AS speechiness,
    COALESCE(rb.tempo,            ext.tempo)            AS tempo,
    COALESCE(rb.valence,          ext.valence)          AS valence,
    CASE
        WHEN rb.status = 'ok'        THEN 'reccobeats'
        WHEN ext.source IS NOT NULL  THEN ext.source
        ELSE NULL
    END AS audio_features_source
FROM tracks t
LEFT JOIN track_reccobeats rb
    ON rb.track_id = t.track_id AND rb.status = 'ok'
LEFT JOIN track_audio_features_external ext
    ON ext.track_id = t.track_id;
