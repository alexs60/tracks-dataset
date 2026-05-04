-- SQLite migration: per-country totals discovered on kworb {cc}_weekly_totals pages.
-- Idempotent. Run after 002_external_audio_features.sql.
-- The legacy IT-specific columns on `tracks` (weeks_on_it, peak_it, last_chart_week)
-- are still updated by the scraper when scraping IT, so existing exports keep working.

CREATE TABLE IF NOT EXISTS track_country_totals (
    track_id        TEXT NOT NULL REFERENCES tracks(track_id),
    country         TEXT NOT NULL,
    weeks_on        INTEGER,
    peak            INTEGER,
    total_streams   INTEGER,
    last_chart_week TEXT,
    last_seen       TEXT NOT NULL,
    PRIMARY KEY (track_id, country)
);
CREATE INDEX IF NOT EXISTS idx_tct_country ON track_country_totals(country);
CREATE INDEX IF NOT EXISTS idx_tct_last_chart ON track_country_totals(last_chart_week);
