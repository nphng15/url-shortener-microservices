CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS clicks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    short_code TEXT NOT NULL,
    clicked_at TIMESTAMPTZ NOT NULL,
    ip_hash TEXT NOT NULL,
    user_agent TEXT NOT NULL DEFAULT '',
    referer TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_clicks_short_code_time
    ON clicks(short_code, clicked_at DESC);

CREATE INDEX IF NOT EXISTS idx_clicks_referer
    ON clicks(short_code, referer)
    WHERE referer IS NOT NULL;

CREATE TABLE IF NOT EXISTS milestones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    short_code TEXT NOT NULL,
    milestone INT NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(short_code, milestone)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_milestones_code_milestone
    ON milestones(short_code, milestone);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
