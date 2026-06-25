-- ── urls table ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS urls (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    short_code   VARCHAR(10)  UNIQUE NOT NULL,
    original_url TEXT         NOT NULL,
    user_id      UUID         NOT NULL,
    user_email   TEXT         NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ  NULL,         -- NULL means no expiry
    is_active    BOOLEAN      NOT NULL DEFAULT true
);
ALTER TABLE urls ADD COLUMN IF NOT EXISTS user_email TEXT NOT NULL DEFAULT '';
-- Redirect lookup: short_code → row. UNIQUE implies B-tree index;
-- explicit name for EXPLAIN ANALYZE verification.
CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_short_code
    ON urls(short_code);
-- Paginated user URL list, newest-first.
-- Composite index supports: WHERE user_id = $1 AND id > $2 ORDER BY created_at DESC.
CREATE INDEX IF NOT EXISTS idx_urls_user_id_created
    ON urls(user_id, created_at DESC);
-- ── outbox table ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type   TEXT         NOT NULL,     -- routing key, e.g. "url.created"
    payload      JSONB        NOT NULL,     -- full event struct serialized
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ  NULL          -- NULL = unpublished; set by worker on success
);
-- Outbox poller index: fetch only unpublished rows, oldest first.
-- Partial index omits published rows (WHERE published_at IS NULL) to stay small.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox(created_at ASC)
    WHERE published_at IS NULL;