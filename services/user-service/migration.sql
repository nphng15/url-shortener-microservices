-- users table: id UUID PK DEFAULT gen_random_uuid(), email TEXT UNIQUE NOT NULL,
--   password_hash TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now()
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);