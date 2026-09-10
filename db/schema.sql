PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS accounts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,              -- "telegram" | "x"
    account_id   TEXT NOT NULL,              -- stable platform id, never the handle
    handle       TEXT NOT NULL,              -- refreshed from the id; display only
    tier         TEXT NOT NULL DEFAULT 'osint',
    status       TEXT NOT NULL DEFAULT 'active',  -- active | inactive
    created_at   TEXT NOT NULL,
    last_seen_at TEXT,
    UNIQUE (source, account_id)
);

CREATE TABLE IF NOT EXISTS posts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source         TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    account_handle TEXT NOT NULL,
    tier           TEXT NOT NULL DEFAULT 'osint',
    url            TEXT NOT NULL,
    text           TEXT NOT NULL,
    quoted_text    TEXT,
    is_reply       INTEGER NOT NULL DEFAULT 0,
    posted_at      TEXT NOT NULL,            -- UTC ISO8601, from the platform
    received_at    TEXT NOT NULL,            -- UTC ISO8601, when we got it
    media_urls     TEXT NOT NULL DEFAULT '[]',
    UNIQUE (source, source_post_id)
);
CREATE INDEX IF NOT EXISTS idx_posts_received ON posts (received_at);

-- One row per classifier run (triage and, near the threshold, review).
-- is_final marks the row whose score drove the pipeline decision.
CREATE TABLE IF NOT EXISTS decisions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id           INTEGER NOT NULL REFERENCES posts (id),
    stage             TEXT NOT NULL,          -- triage | review | unfiltered
    model             TEXT NOT NULL DEFAULT '',
    materiality_score INTEGER,
    sub_scores        TEXT,                   -- JSON
    category          TEXT,
    one_line          TEXT,
    entities          TEXT,                   -- JSON {"vessels":[],"locations":[],"actors":[]}
    reasoning         TEXT,
    error             TEXT,                   -- classification_error | llm_unavailable | null
    is_final          INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_post ON decisions (post_id);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    category        TEXT NOT NULL,
    title           TEXT NOT NULL,
    summary         TEXT NOT NULL,
    entities        TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'open',  -- open | stale
    first_seen_at   TEXT NOT NULL,
    last_updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status, last_updated_at);

CREATE TABLE IF NOT EXISTS event_posts (
    event_id INTEGER NOT NULL REFERENCES events (id),
    post_id  INTEGER NOT NULL REFERENCES posts (id),
    role     TEXT NOT NULL,                   -- origin | update | duplicate
    PRIMARY KEY (event_id, post_id)
);

CREATE TABLE IF NOT EXISTS notifications (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,                 -- NEW_EVENT | UPDATE | UNFILTERED
    post_id    INTEGER NOT NULL REFERENCES posts (id),
    event_id   INTEGER REFERENCES events (id),
    payload    TEXT NOT NULL,                 -- full card JSON as sent over SSE
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_calls (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose       TEXT NOT NULL,              -- classify_triage | classify_review | dedup | summary_merge
    model         TEXT NOT NULL,
    post_id       INTEGER,
    input_tokens  INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    latency_ms    INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_calls_created ON llm_calls (created_at);

-- Daily post-read counter for the official X API budget hard stop.
CREATE TABLE IF NOT EXISTS x_reads (
    day   TEXT PRIMARY KEY,                   -- UTC date YYYY-MM-DD
    reads INTEGER NOT NULL DEFAULT 0
);
