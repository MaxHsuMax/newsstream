"""SQLite repository. One aiosqlite connection, WAL mode, all times UTC ISO8601."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime) -> str:
    """Normalize to aware-UTC ISO8601 so string comparison == time comparison."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


class Repo:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._db: aiosqlite.Connection | None = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Repo.connect() not called"
        return self._db

    async def connect(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # -- accounts -------------------------------------------------------------

    async def upsert_account(self, source: str, account_id: str, handle: str, tier: str) -> None:
        await self.db.execute(
            """INSERT INTO accounts (source, account_id, handle, tier, status, created_at, last_seen_at)
               VALUES (?, ?, ?, ?, 'active', ?, ?)
               ON CONFLICT (source, account_id)
               DO UPDATE SET handle = excluded.handle, tier = excluded.tier,
                             status = 'active', last_seen_at = excluded.last_seen_at""",
            (source, account_id, handle, tier, iso(utcnow()), iso(utcnow())),
        )
        await self.db.commit()

    async def set_account_status(self, source: str, account_id: str, status: str) -> None:
        await self.db.execute(
            "UPDATE accounts SET status = ? WHERE source = ? AND account_id = ?",
            (status, source, account_id),
        )
        await self.db.commit()

    async def get_accounts(self, source: str | None = None) -> list[dict]:
        q = "SELECT * FROM accounts"
        args: tuple = ()
        if source:
            q += " WHERE source = ?"
            args = (source,)
        rows = await (await self.db.execute(q, args)).fetchall()
        return [dict(r) for r in rows]

    # -- posts ----------------------------------------------------------------

    async def insert_post(self, p: Any) -> int | None:
        """Persist a NormalizedPost. Returns row id, or None if already seen."""
        try:
            cur = await self.db.execute(
                """INSERT INTO posts (source, source_post_id, account_id, account_handle, tier,
                                      url, text, quoted_text, is_reply, posted_at, received_at, media_urls)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    p.source, p.source_post_id, p.account_id, p.account_handle,
                    getattr(p, "tier", "osint"), p.url, p.text, p.quoted_text,
                    int(p.is_reply), iso(p.posted_at), iso(p.received_at),
                    json.dumps(p.media_urls),
                ),
            )
        except aiosqlite.IntegrityError:
            return None
        await self.db.commit()
        return cur.lastrowid

    async def get_post(self, post_id: int) -> dict | None:
        row = await (await self.db.execute("SELECT * FROM posts WHERE id = ?", (post_id,))).fetchone()
        return dict(row) if row else None

    # -- decisions ------------------------------------------------------------

    async def insert_decision(
        self, post_id: int, stage: str, model: str, *,
        materiality_score: int | None = None, sub_scores: dict | None = None,
        category: str | None = None, one_line: str | None = None,
        entities: dict | None = None, reasoning: str | None = None,
        error: str | None = None, is_final: bool = False,
    ) -> int:
        cur = await self.db.execute(
            """INSERT INTO decisions (post_id, stage, model, materiality_score, sub_scores,
                                      category, one_line, entities, reasoning, error, is_final, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                post_id, stage, model, materiality_score,
                json.dumps(sub_scores) if sub_scores is not None else None,
                category, one_line,
                json.dumps(entities) if entities is not None else None,
                reasoning, error, int(is_final), iso(utcnow()),
            ),
        )
        await self.db.commit()
        return cur.lastrowid

    # -- events ---------------------------------------------------------------

    async def create_event(self, category: str, title: str, summary: str, entities: dict) -> int:
        now = iso(utcnow())
        cur = await self.db.execute(
            """INSERT INTO events (category, title, summary, entities, status, first_seen_at, last_updated_at)
               VALUES (?, ?, ?, ?, 'open', ?, ?)""",
            (category, title, summary, json.dumps(entities), now, now),
        )
        await self.db.commit()
        return cur.lastrowid

    async def get_event(self, event_id: int) -> dict | None:
        row = await (await self.db.execute("SELECT * FROM events WHERE id = ?", (event_id,))).fetchone()
        return dict(row) if row else None

    async def open_events_since(self, cutoff: datetime) -> list[dict]:
        rows = await (await self.db.execute(
            """SELECT * FROM events WHERE status = 'open' AND last_updated_at >= ?
               ORDER BY last_updated_at DESC""",
            (iso(cutoff),),
        )).fetchall()
        return [dict(r) for r in rows]

    async def update_event(self, event_id: int, summary: str, entities: dict | None = None) -> None:
        if entities is not None:
            await self.db.execute(
                "UPDATE events SET summary = ?, entities = ?, last_updated_at = ? WHERE id = ?",
                (summary, json.dumps(entities), iso(utcnow()), event_id),
            )
        else:
            await self.db.execute(
                "UPDATE events SET summary = ?, last_updated_at = ? WHERE id = ?",
                (summary, iso(utcnow()), event_id),
            )
        await self.db.commit()

    async def mark_stale_events(self, older_than_hours: int) -> int:
        cutoff = iso(utcnow() - timedelta(hours=older_than_hours))
        cur = await self.db.execute(
            "UPDATE events SET status = 'stale' WHERE status = 'open' AND last_updated_at < ?",
            (cutoff,),
        )
        await self.db.commit()
        return cur.rowcount

    async def link_event_post(self, event_id: int, post_id: int, role: str) -> None:
        await self.db.execute(
            "INSERT OR IGNORE INTO event_posts (event_id, post_id, role) VALUES (?, ?, ?)",
            (event_id, post_id, role),
        )
        await self.db.commit()

    # -- notifications --------------------------------------------------------

    async def insert_notification(self, kind: str, post_id: int, event_id: int | None, payload: dict) -> int:
        cur = await self.db.execute(
            "INSERT INTO notifications (kind, post_id, event_id, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (kind, post_id, event_id, json.dumps(payload), iso(utcnow())),
        )
        await self.db.commit()
        return cur.lastrowid

    async def notifications_since(self, since_id: int, limit: int = 200) -> list[dict]:
        rows = await (await self.db.execute(
            "SELECT id, kind, payload FROM notifications WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since_id, limit),
        )).fetchall()
        out = []
        for r in rows:
            payload = json.loads(r["payload"])
            payload["notification_id"] = r["id"]
            out.append(payload)
        return out

    # -- llm accounting -------------------------------------------------------

    async def insert_llm_call(
        self, purpose: str, model: str, post_id: int | None,
        input_tokens: int, output_tokens: int, latency_ms: int,
    ) -> None:
        await self.db.execute(
            """INSERT INTO llm_calls (purpose, model, post_id, input_tokens, output_tokens, latency_ms, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (purpose, model, post_id, input_tokens, output_tokens, latency_ms, iso(utcnow())),
        )
        await self.db.commit()

    async def llm_usage_today(self) -> list[dict]:
        day_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        rows = await (await self.db.execute(
            """SELECT model, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens
               FROM llm_calls WHERE created_at >= ? GROUP BY model""",
            (iso(day_start),),
        )).fetchall()
        return [dict(r) for r in rows]

    # -- x read budget --------------------------------------------------------

    async def add_x_reads(self, n: int) -> int:
        """Add n reads for today (UTC); returns the new daily total."""
        day = utcnow().strftime("%Y-%m-%d")
        await self.db.execute(
            """INSERT INTO x_reads (day, reads) VALUES (?, ?)
               ON CONFLICT (day) DO UPDATE SET reads = reads + excluded.reads""",
            (day, n),
        )
        await self.db.commit()
        row = await (await self.db.execute("SELECT reads FROM x_reads WHERE day = ?", (day,))).fetchone()
        return row["reads"]

    async def x_reads_today(self) -> int:
        day = utcnow().strftime("%Y-%m-%d")
        row = await (await self.db.execute("SELECT reads FROM x_reads WHERE day = ?", (day,))).fetchone()
        return row["reads"] if row else 0

    # -- UI queries -----------------------------------------------------------

    async def feed(self, limit: int = 100) -> list[dict]:
        """Events (newest first) with their surfaced posts nested."""
        events = await (await self.db.execute(
            "SELECT * FROM events ORDER BY last_updated_at DESC LIMIT ?", (limit,),
        )).fetchall()
        out = []
        for ev in events:
            posts = await (await self.db.execute(
                """SELECT p.*, ep.role, d.one_line, d.category, d.materiality_score
                   FROM event_posts ep
                   JOIN posts p ON p.id = ep.post_id
                   LEFT JOIN decisions d ON d.post_id = p.id AND d.is_final = 1
                   WHERE ep.event_id = ? ORDER BY p.received_at ASC""",
                (ev["id"],),
            )).fetchall()
            e = dict(ev)
            e["entities"] = json.loads(e["entities"])
            e["posts"] = [dict(p) for p in posts]
            out.append(e)
        return out

    async def suppressed(self, limit: int = 200) -> list[dict]:
        """Posts whose final decision landed below threshold, with scores for tuning."""
        rows = await (await self.db.execute(
            """SELECT p.id, p.account_handle, p.tier, p.url, p.text, p.posted_at, p.received_at,
                      d.materiality_score, d.sub_scores, d.category, d.one_line, d.error
               FROM posts p JOIN decisions d ON d.post_id = p.id AND d.is_final = 1
               WHERE p.id NOT IN (SELECT post_id FROM event_posts)
                 AND p.id NOT IN (SELECT post_id FROM notifications)
               ORDER BY p.received_at DESC LIMIT ?""",
            (limit,),
        )).fetchall()
        return [dict(r) for r in rows]

    async def counts_today(self) -> dict:
        day_start = iso(utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
        seen = (await (await self.db.execute(
            "SELECT COUNT(*) AS n FROM posts WHERE received_at >= ?", (day_start,),
        )).fetchone())["n"]
        surfaced = (await (await self.db.execute(
            "SELECT COUNT(*) AS n FROM notifications WHERE created_at >= ?", (day_start,),
        )).fetchone())["n"]
        return {"posts_seen": seen, "surfaced": surfaced}
