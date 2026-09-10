"""X push adapter — default vendor: TwitterAPI.io (CLAUDE.md §4, preference #2).

Chosen 2026-09-10: bills ~$0.15 per 1k delivered posts (no per-poll floor, no
monthly minimum), so cost tracks actual account activity instead of poll rate.

Vendor surface (verified against docs.twitterapi.io 2026-09-10):
- REST  https://api.twitterapi.io, auth header `X-API-Key`
    POST   /oapi/tweet_filter/add_rule     {tag, value, interval_seconds}
    POST   /oapi/tweet_filter/update_rule  {rule_id, tag, value, interval_seconds, is_effect}
    GET    /oapi/tweet_filter/get_rules
    DELETE /oapi/tweet_filter/delete_rule  {rule_id}
    GET    /twitter/user/info?userName=<handle>
- WS    wss://ws.twitterapi.io/twitter/tweet/websocket, header `x-api-key`.
  One connection per key (close code 1008 = duplicate); vendor asks for >=90s
  before reconnecting. Messages: event_type "tweet" (rule-matched, `tweets`
  array) and "fast_tweet" (low-latency lane for high-follower authors,
  single `tweet` object).

This adapter syncs filter rules from the configured account list at startup
(`from:a OR from:b` chunks <= 255 chars, tags `<rule_tag>-<i>`), then streams.
Replies are dropped client-side, and every post is filtered against the
configured handle set (fast_tweet events are not rule-attributed). Note a
handle rename silently breaks `from:` rules until the config is updated —
the official x_api adapter (ID-based) is the rename-proof fallback.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

import httpx

from newsstream.adapters.base import AdapterStatus, NormalizedPost, Sink, SourceAdapter
from newsstream.config import Settings
from newsstream.db.repo import Repo

log = logging.getLogger("newsstream.x_push")

REST_BASE = "https://api.twitterapi.io"
RULE_VALUE_MAX = 255
RECONNECT_WAIT_S = 90  # vendor requirement: let the server release the slot
LEGACY_TIME_FMT = "%a %b %d %H:%M:%S %z %Y"  # "Sat Mar 15 05:31:28 +0000 2025"


def build_rule_values(handles: list[str]) -> list[str]:
    """Chunk `from:` clauses into rule values within the 255-char limit."""
    values: list[str] = []
    current = ""
    for h in handles:
        clause = f"from:{h.lstrip('@')}"
        candidate = f"{current} OR {clause}" if current else clause
        if len(candidate) > RULE_VALUE_MAX:
            values.append(current)
            candidate = clause
        current = candidate
    if current:
        values.append(current)
    return values


def _parse_created(t: dict) -> datetime:
    if ms := t.get("created_ms"):
        return datetime.fromtimestamp(ms / 1000, UTC)
    raw = t.get("createdAt") or t.get("created_at")
    if raw:
        try:
            return datetime.strptime(str(raw), LEGACY_TIME_FMT)
        except ValueError:
            try:
                return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except ValueError:
                pass
    return datetime.now(UTC)


def parse_ws_message(
    msg: dict, allowed_handles: set[str] | None = None, *, received_at: datetime | None = None
) -> list[NormalizedPost]:
    """Pure converter for one WebSocket message -> normalized posts.
    allowed_handles (lowercased) filters to the curated list; None disables."""
    event = msg.get("event_type")
    if event == "tweet":
        tweets = msg.get("tweets") or []
    elif event == "fast_tweet":
        tweets = [msg.get("tweet") or {}]
    else:
        return []

    received = received_at or datetime.now(UTC)
    out: list[NormalizedPost] = []
    for t in tweets:
        if not t or not t.get("id"):
            continue
        if (t.get("type") or "").lower() == "reply" or t.get("isReply") or t.get("is_reply"):
            continue
        author = t.get("author") or {}
        handle = author.get("userName") or author.get("username") or t.get("screen_name") or ""
        if allowed_handles is not None and handle.lower() not in allowed_handles:
            continue
        text = t.get("text") or ""
        if not text.strip():
            continue
        quoted = t.get("quoted_tweet") or {}
        media = t.get("media") or []
        out.append(NormalizedPost(
            source="x",
            source_post_id=str(t["id"]),
            account_id=str(author.get("id") or t.get("user_id") or handle),
            account_handle=handle,
            url=f"https://x.com/{handle}/status/{t['id']}",
            text=text,
            quoted_text=quoted.get("text") or None,
            is_reply=False,
            posted_at=_parse_created(t),
            received_at=received,
            media_urls=[m if isinstance(m, str) else m.get("url", "") for m in media if m],
        ))
    return out


class XPushAdapter(SourceAdapter):
    def __init__(self, settings: Settings, repo: Repo | None = None):
        self.settings = settings
        self.repo = repo
        self.status = AdapterStatus(name="x_push")

    def _rest(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=REST_BASE, timeout=20,
            headers={"X-API-Key": self.settings.x_push_api_key},
        )

    async def resolve_account(self, handle: str) -> tuple[str, str]:
        async with self._rest() as rest:
            r = await rest.get("/twitter/user/info", params={"userName": handle.lstrip("@")})
            r.raise_for_status()
            data = r.json()["data"]
            return str(data["id"]), data["userName"]

    async def _sync_rules(self, rest: httpx.AsyncClient, handles: list[str]) -> None:
        """Idempotently make vendor-side rules mirror the configured accounts."""
        prefix = self.settings.x_push_rule_tag
        interval = self.settings.x_push_interval
        desired = build_rule_values(handles)

        r = await rest.get("/oapi/tweet_filter/get_rules")
        r.raise_for_status()
        ours = {
            rule["tag"]: rule
            for rule in r.json().get("rules") or []
            if str(rule.get("tag", "")).startswith(prefix)
        }

        for i, value in enumerate(desired):
            tag = f"{prefix}-{i}"
            existing = ours.pop(tag, None)
            if existing is None:
                resp = await rest.post(
                    "/oapi/tweet_filter/add_rule",
                    json={"tag": tag, "value": value, "interval_seconds": interval},
                )
                resp.raise_for_status()
                body = resp.json()
                if body.get("status") != "success":
                    raise RuntimeError(f"add_rule failed: {body.get('msg')}")
                rule_id = body["rule_id"]
            else:
                rule_id = existing["rule_id"]
            # New rules start inactive; update_rule also activates (is_effect=1).
            resp = await rest.post(
                "/oapi/tweet_filter/update_rule",
                json={"rule_id": rule_id, "tag": tag, "value": value,
                      "interval_seconds": interval, "is_effect": 1},
            )
            resp.raise_for_status()
            if resp.json().get("status") != "success":
                raise RuntimeError(f"update_rule failed: {resp.json().get('msg')}")

        for stale in ours.values():  # rules for accounts no longer configured
            await rest.request("DELETE", "/oapi/tweet_filter/delete_rule",
                               json={"rule_id": stale["rule_id"]})
            log.info("deleted stale x_push rule %s (%s)", stale["tag"], stale["value"])
        log.info("x_push rules synced: %d rule(s) for %d account(s)", len(desired), len(handles))

    async def run(self, sink: Sink) -> None:
        s = self.settings
        if not s.x_push_enabled or not s.x_push_api_key:
            self.status.state = "disabled"
            self.status.detail = "x_push.enabled false or X_PUSH_API_KEY unset (twitterapi.io)"
            log.info("x_push adapter idle: %s", self.status.detail)
            return
        handles = [a.handle for a in s.accounts if a.source == "x"]
        if not handles:
            self.status.state = "disabled"
            self.status.detail = "no accounts with source: x in config.yaml"
            return
        allowed = {h.lower() for h in handles}

        import websockets  # lazy: optional at runtime

        first = True
        while True:
            try:
                if not first:
                    await asyncio.sleep(RECONNECT_WAIT_S)
                first = False
                async with self._rest() as rest:
                    await self._sync_rules(rest, handles)
                async with websockets.connect(
                    s.x_push_url,
                    additional_headers={"x-api-key": s.x_push_api_key},
                    ping_interval=40, ping_timeout=30,
                ) as ws:
                    self.status.state = "live"
                    self.status.detail = f"twitterapi.io, {len(handles)} account(s)"
                    log.info("x_push live (%s)", s.x_push_url)
                    async for raw in ws:
                        try:
                            posts = parse_ws_message(json.loads(raw), allowed)
                        except (json.JSONDecodeError, KeyError, TypeError) as e:
                            log.warning("x_push unparseable message: %s", e)
                            continue
                        for post in posts:
                            post.tier = s.tier_for_handle(post.account_handle)
                            if self.repo:
                                await self.repo.upsert_account(
                                    "x", post.account_id, post.account_handle, post.tier)
                            await sink(post)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.status.state = "reconnecting"
                self.status.detail = f"{str(e)[:120]} (retry in {RECONNECT_WAIT_S}s)"
                log.warning("x_push disconnected (%s); reconnecting in %ds", e, RECONNECT_WAIT_S)
