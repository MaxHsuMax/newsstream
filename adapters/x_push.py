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

Billing behavior observed live 2026-09-10 (trial account):
- ~15 credits (=$0.00015) per DELIVERED tweet, and each interval_seconds
  check redelivers the account's newest tweet, billed again. Effective cost
  ~= 86400/interval_seconds x $0.00015/day (~$0.43/day at 30 s) plus real
  volume. The pipeline dedups redeliveries by post id, so no LLM cost.
- First activation delivers a backlog (posts up to ~2 h old) in one burst.
- Free tier REST limit: 1 request / 5 s (handled by _call).
- Rules are deactivated on shutdown (matching appears to bill even with no
  listener connected) and re-armed by the next startup's rule sync.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import httpx

from newsstream.adapters.base import AdapterStatus, NormalizedPost, Sink, SourceAdapter
from newsstream.config import Settings
from newsstream.db.repo import Repo

log = logging.getLogger("newsstream.x_push")

REST_BASE = "https://api.twitterapi.io"
RULE_VALUE_MAX = 255
RECONNECT_WAIT_S = 90  # vendor requirement: let the server release the slot
LEGACY_TIME_FMT = "%a %b %d %H:%M:%S %z %Y"  # "Sat Mar 15 05:31:28 +0000 2025"
BACKFILL_WINDOW_H = 3  # only recover posts this recent on (re)connect


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

    @staticmethod
    async def _call(rest: httpx.AsyncClient, method: str, path: str, **kw) -> httpx.Response:
        """REST call with 429 pacing: the free tier allows 1 request / 5 s."""
        r = await rest.request(method, path, **kw)
        for _ in range(3):
            if r.status_code != 429:
                break
            await asyncio.sleep(5.5)
            r = await rest.request(method, path, **kw)
        return r

    async def resolve_account(self, handle: str) -> tuple[str, str]:
        async with self._rest() as rest:
            r = await self._call(rest, "GET", "/twitter/user/info",
                                 params={"userName": handle.lstrip("@")})
            r.raise_for_status()
            data = r.json()["data"]
            return str(data["id"]), data["userName"]

    async def _sync_rules(self, rest: httpx.AsyncClient, handles: list[str]) -> None:
        """Idempotently make vendor-side rules mirror the configured accounts."""
        prefix = self.settings.x_push_rule_tag
        interval = self.settings.x_push_interval
        desired = build_rule_values(handles)

        r = await self._call(rest, "GET", "/oapi/tweet_filter/get_rules")
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
                resp = await self._call(
                    rest, "POST", "/oapi/tweet_filter/add_rule",
                    json={"tag": tag, "value": value, "interval_seconds": interval},
                )
                resp.raise_for_status()
                body = resp.json()
                if body.get("status") != "success":
                    raise RuntimeError(f"add_rule failed: {body.get('msg')}")
                rule_id = body["rule_id"]
            elif (existing.get("value") == value
                  and float(existing.get("interval_seconds") or 0) == float(interval)
                  and existing.get("is_effect") == 1):
                # Nothing to change. Never touch an active up-to-date rule:
                # the vendor rejects update_rule on active rules ("update rule
                # failed"), which crash-looped every reconnect on 2026-09-10.
                continue
            else:
                rule_id = existing["rule_id"]
            # New rules start inactive; update_rule also activates (is_effect=1).
            resp = await self._call(
                rest, "POST", "/oapi/tweet_filter/update_rule",
                json={"rule_id": rule_id, "tag": tag, "value": value,
                      "interval_seconds": interval, "is_effect": 1},
            )
            resp.raise_for_status()
            if resp.json().get("status") != "success":
                raise RuntimeError(f"update_rule failed: {resp.json().get('msg')}")

        for stale in ours.values():  # rules for accounts no longer configured
            await self._call(rest, "DELETE", "/oapi/tweet_filter/delete_rule",
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
        try:
            while True:
                try:
                    if not first:
                        await asyncio.sleep(RECONNECT_WAIT_S)
                    first = False
                    async with self._rest() as rest:
                        try:
                            await self._sync_rules(rest, handles)
                        except Exception as e:
                            # Rules from a previous run are usually still armed;
                            # a sync hiccup must not block streaming.
                            log.error("x_push rule sync failed (%s); streaming with "
                                      "existing vendor-side rules", e)
                        async with websockets.connect(
                            s.x_push_url,
                            additional_headers={"x-api-key": s.x_push_api_key},
                            ping_interval=40, ping_timeout=30,
                        ) as ws:
                            self.status.state = "live"
                            self.status.detail = f"twitterapi.io, {len(handles)} account(s)"
                            log.info("x_push live (%s)", s.x_push_url)
                            try:
                                await self._backfill(rest, handles, allowed, sink)
                            except Exception as e:
                                log.warning("x_push backfill failed: %s", e)
                            async for raw in ws:
                                try:
                                    posts = parse_ws_message(json.loads(raw), allowed)
                                except (json.JSONDecodeError, KeyError, TypeError) as e:
                                    log.warning("x_push unparseable message: %s", e)
                                    continue
                                await self._emit(posts, sink)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.status.state = "reconnecting"
                    self.status.detail = f"{str(e)[:120]} (retry in {RECONNECT_WAIT_S}s)"
                    log.warning("x_push disconnected (%s); reconnecting in %ds", e, RECONNECT_WAIT_S)
        except asyncio.CancelledError:
            await self._deactivate_rules()
            raise

    async def _emit(self, posts: list[NormalizedPost], sink: Sink) -> None:
        for post in posts:
            post.tier = self.settings.tier_for_handle(post.account_handle)
            if self.repo:
                await self.repo.upsert_account(
                    "x", post.account_id, post.account_handle, post.tier)
            await sink(post)

    async def _backfill(self, rest: httpx.AsyncClient, handles: list[str],
                        allowed: set[str], sink: Sink) -> None:
        """Fetch each account's recent posts over REST after (re)connecting.
        The WebSocket has no replay, so posts published while disconnected
        would otherwise be lost (happened live 2026-09-10: a 25-min reconnect
        loop silently dropped a post). Downstream dedup skips already-seen ids;
        the window keeps a cold start from flooding the queue with old posts
        (last_tweets returns ~20/account regardless of age)."""
        cutoff = datetime.now(UTC) - timedelta(hours=BACKFILL_WINDOW_H)
        pushed = 0
        for handle in handles:
            r = await self._call(rest, "GET", "/twitter/user/last_tweets",
                                 params={"userName": handle})
            if r.status_code != 200:
                log.warning("x_push backfill: last_tweets %s -> %d", handle, r.status_code)
                continue
            body = r.json()
            tweets = body.get("tweets") or (body.get("data") or {}).get("tweets") or []
            posts = parse_ws_message({"event_type": "tweet", "tweets": tweets}, allowed)
            recent = [p for p in posts if p.posted_at >= cutoff]
            await self._emit(recent, sink)
            pushed += len(recent)
        log.info("x_push backfill: %d recent post(s) pushed (dedup drops the already-seen)", pushed)

    async def _deactivate_rules(self) -> None:
        """Best-effort on shutdown: matched tweets bill even with no listener
        connected, so stop our rules; the next startup's sync re-arms them."""
        try:
            async with asyncio.timeout(25):
                async with self._rest() as rest:
                    r = await self._call(rest, "GET", "/oapi/tweet_filter/get_rules")
                    for rule in r.json().get("rules") or []:
                        if not str(rule.get("tag", "")).startswith(self.settings.x_push_rule_tag):
                            continue
                        await self._call(
                            rest, "POST", "/oapi/tweet_filter/update_rule",
                            json={"rule_id": rule["rule_id"], "tag": rule["tag"],
                                  "value": rule["value"],
                                  "interval_seconds": rule.get("interval_seconds",
                                                               self.settings.x_push_interval),
                                  "is_effect": 0},
                        )
                        log.info("deactivated x_push rule %s", rule["tag"])
        except Exception as e:
            log.warning("could not deactivate x_push rules on shutdown: %s "
                        "(they keep billing until the next run or manual delete)", e)
