"""Official X API adapter: pay-per-use polling (CLAUDE.md §4, preference #3).

- GET /2/users/:id/tweets with since_id, exclude=replies, and
  expansions=referenced_tweets.id so quote tweets arrive with quoted text.
- author_id expansion refreshes the handle from the stable ID on every poll
  that returns data (rename-safe); a 404/403 on the timeline marks the
  account inactive and notifies via adapter status. Never crashes.
- Daily read budget is a hard stop: pause polling until the next UTC day,
  log loudly, banner via status. Telegram keeps running (§12).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx

from newsstream.adapters.base import AdapterStatus, NormalizedPost, Sink, SourceAdapter
from newsstream.config import Settings
from newsstream.db.repo import Repo

log = logging.getLogger("newsstream.x_api")

BASE = "https://api.x.com/2"
TWEET_PARAMS = {
    "max_results": "25",
    "exclude": "replies",
    "tweet.fields": "created_at,referenced_tweets,text",
    "expansions": "referenced_tweets.id,author_id,attachments.media_keys",
    "user.fields": "username",
    "media.fields": "url",
}


def parse_timeline(
    payload: dict, account_id: str, fallback_handle: str,
    *, received_at: datetime | None = None,
) -> tuple[list[NormalizedPost], str]:
    """Pure converter: API payload -> (posts oldest-first, current handle)."""
    received = received_at or datetime.now(UTC)
    includes = payload.get("includes") or {}
    quoted_by_id = {t["id"]: t.get("text", "") for t in includes.get("tweets", [])}
    media_by_key = {m["media_key"]: m.get("url") for m in includes.get("media", []) if m.get("media_key")}
    handle = fallback_handle
    for u in includes.get("users", []):
        if u.get("id") == account_id:
            handle = u.get("username", fallback_handle)

    posts: list[NormalizedPost] = []
    for t in payload.get("data") or []:
        refs = t.get("referenced_tweets") or []
        if any(r.get("type") == "replied_to" for r in refs):  # belt: exclude=replies upstream
            continue
        quoted_text = None
        for r in refs:
            if r.get("type") == "quoted":
                quoted_text = quoted_by_id.get(r.get("id")) or None
        media_urls = [
            media_by_key[k] for k in (t.get("attachments") or {}).get("media_keys", [])
            if media_by_key.get(k)
        ]
        posts.append(NormalizedPost(
            source="x",
            source_post_id=t["id"],
            account_id=account_id,
            account_handle=handle,
            url=f"https://x.com/{handle}/status/{t['id']}",
            text=t.get("text", ""),
            quoted_text=quoted_text,
            is_reply=False,
            posted_at=datetime.fromisoformat(t["created_at"].replace("Z", "+00:00")),
            received_at=received,
            media_urls=media_urls,
        ))
    posts.reverse()  # API returns newest first; emit oldest first
    return posts, handle


def _seconds_to_next_utc_day() -> float:
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (tomorrow - now).total_seconds()


class XApiAdapter(SourceAdapter):
    def __init__(self, settings: Settings, repo: Repo, http: httpx.AsyncClient | None = None):
        self.settings = settings
        self.repo = repo
        self.status = AdapterStatus(name="x_api")
        self._http = http  # injectable for tests
        self._since_ids: dict[str, str] = {}

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=15,
                headers={"Authorization": f"Bearer {self.settings.x_bearer_token}"},
            )
        return self._http

    async def resolve_account(self, handle: str) -> tuple[str, str]:
        r = await self._client().get(f"{BASE}/users/by/username/{handle.lstrip('@')}")
        r.raise_for_status()
        await self.repo.add_x_reads(1)  # user reads count toward the budget too
        data = r.json()["data"]
        return data["id"], data["username"]

    async def run(self, sink: Sink) -> None:
        s = self.settings
        if not s.x_api_enabled or not s.x_bearer_token:
            self.status.state = "disabled"
            self.status.detail = "x_api.enabled false or X_BEARER_TOKEN unset"
            log.info("x_api adapter idle: %s", self.status.detail)
            return

        while True:
            reads = await self.repo.x_reads_today()
            if reads >= s.x_daily_read_budget:
                wait = _seconds_to_next_utc_day()
                self.status.state = "budget-paused"
                self.status.detail = f"{reads}/{s.x_daily_read_budget} reads used; resumes next UTC day"
                log.error(
                    "X DAILY READ BUDGET EXHAUSTED (%d/%d) - polling PAUSED for %.0f min. "
                    "Telegram keeps running.", reads, s.x_daily_read_budget, wait / 60,
                )
                await asyncio.sleep(min(wait, 3600))
                continue

            accounts = [a for a in await self.repo.get_accounts("x") if a["status"] == "active"]
            for acct in accounts:
                try:
                    await self._poll_account(acct, sink)
                except httpx.HTTPStatusError as e:
                    await self._handle_http_error(acct, e)
                except httpx.HTTPError as e:
                    self.status.state = "reconnecting"
                    self.status.detail = str(e)[:120]
                    log.warning("x poll network error for @%s: %s", acct["handle"], e)
            if self.status.state in ("starting", "reconnecting"):
                self.status.state = "live"
                self.status.detail = f"polling {len(accounts)} account(s) every {s.x_poll_interval}s"
            await asyncio.sleep(s.x_poll_interval)

    async def _poll_account(self, acct: dict, sink: Sink) -> None:
        params = dict(TWEET_PARAMS)
        since = self._since_ids.get(acct["account_id"])
        if since:
            params["since_id"] = since
        r = await self._client().get(f"{BASE}/users/{acct['account_id']}/tweets", params=params)
        if r.status_code == 429:
            reset = float(r.headers.get("x-rate-limit-reset", 0))
            wait = max(5.0, reset - datetime.now(UTC).timestamp())
            log.warning("x rate limited; sleeping %.0fs", wait)
            await asyncio.sleep(min(wait, 900))
            return
        r.raise_for_status()
        payload = r.json()

        posts, handle = parse_timeline(payload, acct["account_id"], acct["handle"])
        # Each poll bills at least one read; each returned post is a post read.
        await self.repo.add_x_reads(max(len(posts), 1))
        if handle != acct["handle"]:
            log.info("handle change: @%s -> @%s (id %s)", acct["handle"], handle, acct["account_id"])
        await self.repo.upsert_account("x", acct["account_id"], handle, acct["tier"])
        for post in posts:
            post.tier = acct["tier"]
            self._since_ids[acct["account_id"]] = max(
                self._since_ids.get(acct["account_id"], "0"), post.source_post_id, key=int
            )
            await sink(post)
        if posts:
            self.status.state = "live"

    async def _handle_http_error(self, acct: dict, e: httpx.HTTPStatusError) -> None:
        if e.response.status_code in (403, 404):
            # ID no longer resolves: deleted/suspended. Mark inactive, tell the UI.
            await self.repo.set_account_status("x", acct["account_id"], "inactive")
            self.status.detail = f"@{acct['handle']} unresolvable -> marked inactive"
            log.error("x account @%s (id %s) unresolvable (%d); marked inactive",
                      acct["handle"], acct["account_id"], e.response.status_code)
        else:
            self.status.state = "reconnecting"
            self.status.detail = f"HTTP {e.response.status_code} for @{acct['handle']}"
            log.warning("x poll error %d for @%s", e.response.status_code, acct["handle"])
