"""Notification cards + in-process SSE bus (CLAUDE.md §6 step 7, §10)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from newsstream.adapters.base import NormalizedPost
from newsstream.db.repo import Repo, iso
from newsstream.pipeline.classify import Classification

log = logging.getLogger("newsstream.notify")


class Bus:
    """Fan-out of notification payloads to connected SSE clients."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()

    def publish(self, payload: dict) -> None:
        for q in list(self._subscribers):
            q.put_nowait(payload)

    @contextlib.asynccontextmanager
    async def subscribe(self) -> AsyncIterator[asyncio.Queue]:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(q)
        try:
            yield q
        finally:
            self._subscribers.discard(q)


def build_card(
    kind: str,
    post: NormalizedPost,
    post_id: int,
    cls: Classification | None,
    *,
    event_id: int | None = None,
    event_title: str = "",
    what_changed: str = "",
    badges: list[str] | None = None,
) -> dict:
    platform_latency = (post.received_at - post.posted_at).total_seconds()
    return {
        "kind": kind,
        "post_id": post_id,
        "category": cls.category if cls else None,
        "one_line": (cls.one_line if cls else "") or post.text[:140],
        "score": cls.materiality_score if cls else None,
        "handle": post.account_handle,
        "tier": post.tier,
        "url": post.url,
        "posted_at": iso(post.posted_at),
        "received_at": iso(post.received_at),
        "platform_latency_s": round(platform_latency, 1),
        "event_id": event_id,
        "event_title": event_title,
        "what_changed": what_changed,
        "badges": badges or [],
    }


async def notify(repo: Repo, bus: Bus, card: dict) -> int:
    nid = await repo.insert_notification(card["kind"], card["post_id"], card.get("event_id"), card)
    card["notification_id"] = nid
    bus.publish(card)
    log.info("surfaced | kind=%s post=%s event=%s", card["kind"], card["post_id"], card.get("event_id"))
    return nid
