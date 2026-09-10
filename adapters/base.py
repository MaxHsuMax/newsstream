"""SourceAdapter ABC + NormalizedPost (CLAUDE.md §4).

Rules every adapter must obey:
- Track by numeric account ID; refresh the handle from the ID; an ID that stops
  resolving marks the account inactive (never crash).
- Drop replies at the adapter: is_reply=True never enters the pipeline.
- Quote tweets are top-level posts with quoted_text populated.
- Record received_at; posted_at - received_at is the latency metric.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class NormalizedPost:
    source: str            # "telegram" | "x"
    source_post_id: str
    account_id: str        # STABLE numeric id, never the handle
    account_handle: str    # display only; may change
    url: str
    text: str
    quoted_text: str | None
    is_reply: bool
    posted_at: datetime    # UTC, from the platform
    received_at: datetime  # UTC, when we got it
    media_urls: list[str] = field(default_factory=list)
    # Resolved from config by the pipeline (source tier is metadata only, §5);
    # adapters may leave the default.
    tier: str = "osint"


Sink = Callable[[NormalizedPost], Awaitable[None]]


@dataclass
class AdapterStatus:
    name: str
    state: str = "starting"   # live | reconnecting | budget-paused | disabled | error | starting
    detail: str = ""


class SourceAdapter(ABC):
    status: AdapterStatus

    @abstractmethod
    async def run(self, sink: Sink) -> None:
        """Long-running task: push every new top-level post into sink."""

    @abstractmethod
    async def resolve_account(self, handle: str) -> tuple[str, str]:
        """handle -> (stable id, canonical handle)."""
