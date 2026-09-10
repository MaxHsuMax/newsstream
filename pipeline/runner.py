"""Pipeline orchestration (CLAUDE.md §6): persist -> classify -> dedup -> notify.

Operational rules honored here (§12):
- Persist raw before any LLM call; never lose a post to a downstream failure.
- 3+ consecutive Anthropic errors -> fail open: official/tracker posts surface
  unclassified with an "unfiltered" badge until calls succeed again.
- One INFO line per post: received | classified | dedup | latency.
- Dedup candidates are fetched concurrently with classification (latency §6).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime

from newsstream.adapters.base import NormalizedPost
from newsstream.config import Settings
from newsstream.db.repo import Repo, utcnow
from newsstream.pipeline import classify as classify_mod
from newsstream.pipeline import dedup as dedup_mod
from newsstream.pipeline.llm import LLMClient, LLMUnavailable
from newsstream.pipeline.normalize import normalize
from newsstream.pipeline.notify import Bus, build_card, notify

log = logging.getLogger("newsstream.pipeline")


class Stats:
    """Rolling latency metrics for the UI footer."""

    def __init__(self) -> None:
        self.platform: deque[tuple[float, float]] = deque(maxlen=2000)  # posted -> received
        self.surface: deque[tuple[float, float]] = deque(maxlen=2000)   # received -> published

    @staticmethod
    def _percentiles(samples: deque[tuple[float, float]]) -> dict:
        cutoff = time.time() - 3600
        vals = sorted(v for ts, v in samples if ts >= cutoff)
        if not vals:
            return {"p50": None, "p95": None}
        return {
            "p50": round(vals[len(vals) // 2], 1),
            "p95": round(vals[min(len(vals) - 1, int(len(vals) * 0.95))], 1),
        }

    def snapshot(self) -> dict:
        return {"platform": self._percentiles(self.platform), "surface": self._percentiles(self.surface)}


class Pipeline:
    def __init__(self, settings: Settings, repo: Repo, llm: LLMClient, bus: Bus):
        self.settings = settings
        self.repo = repo
        self.llm = llm
        self.bus = bus
        self.stats = Stats()
        self.queue: asyncio.Queue[NormalizedPost] = asyncio.Queue()

    async def sink(self, post: NormalizedPost) -> None:
        """Adapter-facing entry point: enqueue, never block ingest on the LLM."""
        self.queue.put_nowait(post)

    async def run(self) -> None:
        while True:
            post = await self.queue.get()
            try:
                await self.process(post)
            except Exception:
                log.exception("pipeline failure for %s:%s (post persisted, continuing)",
                              post.source, post.source_post_id)

    async def process(self, raw_post: NormalizedPost) -> str:
        post = normalize(raw_post, self.settings)
        if post is None:
            return "dropped"

        post_id = await self.repo.insert_post(post)
        if post_id is None:
            log.debug("already seen %s:%s", post.source, post.source_post_id)
            return "seen"

        t0 = time.monotonic()
        self.stats.platform.append((time.time(), (post.received_at - post.posted_at).total_seconds()))

        # Candidate prefetch runs while classification is in flight.
        events_task = asyncio.create_task(dedup_mod.fetch_open_events(self.repo, self.settings))
        try:
            classifications = await classify_mod.classify(self.llm, self.settings, post, post_id)
        except LLMUnavailable:
            events_task.cancel()
            return await self._handle_llm_down(post, post_id, t0)

        for i, c in enumerate(classifications):
            await self.repo.insert_decision(
                post_id, c.stage, c.model,
                materiality_score=c.materiality_score, sub_scores=c.sub_scores,
                category=c.category, one_line=c.one_line, entities=c.entities,
                reasoning=c.reasoning, error=c.error,
                is_final=(i == len(classifications) - 1),
            )
        final = classifications[-1]

        if final.materiality_score < self.settings.market.threshold:
            events_task.cancel()
            outcome = "suppressed"
        else:
            outcome = await self._surface(post, post_id, final, events_task)

        elapsed = time.monotonic() - t0
        if outcome in ("NEW_EVENT", "UPDATE"):
            self.stats.surface.append((time.time(), elapsed))
        log.info(
            "received %s:%s | classified score=%d cat=%s | dedup=%s | latency=%.1fs",
            post.source, post.source_post_id, final.materiality_score, final.category,
            outcome, elapsed,
        )
        return outcome

    async def _surface(
        self, post: NormalizedPost, post_id: int,
        final: classify_mod.Classification, events_task: asyncio.Task,
    ) -> str:
        open_events = await events_task
        candidates = dedup_mod.filter_candidates(open_events, final)
        try:
            result = await dedup_mod.decide(self.llm, self.settings, post, final, candidates, post_id)
        except LLMUnavailable:
            # Classification succeeded but dedup could not run: surfacing a
            # possible duplicate beats going dark.
            result = dedup_mod.DedupResult("NEW_EVENT", None, reasoning="dedup_llm_unavailable")

        badges = ["classifier-error"] if final.error else []

        if result.decision == "NEW_EVENT":
            title = final.one_line or post.text[:120]
            event_id = await self.repo.create_event(final.category, title, title, final.entities)
            await self.repo.link_event_post(event_id, post_id, "origin")
            await notify(self.repo, self.bus, build_card(
                "NEW_EVENT", post, post_id, final,
                event_id=event_id, event_title=title, badges=badges,
            ))
            return "NEW_EVENT"

        event = await self.repo.get_event(result.event_id)
        if result.decision == "UPDATE":
            summary = await dedup_mod.merge_summary(self.llm, self.settings, event, post, result, post_id)
            entities = dedup_mod.merged_entities(event["entities"], final.entities)
            await self.repo.update_event(result.event_id, summary, entities)
            await self.repo.link_event_post(result.event_id, post_id, "update")
            await notify(self.repo, self.bus, build_card(
                "UPDATE", post, post_id, final,
                event_id=result.event_id, event_title=event["title"],
                what_changed=result.what_changed, badges=badges,
            ))
            return "UPDATE"

        await self.repo.link_event_post(result.event_id, post_id, "duplicate")
        return "DUPLICATE"

    async def _handle_llm_down(self, post: NormalizedPost, post_id: int, t0: float) -> str:
        await self.repo.insert_decision(
            post_id, "unfiltered", "", error="llm_unavailable", is_final=True,
        )
        if self.llm.fail_open and post.tier in ("official", "tracker"):
            await notify(self.repo, self.bus, build_card(
                "UNFILTERED", post, post_id, None, badges=["unfiltered"],
            ))
            outcome = "UNFILTERED"
        else:
            outcome = "held-llm-down"
        log.warning(
            "received %s:%s | classify unavailable (consecutive_errors=%d) | %s | latency=%.1fs",
            post.source, post.source_post_id, self.llm.consecutive_errors, outcome,
            time.monotonic() - t0,
        )
        return outcome

    async def stale_sweeper(self) -> None:
        """Mark events stale after stale_after_hours with no updates (§8)."""
        while True:
            n = await self.repo.mark_stale_events(self.settings.stale_after_hours)
            if n:
                log.info("marked %d events stale", n)
            await asyncio.sleep(600)
