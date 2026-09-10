"""FastAPI service: SSE stream, UI, status, ingest. `uv run newsstream` starts it."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

from newsstream.adapters.base import NormalizedPost, SourceAdapter
from newsstream.adapters.telegram import TelegramAdapter
from newsstream.adapters.x_api import XApiAdapter
from newsstream.adapters.x_push import XPushAdapter
from newsstream.config import ROOT, Settings, load_settings
from newsstream.db.repo import Repo
from newsstream.pipeline.llm import LLMClient
from newsstream.pipeline.notify import Bus
from newsstream.pipeline.runner import Pipeline

log = logging.getLogger("newsstream.app")

INDEX_HTML = Path(__file__).parent / "static" / "index.html"


async def _guarded(adapter: SourceAdapter, coro) -> None:
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception as e:
        adapter.status.state = "error"
        adapter.status.detail = str(e)[:160]
        log.exception("adapter %s crashed", adapter.status.name)


async def _resolve_x_accounts(settings: Settings, repo: Repo, x_api: XApiAdapter) -> None:
    """Resolve configured handles to stable IDs once and store both (§4)."""
    known_handles = {a["handle"].lower() for a in await repo.get_accounts("x")}
    for acct in settings.accounts:
        if acct.source != "x" or acct.handle.lower() in known_handles:
            continue
        try:
            account_id, canonical = await x_api.resolve_account(acct.handle)
            await repo.upsert_account("x", account_id, canonical, acct.tier)
            log.info("resolved @%s -> id %s", canonical, account_id)
        except Exception as e:
            log.error("could not resolve X handle @%s: %s (renamed? update config.yaml)",
                      acct.handle, e)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    repo = Repo(settings.db_path)
    bus = Bus()
    llm = LLMClient(settings.anthropic_api_key, repo)
    pipeline = Pipeline(settings, repo, llm, bus)

    telegram = TelegramAdapter(settings)
    x_api = XApiAdapter(settings, repo)
    x_push = XPushAdapter(settings, repo)
    adapters: list[SourceAdapter] = [telegram, x_api, x_push]

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        await repo.connect()
        tasks = [
            asyncio.create_task(pipeline.run(), name="pipeline"),
            asyncio.create_task(pipeline.stale_sweeper(), name="stale-sweeper"),
        ]
        if settings.x_api_enabled and settings.x_bearer_token:
            await _resolve_x_accounts(settings, repo, x_api)
        for adapter in adapters:
            tasks.append(asyncio.create_task(
                _guarded(adapter, adapter.run(pipeline.sink)), name=adapter.status.name,
            ))
        log.info("newsstream up at http://%s:%d", settings.host, settings.port)
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await repo.close()

    app = FastAPI(title="newsstream", lifespan=lifespan)
    app.state.pipeline = pipeline
    app.state.repo = repo
    app.state.settings = settings

    @app.get("/")
    async def index():
        return FileResponse(INDEX_HTML)

    @app.get("/stream")
    async def stream(request: Request):
        async def gen():
            async with bus.subscribe() as q:
                yield ": connected\n\n"
                while not await request.is_disconnected():
                    try:
                        payload = await asyncio.wait_for(q.get(), timeout=15)
                        yield (f"id: {payload['notification_id']}\n"
                               f"event: notification\ndata: {json.dumps(payload)}\n\n")
                    except TimeoutError:
                        yield ": heartbeat\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    @app.get("/notifications")
    async def notifications(since: int = 0):
        return await repo.notifications_since(since)

    @app.get("/feed")
    async def feed():
        return {"events": await repo.feed()}

    @app.get("/suppressed")
    async def suppressed():
        return {"posts": await repo.suppressed()}

    @app.get("/status")
    async def status():
        usage = await repo.llm_usage_today()
        spend = sum(
            r["input_tokens"] / 1e6 * settings.price_for(r["model"])[0]
            + r["output_tokens"] / 1e6 * settings.price_for(r["model"])[1]
            for r in usage
        )
        inactive = [a["handle"] for a in await repo.get_accounts() if a["status"] == "inactive"]
        return {
            "adapters": [vars(a.status) for a in adapters],
            "latency": pipeline.stats.snapshot(),
            "counts": await repo.counts_today(),
            "llm_spend_today": round(spend, 2),
            "llm_fail_open": llm.fail_open,
            "x_reads_today": await repo.x_reads_today(),
            "x_daily_read_budget": settings.x_daily_read_budget,
            "threshold": settings.market.threshold,
            "max_stacked": settings.ui_max_stacked,
            "inactive_accounts": inactive,
        }

    @app.post("/ingest")
    async def ingest(body: dict):
        """Local ingestion endpoint used by `python -m newsstream.replay`."""
        try:
            post = NormalizedPost(
                source=body["source"],
                source_post_id=str(body["source_post_id"]),
                account_id=str(body.get("account_id", body.get("account_handle", ""))),
                account_handle=body["account_handle"],
                url=body.get("url", ""),
                text=body.get("text", ""),
                quoted_text=body.get("quoted_text"),
                is_reply=bool(body.get("is_reply", False)),
                posted_at=datetime.fromisoformat(body["posted_at"]),
                received_at=(datetime.fromisoformat(body["received_at"])
                             if body.get("received_at") else datetime.now(UTC)),
                media_urls=list(body.get("media_urls", [])),
                tier=body.get("tier", "osint"),
            )
        except (KeyError, ValueError) as e:
            raise HTTPException(422, f"bad post: {e}") from e
        await pipeline.sink(post)
        return {"queued": True}

    return app


def main() -> None:
    level = logging.DEBUG if os.environ.get("NEWSSTREAM_DEBUG") else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = load_settings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_level="warning")


if __name__ == "__main__":
    main()
