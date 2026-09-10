"""Replay fixture posts through the live pipeline at 10x speed (CLAUDE.md §11).

    python -m newsstream.replay tests/fixtures/ [--base-url http://localhost:8000]
                                                [--speed 10] [--preserve-timestamps]

Posts go to the running server's /ingest endpoint so the real pipeline and UI
are exercised without live platform credentials. By default posted_at is
rewritten to ~20s before send time so cards look current and latency badges
are sensible; --preserve-timestamps keeps the original September 2026 times.
Gaps between posts are capped at 300s of original time, then divided by
--speed. Works offline if cassettes are recorded (NEWSSTREAM_CASSETTE on the
server process).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

MAX_GAP_S = 300.0


def load_posts(fixtures_dir: Path) -> list[dict]:
    posts: list[dict] = []
    files = sorted(fixtures_dir.glob("*.json"))
    if not files:
        sys.exit(f"no fixture .json files in {fixtures_dir}")
    for f in files:
        data = json.loads(f.read_text())
        for p in data.get("posts", []):
            p = dict(p)
            p.pop("expect", None)
            posts.append(p)
    posts.sort(key=lambda p: p["posted_at"])
    return posts


async def replay(posts: list[dict], base_url: str, speed: float, preserve_ts: bool) -> None:
    async with httpx.AsyncClient(base_url=base_url, timeout=10) as http:
        try:
            await http.get("/status")
        except httpx.HTTPError:
            sys.exit(f"cannot reach {base_url} - start the server first: uv run newsstream")

        prev_ts: datetime | None = None
        for i, p in enumerate(posts, 1):
            ts = datetime.fromisoformat(p["posted_at"])
            if prev_ts is not None:
                gap = min((ts - prev_ts).total_seconds(), MAX_GAP_S)
                await asyncio.sleep(max(0.0, gap) / speed)
            prev_ts = ts
            if not preserve_ts:
                p["posted_at"] = (datetime.now(UTC) - timedelta(seconds=20)).isoformat()
                p.pop("received_at", None)
            r = await http.post("/ingest", json=p)
            r.raise_for_status()
            print(f"[{i}/{len(posts)}] @{p['account_handle']}: {p['text'][:70]!r} -> queued")
    print("replay complete - watch the UI")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("fixtures", type=Path, help="directory of fixture .json files")
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--speed", type=float, default=10.0)
    ap.add_argument("--preserve-timestamps", action="store_true")
    args = ap.parse_args()
    asyncio.run(replay(load_posts(args.fixtures), args.base_url, args.speed, args.preserve_timestamps))


if __name__ == "__main__":
    main()
