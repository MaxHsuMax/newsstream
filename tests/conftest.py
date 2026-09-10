"""Shared fixtures.

Two LLM strategies (CLAUDE.md §11):
- StubLLM: scripted responses for pipeline-logic tests; always runs offline.
- real_llm: LLMClient backed by tests/cassettes/llm.json. With
  ANTHROPIC_API_KEY set, the first run records the cassette; afterwards the
  model-behavior tests run offline and deterministically. Without key or
  cassette they skip with a clear message. Record in ONE full pytest run so
  the dedup sequences (whose prompts chain on earlier recorded outputs) stay
  consistent: ANTHROPIC_API_KEY=... uv run pytest
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pytest

from newsstream.adapters.base import NormalizedPost
from newsstream.config import ROOT, load_settings
from newsstream.db.repo import Repo
from newsstream.pipeline.llm import Cassette, LLMClient, LLMParseError, LLMUnavailable

FIXTURES_DIR = Path(__file__).parent / "fixtures"
CASSETTE_PATH = Path(__file__).parent / "cassettes" / "llm.json"

HAVE_KEY = bool(os.environ.get("ANTHROPIC_API_KEY"))
HAVE_CASSETTE = CASSETTE_PATH.exists()

requires_llm = pytest.mark.skipif(
    not (HAVE_KEY or HAVE_CASSETTE),
    reason="needs ANTHROPIC_API_KEY (records cassette) or tests/cassettes/llm.json (replays)",
)


@pytest.fixture
def settings(tmp_path):
    return load_settings(config_path=ROOT / "config.yaml", db_path=tmp_path / "test.db")


@pytest.fixture
async def repo(settings):
    r = Repo(settings.db_path)
    await r.connect()
    yield r
    await r.close()


@pytest.fixture
def real_llm(repo):
    mode = "auto" if HAVE_KEY else "replay"
    return LLMClient(
        os.environ.get("ANTHROPIC_API_KEY", ""), repo,
        cassette=Cassette(CASSETTE_PATH, mode),
    )


class StubLLM:
    """Duck-typed LLMClient: pops scripted responses; exceptions raise."""

    def __init__(self, responses: list | None = None):
        self.responses = list(responses or [])
        self.calls: list[dict] = []
        self.consecutive_errors = 0
        self.repo = None

    @property
    def fail_open(self) -> bool:
        return self.consecutive_errors >= 3

    async def call_json(self, purpose, model, system, user, **kw):
        self.calls.append({"purpose": purpose, "model": model, "user": user})
        item = self.responses.pop(0) if self.responses else {}
        if isinstance(item, Exception):
            if isinstance(item, LLMUnavailable) and not isinstance(item, LLMParseError):
                self.consecutive_errors += 1
            raise item
        self.consecutive_errors = 0
        return item


def sub_scores(e=0, f=0, p=0, s=0, n=0) -> dict:
    return {"event_not_commentary": e, "flow_impact": f, "primary_source": p,
            "specificity": s, "novelty_prior": n}


def classify_response(score_subs: dict, category="STRIKE_MILITARY", one_line="something happened",
                      entities=None) -> dict:
    return {
        "sub_scores": score_subs,
        "category": category,
        "one_line": one_line,
        "entities": entities or {"vessels": [], "locations": [], "actors": []},
        "reasoning": "stub",
    }


def fixture_post(d: dict) -> tuple[NormalizedPost, dict]:
    """Fixture JSON entry -> (NormalizedPost, expectations).
    received_at is posted_at + 30s so prompts (and cassette keys) are stable."""
    from datetime import datetime

    posted = datetime.fromisoformat(d["posted_at"])
    return (
        NormalizedPost(
            source=d["source"],
            source_post_id=d["source_post_id"],
            account_id=d["account_id"],
            account_handle=d["account_handle"],
            url=d["url"],
            text=d["text"],
            quoted_text=d.get("quoted_text"),
            is_reply=d.get("is_reply", False),
            posted_at=posted,
            received_at=posted + timedelta(seconds=30),
            media_urls=list(d.get("media_urls", [])),
            tier=d.get("tier", "osint"),
        ),
        d.get("expect", {}),
    )


def load_fixture(name: str) -> list[tuple[NormalizedPost, dict]]:
    data = json.loads((FIXTURES_DIR / f"{name}.json").read_text())
    return [fixture_post(p) for p in data["posts"]]


def all_fixture_posts() -> list[tuple[str, NormalizedPost, dict]]:
    out = []
    for f in sorted(FIXTURES_DIR.glob("*.json")):
        data = json.loads(f.read_text())
        for p in data["posts"]:
            post, expect = fixture_post(p)
            out.append((f"{data['event']}:{post.source_post_id}", post, expect))
    return out
