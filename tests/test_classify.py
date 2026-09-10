"""Classifier tests (CLAUDE.md §11): offline logic + cassette-backed fixture scoring."""

from __future__ import annotations

import pytest

from newsstream.pipeline import classify as C
from newsstream.pipeline.llm import LLMParseError, LLMUnavailable
from newsstream.pipeline.notify import Bus
from newsstream.pipeline.runner import Pipeline

from .conftest import (
    StubLLM, all_fixture_posts, classify_response, load_fixture, requires_llm, sub_scores,
)

WEIGHTS = {"event_not_commentary": 3.0, "flow_impact": 3.0, "primary_source": 1.5,
           "specificity": 1.5, "novelty_prior": 1.0}


# ---------------- offline: scoring formula and parsing ----------------

def test_score_formula_bounds():
    assert C.compute_score(sub_scores(10, 10, 10, 10, 10), WEIGHTS) == 100
    assert C.compute_score(sub_scores(0, 0, 0, 0, 0), WEIGHTS) == 0


def test_score_formula_weighted():
    # (8*3 + 7*3 + 5*1.5 + 6*1.5 + 4*1) / 10 * 10 = 65.5 -> 66
    assert C.compute_score(sub_scores(8, 7, 5, 6, 4), WEIGHTS) == 66


def test_parse_clamps_and_category_fallback():
    raw = classify_response({"event_not_commentary": 99, "flow_impact": -3}, category="NOT_A_CATEGORY")
    cls = C._parse(raw, "triage", "m", WEIGHTS)
    assert cls.sub_scores["event_not_commentary"] == 10
    assert cls.sub_scores["flow_impact"] == 0
    assert cls.sub_scores["specificity"] == 0  # missing -> 0
    assert cls.category == "OTHER"


# ---------------- offline: model routing ----------------

async def test_high_score_skips_review(settings):
    post, _ = load_fixture("kharg_strikes")[0]
    llm = StubLLM([classify_response(sub_scores(10, 10, 10, 10, 5))])  # 95, outside band
    results = await C.classify(llm, settings, post, 1)
    assert len(results) == 1 and results[0].stage == "triage"


async def test_band_score_reruns_on_review_model(settings):
    post, _ = load_fixture("kharg_strikes")[0]
    llm = StubLLM([
        classify_response(sub_scores(7, 8, 7, 7, 7)),   # 73: within 70 +/- 8
        classify_response(sub_scores(9, 9, 8, 8, 7)),   # review verdict
    ])
    results = await C.classify(llm, settings, post, 1)
    assert [r.stage for r in results] == ["triage", "review"]
    assert llm.calls[0]["model"] == settings.triage_model
    assert llm.calls[1]["model"] == settings.review_model
    # (9*3 + 9*3 + 8*1.5 + 8*1.5 + 7*1) / 10 * 10 = 85; review result is final
    assert results[-1].materiality_score == 85


async def test_parse_failure_fails_open_at_threshold(settings):
    post, _ = load_fixture("kharg_strikes")[0]
    llm = StubLLM([LLMParseError("not json")])
    results = await C.classify(llm, settings, post, 1)
    final = results[-1]
    assert final.error == "classification_error"
    assert final.materiality_score == settings.market.threshold  # surfaces at threshold


# ---------------- offline: API-down fail-open through the pipeline ----------------

async def test_three_errors_switch_to_unfiltered_for_official_tier(settings, repo):
    pipeline = Pipeline(settings, repo, StubLLM([LLMUnavailable("boom")] * 4), Bus())
    posts = load_fixture("khasab_vlcc")
    official = next(p for p, _ in posts if p.tier == "official")
    tracker = next(p for p, _ in posts if p.tier == "tracker")
    osint = next(p for p, _ in posts if p.tier == "osint")

    assert await pipeline.process(tracker) == "held-llm-down"       # error 1
    assert await pipeline.process(osint) == "held-llm-down"         # error 2
    assert await pipeline.process(official) == "UNFILTERED"         # error 3 -> fail open
    # osint stays held even in fail-open mode
    osint2 = load_fixture("noise")[0][0]
    assert await pipeline.process(osint2) == "held-llm-down"


async def test_duplicate_delivery_skipped(settings, repo):
    llm = StubLLM([classify_response(sub_scores(2, 2, 2, 2, 2))] * 2)
    pipeline = Pipeline(settings, repo, llm, Bus())
    post, _ = load_fixture("noise")[0]
    assert await pipeline.process(post) == "suppressed"
    assert await pipeline.process(post) == "seen"
    assert len(llm.calls) == 1  # no second classification


# ---------------- cassette/live: fixture posts score on the right side ----------------

@requires_llm
@pytest.mark.parametrize(
    "post,expect",
    [pytest.param(p, e, id=i) for i, p, e in all_fixture_posts()],
)
async def test_fixture_classification(settings, real_llm, post, expect):
    results = await C.classify(real_llm, settings, post, None)
    final = results[-1]
    assert final.error is None, f"classifier error: {final.error}"
    threshold = settings.market.threshold
    if expect["surfaced"]:
        assert final.materiality_score >= threshold, (
            f"expected surfaced, got {final.materiality_score} ({final.sub_scores})")
    else:
        cap = expect.get("max_score", threshold - 1)
        assert final.materiality_score <= cap, (
            f"expected <= {cap}, got {final.materiality_score} ({final.sub_scores})")
    assert final.category in expect["category_in"], (
        f"category {final.category} not in {expect['category_in']}")
