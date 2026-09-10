"""Dedup tests (CLAUDE.md §11): offline clustering logic + cassette-backed
sequence replays producing the expected NEW_EVENT / UPDATE / DUPLICATE labels."""

from __future__ import annotations

from datetime import timedelta

import pytest

from newsstream.db.repo import utcnow
from newsstream.pipeline import dedup as D
from newsstream.pipeline.classify import Classification
from newsstream.pipeline.notify import Bus
from newsstream.pipeline.runner import Pipeline

from .conftest import StubLLM, load_fixture, requires_llm


def _cls(category="STRIKE_MILITARY", vessels=(), locations=(), actors=()):
    return Classification(
        stage="triage", model="stub", materiality_score=80, category=category,
        one_line="x", entities={"vessels": list(vessels), "locations": list(locations),
                                "actors": list(actors)},
    )


def _event(eid, category="STRIKE_MILITARY", vessels=(), locations=()):
    return {"id": eid, "category": category, "title": f"ev{eid}", "summary": "s",
            "entities": {"vessels": list(vessels), "locations": list(locations), "actors": []}}


# ---------------- offline: candidate selection ----------------

def test_candidates_by_category_or_entity_overlap():
    events = [
        _event(1, category="SHIPPING_INCIDENT", vessels=["M/T Downy"]),
        _event(2, category="STRIKE_MILITARY"),                       # category match
        _event(3, category="DIPLOMACY", locations=["Kharg Island"]),  # entity match
        _event(4, category="DIPLOMACY", locations=["Vienna"]),        # no match
    ]
    cls = _cls(category="STRIKE_MILITARY", vessels=["m/t downy"], locations=["KHARG ISLAND"])
    picked = [e["id"] for e in D.filter_candidates(events, cls)]
    assert picked == [1, 2, 3]


def test_candidates_capped_at_eight():
    events = [_event(i) for i in range(20)]
    assert len(D.filter_candidates(events, _cls())) == 8


async def test_no_candidates_is_new_event_without_llm(settings):
    llm = StubLLM()
    post, _ = load_fixture("kharg_strikes")[0]
    result = await D.decide(llm, settings, post, _cls(), [], None)
    assert result.decision == "NEW_EVENT"
    assert llm.calls == []


async def test_bad_event_id_clamped_to_newest_candidate(settings):
    llm = StubLLM([{"decision": "DUPLICATE", "event_id": 999, "reasoning": "r"}])
    post, _ = load_fixture("kharg_strikes")[0]
    result = await D.decide(llm, settings, post, _cls(), [_event(5), _event(6)], None)
    assert result.decision == "DUPLICATE" and result.event_id == 5


def test_merged_entities_case_insensitive_union():
    merged = D.merged_entities(
        {"vessels": ["M/T Downy"], "locations": ["Kharg"], "actors": []},
        {"vessels": ["m/t downy", "Stark 1"], "locations": [], "actors": ["CENTCOM"]},
    )
    assert merged["vessels"] == ["M/T Downy", "Stark 1"]
    assert merged["actors"] == ["CENTCOM"]


# ---------------- offline: staleness ----------------

async def test_stale_events_leave_candidate_pool(settings, repo):
    eid = await repo.create_event("STRIKE_MILITARY", "old strike", "s", {})
    await repo.db.execute(
        "UPDATE events SET last_updated_at = ? WHERE id = ?",
        ((utcnow() - timedelta(hours=30)).isoformat(), eid),
    )
    await repo.db.commit()
    assert await D.fetch_open_events(repo, settings) == []
    assert await repo.mark_stale_events(settings.stale_after_hours) == 1
    assert (await repo.get_event(eid))["status"] == "stale"


# ---------------- cassette/live: fixture sequences replay in order ----------------

@requires_llm
@pytest.mark.parametrize("name", ["khasab_vlcc", "kharg_strikes", "jazan_refinery", "irgc_warning"])
async def test_sequence_labels(settings, repo, real_llm, name):
    pipeline = Pipeline(settings, repo, real_llm, Bus())
    outcomes, expected = [], []
    for post, expect in load_fixture(name):
        outcomes.append(await pipeline.process(post))
        expected.append(expect["dedup"] if expect["surfaced"] else "suppressed")
    assert outcomes == expected
