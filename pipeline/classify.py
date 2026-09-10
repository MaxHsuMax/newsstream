"""Rubric-scored materiality classification (CLAUDE.md §7).

Triage model classifies every post; a score within ±review_band of the
threshold is re-run on the review model and the review result is final.
A classifier that cannot produce JSON fails OPEN: the post is surfaced at
exactly the threshold with error="classification_error".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from newsstream.adapters.base import NormalizedPost
from newsstream.config import CATEGORIES, RUBRIC_DESCRIPTIONS, Settings
from newsstream.pipeline.llm import LLMClient, LLMParseError

log = logging.getLogger("newsstream.classify")

PROMPT_PATH = Path(__file__).parent / "prompts" / "classify_crude.md"

# max_tokens=400 per spec fits the triage model; the review slot needs headroom
# because 5-family models spend always-on thinking tokens from the same budget.
TRIAGE_MAX_TOKENS = 400
REVIEW_MAX_TOKENS = 1400


@dataclass
class Classification:
    stage: str                 # triage | review | error
    model: str
    materiality_score: int
    sub_scores: dict[str, int] = field(default_factory=dict)
    category: str = "OTHER"
    one_line: str = ""
    entities: dict = field(default_factory=lambda: {"vessels": [], "locations": [], "actors": []})
    reasoning: str = ""
    error: str | None = None


def compute_score(sub_scores: dict[str, int], weights: dict[str, float]) -> int:
    """materiality_score = round(sum(sub * weight) / sum(weights) * 10) -> 0-100."""
    total_w = sum(weights.values())
    weighted = sum(sub_scores.get(k, 0) * w for k, w in weights.items())
    return max(0, min(100, round(weighted / total_w * 10)))


def render_system(settings: Settings) -> str:
    rubric_lines = [
        f"- {name} (0-10, weight {settings.market.rubric_weights.get(name, 1.0)}): {desc}"
        for name, desc in RUBRIC_DESCRIPTIONS.items()
    ]
    template = PROMPT_PATH.read_text()
    return template.format(rubric="\n".join(rubric_lines), categories=", ".join(CATEGORIES))


def build_user(post: NormalizedPost) -> str:
    parts = [
        f"Account: @{post.account_handle} (tier: {post.tier})",
        f"Posted at: {post.posted_at.isoformat()}",
        f"Post:\n{post.text}",
    ]
    if post.quoted_text:
        parts.append(f"Quoted post:\n{post.quoted_text}")
    return "\n\n".join(parts)


def _truncate_words(text: str, n: int) -> str:
    words = text.split()
    return " ".join(words[:n]) + ("…" if len(words) > n else "")


def _parse(result: dict, stage: str, model: str, weights: dict[str, float]) -> Classification:
    subs_raw = result.get("sub_scores") or {}
    subs = {}
    for key in RUBRIC_DESCRIPTIONS:
        try:
            subs[key] = max(0, min(10, int(subs_raw.get(key, 0))))
        except (TypeError, ValueError):
            subs[key] = 0
    category = result.get("category")
    if category not in CATEGORIES:
        category = "OTHER"
    entities = result.get("entities") or {}
    clean_entities = {
        k: [str(x) for x in (entities.get(k) or []) if x]
        for k in ("vessels", "locations", "actors")
    }
    return Classification(
        stage=stage,
        model=model,
        materiality_score=compute_score(subs, weights),
        sub_scores=subs,
        category=category,
        one_line=str(result.get("one_line") or "")[:200],
        entities=clean_entities,
        reasoning=str(result.get("reasoning") or "")[:400],
    )


async def classify(
    llm: LLMClient, settings: Settings, post: NormalizedPost, post_id: int | None
) -> list[Classification]:
    """Returns 1-2 classifications; the LAST one is final. Raises LLMUnavailable."""
    system = render_system(settings)
    user = build_user(post)
    weights = settings.market.rubric_weights
    threshold = settings.market.threshold

    try:
        # Spec asks for temperature 0; the anthropic 1.x SDK removed sampling
        # params entirely, so determinism comes from cassettes in tests instead.
        raw = await llm.call_json(
            "classify_triage", settings.triage_model, system, user,
            max_tokens=TRIAGE_MAX_TOKENS, post_id=post_id,
        )
        triage = _parse(raw, "triage", settings.triage_model, weights)
    except LLMParseError:
        return [_fail_open_classification(settings.triage_model, post, threshold)]

    results = [triage]
    if abs(triage.materiality_score - threshold) <= settings.market.review_band:
        try:
            raw = await llm.call_json(
                "classify_review", settings.review_model, system, user,
                max_tokens=REVIEW_MAX_TOKENS, post_id=post_id,
                prefer={"output_config": {"effort": "low"}},
            )
            results.append(_parse(raw, "review", settings.review_model, weights))
        except LLMParseError:
            results.append(_fail_open_classification(settings.review_model, post, threshold))
        log.info(
            "review re-run | post=%s triage=%d review=%d",
            post_id, triage.materiality_score, results[-1].materiality_score,
        )
    return results


def _fail_open_classification(model: str, post: NormalizedPost, threshold: int) -> Classification:
    """A broken classifier must fail open, not silent: surface at threshold."""
    return Classification(
        stage="error",
        model=model,
        materiality_score=threshold,
        one_line=_truncate_words(post.text, 20),
        error="classification_error",
    )
