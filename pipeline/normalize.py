"""Post normalization between adapters and the pipeline."""

from __future__ import annotations

import re
from datetime import UTC

from newsstream.adapters.base import NormalizedPost
from newsstream.config import Settings

_ZERO_WIDTH = re.compile(r"[​‌‍﻿]")


def normalize(post: NormalizedPost, settings: Settings) -> NormalizedPost | None:
    """Clean and validate; returns None for posts that must not enter the pipeline."""
    if post.is_reply:  # adapters drop these already; defense in depth
        return None
    post.text = _ZERO_WIDTH.sub("", post.text or "").strip()
    if post.quoted_text:
        post.quoted_text = _ZERO_WIDTH.sub("", post.quoted_text).strip() or None
    if not post.text and not post.quoted_text:
        return None
    for attr in ("posted_at", "received_at"):
        dt = getattr(post, attr)
        if dt.tzinfo is None:
            setattr(post, attr, dt.replace(tzinfo=UTC))
    if post.tier == "osint":  # adapter default; let config override by handle
        post.tier = settings.tier_for_handle(post.account_handle)
    return post
