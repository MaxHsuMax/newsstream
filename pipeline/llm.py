"""Anthropic Messages API wrapper.

Responsibilities (CLAUDE.md §7, §12):
- strict JSON responses with one "Return only JSON." retry
- per-call logging: model, input/output tokens, latency, post id
- consecutive-error counter driving pipeline fail-open (3+ errors)
- optional cassette record/replay (NEWSSTREAM_CASSETTE=path,
  NEWSSTREAM_CASSETTE_MODE=replay|record|auto) so tests and fixture replay run
  offline and deterministically.

Model quirk handling: current 4.6+/5-family models reject `temperature` and
some older models reject `output_config`; rather than hardcoding model-family
tables, a param rejected by the API with a 400 naming it is stripped and
remembered for that model.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import anthropic

from newsstream.db.repo import Repo

log = logging.getLogger("newsstream.llm")

FAIL_OPEN_AFTER = 3


class LLMUnavailable(Exception):
    """The API errored; caller decides fail-open behavior."""


class LLMParseError(Exception):
    """The model answered but not with parseable JSON (after one retry)."""

    def __init__(self, raw: str):
        super().__init__(f"unparseable JSON: {raw[:200]!r}")
        self.raw = raw


def _extract_json(text: str) -> dict:
    s = text.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[1] if "\n" in s else s
        s = s.rsplit("```", 1)[0]
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object found")
    return json.loads(s[start : end + 1])


class Cassette:
    def __init__(self, path: Path, mode: str):
        self.path = path
        self.mode = mode  # replay | record | auto
        self.data: dict[str, dict] = {}
        if path.exists():
            self.data = json.loads(path.read_text())

    @staticmethod
    def key(model: str, system: str, user: str, max_tokens: int) -> str:
        blob = json.dumps(
            {"model": model, "system": system, "user": user, "max_tokens": max_tokens},
            sort_keys=True,
        )
        return hashlib.sha256(blob.encode()).hexdigest()[:32]

    def get(self, key: str) -> dict | None:
        return self.data.get(key)

    def put(self, key: str, value: dict) -> None:
        self.data[key] = value
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=1, sort_keys=True))


def cassette_from_env() -> Cassette | None:
    path = os.environ.get("NEWSSTREAM_CASSETTE")
    if not path:
        return None
    return Cassette(Path(path), os.environ.get("NEWSSTREAM_CASSETTE_MODE", "auto"))


class CassetteMiss(LLMUnavailable):
    pass


class _NoCredentials(Exception):
    pass


class LLMClient:
    def __init__(self, api_key: str, repo: Repo | None = None, cassette: Cassette | None = None):
        try:
            self._client: anthropic.AsyncAnthropic | None = anthropic.AsyncAnthropic(
                api_key=api_key or None
            )
        except Exception as e:  # no key, no auth profile: run in fail-open mode
            log.error("Anthropic client unavailable (%s); pipeline will fail open", e)
            self._client = None
        self.repo = repo
        self.cassette = cassette or cassette_from_env()
        self.consecutive_errors = 0
        self._rejected_params: dict[str, set[str]] = {}

    @property
    def fail_open(self) -> bool:
        return self.consecutive_errors >= FAIL_OPEN_AFTER

    async def _create_raw(
        self, model: str, system: str, user: str, max_tokens: int, prefer: dict[str, Any]
    ) -> dict:
        """One API call -> {"text", "input_tokens", "output_tokens"}, via cassette if configured."""
        key = Cassette.key(model, system, user, max_tokens)
        if self.cassette:
            hit = self.cassette.get(key)
            if hit is not None and self.cassette.mode in ("replay", "auto"):
                return hit
            if self.cassette.mode == "replay":
                raise CassetteMiss(f"cassette miss for {model} in replay mode (key {key})")

        if self._client is None:
            raise _NoCredentials("no Anthropic credentials configured")
        rejected = self._rejected_params.setdefault(model, set())
        kwargs = {k: v for k, v in prefer.items() if k not in rejected}
        while True:
            try:
                resp = await self._client.messages.create(
                    model=model,
                    system=system,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": user}],
                    **kwargs,
                )
                break
            except (anthropic.BadRequestError, TypeError) as e:
                # Server 400 naming a param (model doesn't support it) or a
                # client-side "unexpected keyword argument" (SDK dropped it).
                # The SDK also raises TypeError at request time when it can't
                # resolve any credentials; that's an availability problem.
                if isinstance(e, TypeError) and "authentication" in str(e).lower():
                    raise _NoCredentials(str(e)) from e
                culprit = next((k for k in list(kwargs) if k in str(e)), None)
                if culprit is None:
                    raise
                rejected.add(culprit)
                kwargs.pop(culprit)
                log.info("param %r not accepted for %s; retrying without it", culprit, model)

        out = {
            "text": "".join(b.text for b in resp.content if b.type == "text"),
            "input_tokens": resp.usage.input_tokens,
            "output_tokens": resp.usage.output_tokens,
        }
        if self.cassette and self.cassette.mode in ("record", "auto"):
            self.cassette.put(key, out)
        return out

    async def call_json(
        self,
        purpose: str,
        model: str,
        system: str,
        user: str,
        *,
        max_tokens: int = 400,
        post_id: int | None = None,
        prefer: dict[str, Any] | None = None,
    ) -> dict:
        """JSON-returning call. Raises LLMUnavailable (API down) or LLMParseError."""
        prefer = prefer or {}
        attempts = [user, user + "\n\nReturn only JSON."]
        raw = ""
        for attempt, msg in enumerate(attempts):
            t0 = time.monotonic()
            try:
                resp = await self._create_raw(model, system, msg, max_tokens, prefer)
            except CassetteMiss:
                raise
            except (anthropic.APIError, _NoCredentials) as e:
                self.consecutive_errors += 1
                log.warning(
                    "llm call failed | purpose=%s model=%s post=%s consecutive_errors=%d err=%s",
                    purpose, model, post_id, self.consecutive_errors, e,
                )
                raise LLMUnavailable(str(e)) from e
            latency_ms = int((time.monotonic() - t0) * 1000)
            self.consecutive_errors = 0
            log.info(
                "llm call | purpose=%s model=%s post=%s in=%d out=%d latency=%dms",
                purpose, model, post_id, resp["input_tokens"], resp["output_tokens"], latency_ms,
            )
            log.debug("llm prompt | %s", msg)
            log.debug("llm response | %s", resp["text"])
            if self.repo:
                await self.repo.insert_llm_call(
                    purpose, model, post_id, resp["input_tokens"], resp["output_tokens"], latency_ms
                )
            raw = resp["text"]
            try:
                return _extract_json(raw)
            except (ValueError, json.JSONDecodeError):
                if attempt == 0:
                    log.warning("JSON parse failure, retrying once | purpose=%s post=%s", purpose, post_id)
                    continue
        raise LLMParseError(raw)
