"""Load .env + config.yaml into a typed Settings object.

This module is the ONLY place model IDs and LLM prices are known
(CLAUDE.md §3, §9). Everything else takes them from Settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent

# Defaults if the env vars are unset. Verified against docs.claude.com 2026-09-10.
DEFAULT_TRIAGE_MODEL = "claude-haiku-4-5"
DEFAULT_REVIEW_MODEL = "claude-opus-5"

# Default X push vendor: TwitterAPI.io (decided 2026-09-10; see README costs).
DEFAULT_X_PUSH_URL = "wss://ws.twitterapi.io/twitter/tweet/websocket"

# USD per 1M tokens (input, output) for the footer spend estimate. Overridable
# via config.yaml `llm_prices`. Unknown models estimate at opus-5 rates.
DEFAULT_MODEL_PRICES: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-5": (2.00, 10.00),
    "claude-sonnet-4-6": (3.00, 15.00),
    "claude-opus-5": (5.00, 25.00),
    "claude-opus-4-8": (5.00, 25.00),
}
FALLBACK_PRICE = (5.00, 25.00)

CATEGORIES = [
    "STRIKE_MILITARY",
    "SHIPPING_INCIDENT",
    "CHOKEPOINT_STATUS",
    "OFFICIAL_STATEMENT",
    "INFRASTRUCTURE",
    "SANCTIONS_POLICY",
    "SUPPLY_DATA",
    "DIPLOMACY",
    "OTHER",
]

RUBRIC_DESCRIPTIONS = {
    "event_not_commentary": "Is this a *new fact* (strike, seizure, mine, closure, statement by a principal) vs. analysis/opinion/recap?",
    "flow_impact": "Does it plausibly change physical barrels moving (Hormuz, Bab el-Mandeb, Kharg, Fujairah, Yanbu, Cushing, SPR, OPEC+)?",
    "primary_source": "Is the account reporting first-hand / from tracking data, vs. re-posting a wire?",
    "specificity": "Named vessel/location/time vs. vague \"reports of\"",
    "novelty_prior": "Based only on the post text, does this look like something not already widely known? (Dedup does the real check; this is a cheap prior.)",
}

TIERS = ("official", "tracker", "osint")


@dataclass(frozen=True)
class AccountCfg:
    handle: str
    source: str  # "x" | "telegram"
    tier: str


@dataclass(frozen=True)
class ChannelCfg:
    channel: str
    tier: str


@dataclass(frozen=True)
class MarketCfg:
    name: str
    threshold: int
    review_band: int
    rubric_weights: dict[str, float]


@dataclass(frozen=True)
class Settings:
    anthropic_api_key: str
    triage_model: str
    review_model: str

    market: MarketCfg
    accounts: list[AccountCfg]
    telegram_channels: list[ChannelCfg]

    telegram_api_id: str
    telegram_api_hash: str
    telegram_bot_token: str

    x_bearer_token: str
    x_api_enabled: bool
    x_poll_interval: int
    x_daily_read_budget: int

    x_push_enabled: bool
    x_push_url: str
    x_push_api_key: str
    x_push_rule_tag: str
    x_push_interval: float

    ui_max_stacked: int
    stale_after_hours: int

    host: str
    port: int
    db_path: Path

    model_prices: dict[str, tuple[float, float]] = field(default_factory=dict)

    def price_for(self, model: str) -> tuple[float, float]:
        return self.model_prices.get(model, FALLBACK_PRICE)

    def tier_for_handle(self, handle: str) -> str:
        h = handle.lstrip("@").lower()
        for a in self.accounts:
            if a.handle.lower() == h:
                return a.tier
        for c in self.telegram_channels:
            if c.channel.lstrip("@").lower() == h:
                return c.tier
        return "osint"


def _channels(raw: list) -> list[ChannelCfg]:
    out = []
    for entry in raw or []:
        if isinstance(entry, str):
            out.append(ChannelCfg(channel=entry, tier="osint"))
        else:
            out.append(ChannelCfg(channel=entry["channel"], tier=entry.get("tier", "osint")))
    return out


def load_settings(
    config_path: Path | None = None,
    env_path: Path | None = None,
    db_path: Path | None = None,
) -> Settings:
    load_dotenv(env_path or ROOT / ".env")
    cfg_file = config_path or ROOT / "config.yaml"
    cfg = yaml.safe_load(cfg_file.read_text()) or {}

    market_name, market_raw = next(iter((cfg.get("markets") or {"crude_oil": {}}).items()))
    weights = market_raw.get("rubric_weights") or {}
    for key in RUBRIC_DESCRIPTIONS:
        weights.setdefault(key, 1.0)

    accounts = [
        AccountCfg(handle=a["handle"].lstrip("@"), source=a.get("source", "x"), tier=a.get("tier", "osint"))
        for a in cfg.get("accounts") or []
    ]

    x_api = cfg.get("x_api") or {}
    x_push = cfg.get("x_push") or {}
    ui = cfg.get("ui") or {}
    server = cfg.get("server") or {}

    prices = dict(DEFAULT_MODEL_PRICES)
    for model, p in (cfg.get("llm_prices") or {}).items():
        prices[model] = (float(p["input"]), float(p["output"]))

    raw_db = Path(server.get("db_path", "data/newsstream.db"))
    resolved_db = db_path or (raw_db if raw_db.is_absolute() else ROOT / raw_db)

    return Settings(
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        triage_model=os.environ.get("TRIAGE_MODEL") or DEFAULT_TRIAGE_MODEL,
        review_model=os.environ.get("REVIEW_MODEL") or DEFAULT_REVIEW_MODEL,
        market=MarketCfg(
            name=market_name,
            threshold=int(market_raw.get("threshold", 70)),
            review_band=int(market_raw.get("review_band", 8)),
            rubric_weights={k: float(v) for k, v in weights.items()},
        ),
        accounts=accounts,
        telegram_channels=_channels(cfg.get("telegram_channels")),
        telegram_api_id=os.environ.get("TELEGRAM_API_ID", ""),
        telegram_api_hash=os.environ.get("TELEGRAM_API_HASH", ""),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        x_bearer_token=os.environ.get("X_BEARER_TOKEN", ""),
        x_api_enabled=bool(x_api.get("enabled", False)),
        x_poll_interval=int(x_api.get("poll_interval_seconds", 60)),
        x_daily_read_budget=int(x_api.get("daily_read_budget", 5000)),
        x_push_enabled=bool(x_push.get("enabled", True)),
        x_push_url=str(x_push.get("url") or DEFAULT_X_PUSH_URL),
        x_push_api_key=os.environ.get("X_PUSH_API_KEY", ""),
        x_push_rule_tag=str(x_push.get("rule_tag", "newsstream")),
        x_push_interval=float(x_push.get("interval_seconds", 10)),
        ui_max_stacked=int(ui.get("max_stacked", 5)),
        stale_after_hours=int(ui.get("stale_after_hours", 24)),
        host=str(server.get("host", "127.0.0.1")),
        port=int(server.get("port", 8000)),
        db_path=resolved_db,
        model_prices=prices,
    )
