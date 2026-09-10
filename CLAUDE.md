# Newsstream — Agentic Financial News Monitor

This file is the working spec for Claude Code. Read it fully before writing code.
When the spec and the code disagree, the spec wins unless the spec is marked OPEN.

## 1. Purpose

Trading P&L in event-driven markets (crude oil first) depends on learning about
events minutes before the wire services carry them. Those minutes are currently
won by OSINT accounts on X and Telegram, and lost by traders who have to scroll
for them. Newsstream watches a curated list of accounts, decides which posts
are *materially* relevant to the user's markets, deduplicates them into events,
and surfaces them in a local web UI within seconds of publication.

The intelligence is in three places, in priority order:

1. **Materiality classification** — is this post something the market will move on?
2. **Event deduplication** — is this the same event I already surfaced, and if so,
   does this post *change* the picture enough to surface again?
3. **Source discovery** — (v3, not v1) find accounts worth watching.

The bottleneck is not intelligence. It is **data access latency**. Every design
decision below is made in service of getting the post into the classifier fast.

## 2. v1 scope

**In scope**

- One market: crude oil futures (WTI/Brent). The set of post categories is fixed and listed in the Classification section.
- Curated account list (seed list in §5). No automatic discovery.
- Ingest from Telegram (free, push) and from X via whichever adapter the user
  configures (see §4). Adapters are pluggable behind one interface.
- Classify every new top-level post with a rubric-scored LLM call.
- Cluster into events; surface new events and material updates only.
- Local FastAPI service + single-page frontend with a stacked notification tray.
- Persist everything in SQLite so the user can audit what was and wasn't surfaced.

**Explicitly out of scope for v1**

- OS-level popups. In-window only.
- Account discovery / "agent swarm." User curates the list.
- Multiple markets. The data model supports it; the UI and prompts don't need to.
- Trustworthiness scoring. Surface regardless of source reliability; show the
  source tier as metadata only (§7).
- Mobile.

## 3. Architecture

```
 [Telegram adapter] ─┐
 [X adapter]        ─┼─► ingest queue ─► normalize ─► classifier ─► dedup/cluster ─► notify ─► SSE ─► browser
 [RSS/other]        ─┘                     │              │              │
                                           └──────── SQLite (posts, events, decisions) ────────┘
```

- **Language:** Python 3.12. `uv` for env management.
- **Service:** FastAPI + uvicorn. One process, asyncio. Adapters run as tasks.
- **Storage:** SQLite via `aiosqlite`. Single file `./data/newsstream.db`.
- **Frontend:** one `index.html` served by FastAPI. Vanilla JS. Server-Sent
  Events at `/stream`. No build step, no framework.
- **LLM:** Anthropic Messages API via the official Python SDK. Two model slots,
  configurable by env var (see §9). Do not hardcode model strings anywhere
  except `config.py`.
- **Config:** `.env` for secrets, `config.yaml` for accounts/thresholds/categories.

Directory layout:

```
newsstream/
  app.py              # FastAPI app, SSE endpoint, static serving
  config.py           # loads .env + config.yaml, exposes typed Settings
  adapters/
    base.py           # SourceAdapter ABC + NormalizedPost dataclass
    telegram.py
    x_api.py          # official X API, pay-per-use, polling
    x_push.py         # third-party push feed (WebSocket), if configured
  pipeline/
    normalize.py
    classify.py       # rubric prompt, JSON parse, retry
    dedup.py          # event clustering
    notify.py         # decides surface / suppress / update; writes to SSE bus
  db/
    schema.sql
    repo.py
  static/index.html
  tests/
    fixtures/         # real posts from Sept 2026 (see §11)
    test_classify.py
    test_dedup.py
  config.yaml
  .env.example
```

## 4. Data sources and the latency problem

Read this section before touching adapters. It is the reason the project
succeeds or fails.

### The constraint

- X's official API (as of Feb 2026) is **pay-per-use for new signups**; the
  legacy Basic/Pro tiers are closed. **Filtered stream (push) is Enterprise-only.**
  Everything self-serve is **polling**, billed per post read, with a 24h dedup
  where re-reading the same post in a UTC day is charged once.
- Therefore poll interval = latency floor, and cost scales with 1/interval.
  Eight accounts at a 30s interval is ~23k reads/day. Do the math in the README
  for the user's chosen interval before they turn it on.
- Verify current pricing at https://developer.x.com before writing cost estimates.

### Adapter strategy (ordered by preference)

1. **Telegram (`adapters/telegram.py`)** — free, push-based via Bot API long-poll
   or `telethon` (MTProto). Several seed accounts mirror to Telegram channels.
   This should be the *first* adapter built and the one used in tests.
   The user must supply the channel usernames; do not guess them.
2. **Third-party X push feed (`adapters/x_push.py`)** — WebSocket services exist
   that track N accounts for a flat monthly fee with no per-read cap. Build this
   behind the same interface. Leave the vendor pluggable; the user picks one.
3. **Official X API (`adapters/x_api.py`)** — pay-per-use polling. Build it, but
   make the poll interval and daily read budget explicit config with a hard
   stop when the budget is exhausted (log loudly, do not silently go dark).
   Use `GET /2/users/:id/tweets` with `since_id`, `exclude=replies`, and
   `expansions=referenced_tweets.id` so quote tweets arrive with quoted text.

### Adapter interface

```python
@dataclass
class NormalizedPost:
    source: str            # "telegram" | "x"
    source_post_id: str
    account_id: str        # STABLE numeric id, never the handle
    account_handle: str    # display only; may change
    url: str
    text: str
    quoted_text: str | None
    is_reply: bool
    posted_at: datetime    # UTC, from the platform
    received_at: datetime  # UTC, when we got it
    media_urls: list[str]

class SourceAdapter(ABC):
    async def run(self, sink: Callable[[NormalizedPost], Awaitable[None]]) -> None: ...
    async def resolve_account(self, handle: str) -> tuple[str, str]:  # (id, canonical_handle)
```

Rules every adapter must obey:

- **Track by numeric account ID.** Resolve handle→ID once at setup, store both,
  refresh the handle from the ID on each poll. This solves renames and lets us
  detect deletions (ID stops resolving → mark account `inactive`, notify user in
  the UI, never crash).
- **Drop replies at the adapter.** `is_reply=True` never enters the pipeline.
- **Quote tweets are top-level posts** whose `quoted_text` is populated. The
  classifier sees both. The common case is an OSINT account adding "confirmed"
  or "denied" to someone else's report — that is high-value.
- **Record `received_at`.** `posted_at - received_at` is the latency metric.
  Log it per post; show p50/p95 in the UI footer.

## 5. Seed accounts (crude oil)

Resolve these to IDs at first run and store in the DB. Handles below are the
user's initial list; do not treat them as stable.

| Handle | Notes |
|---|---|
| @FaytuksNetwork | Breaking geopolitical; also on Telegram |
| @HormuzLetter | Hormuz-specific |
| @MoloWarMonitor | Conflict monitor |
| @TankerTrackers | Tanker tracking, satellite-based; fastest on strikes at sea |
| @Osint613 | OSINT |
| @GavMcCracken | Analysis |
| @UK_MTO | UK Maritime Trade Operations — semi-official incident reports |
| @MenchOsint | OSINT |

Source tier (metadata only, never filters): `official` (UK_MTO, CENTCOM-type),
`tracker` (TankerTrackers), `osint` (the rest). Store in `config.yaml`.

## 6. Pipeline

For each `NormalizedPost` arriving from any adapter:

1. **Persist raw** to `posts` immediately (before any LLM call). Never lose a post
   to a downstream failure.
2. **Skip** if `source_post_id` already seen (adapter re-delivery).
3. **Classify** (§7). Store the full JSON decision in `decisions`.
4. If `materiality_score < threshold` → stop. (Still stored; visible in the
   "suppressed" tab of the UI for auditing.)
5. **Dedup / cluster** (§8). Outcome is one of `NEW_EVENT`, `UPDATE`, `DUPLICATE`.
6. `DUPLICATE` → stop (stored, linked to the event).
7. `NEW_EVENT` / `UPDATE` → **notify**: write a `notifications` row, publish on
   the SSE bus. Frontend renders it.

Latency budget from `received_at` to SSE publish: **≤ 4 s p95**. Classifier and
dedup calls run concurrently where possible (dedup candidates can be fetched
while classification is in flight).

## 7. Classification

### Why not "confidence %"

An LLM's self-reported "95% confident" is a stylistic output, not a calibrated
probability. Thresholding on it produces unpredictable behavior. Instead the
model scores against a **rubric the user controls**, and the threshold is on the
rubric score. The user can still set the threshold (default 70/100).

### Rubric (crude oil)

The classifier returns integer sub-scores; `materiality_score` is their weighted
sum. Weights live in `config.yaml` so the user can tune without touching prompts.

| Sub-score | Range | What it measures | Default weight |
|---|---|---|---|
| `event_not_commentary` | 0–10 | Is this a *new fact* (strike, seizure, mine, closure, statement by a principal) vs. analysis/opinion/recap? | 3.0 |
| `flow_impact` | 0–10 | Does it plausibly change physical barrels moving (Hormuz, Bab el-Mandeb, Kharg, Fujairah, Yanbu, Cushing, SPR, OPEC+)? | 3.0 |
| `primary_source` | 0–10 | Is the account reporting first-hand / from tracking data, vs. re-posting a wire? | 1.5 |
| `specificity` | 0–10 | Named vessel/location/time vs. vague "reports of" | 1.5 |
| `novelty_prior` | 0–10 | Based only on the post text, does this look like something not already widely known? (Dedup does the real check; this is a cheap prior.) | 1.0 |

`materiality_score = round(sum(sub * weight) / sum(weights) * 10)` → 0–100.

### Categories (exactly one)

`STRIKE_MILITARY` · `SHIPPING_INCIDENT` · `CHOKEPOINT_STATUS` · `OFFICIAL_STATEMENT`
· `INFRASTRUCTURE` · `SANCTIONS_POLICY` · `SUPPLY_DATA` · `DIPLOMACY` · `OTHER`

### Prompt

System prompt (store in `pipeline/prompts/classify_crude.md`; version it):

```
You are a materiality filter for a crude oil futures trader. You will be given
one social media post (and, if present, the text it quotes). Score it against
the rubric below and return ONLY a JSON object. No prose.

The trader cares about events that change physical oil flows or the perceived
risk to them in the Persian Gulf, Strait of Hormuz, Gulf of Oman, Red Sea /
Bab el-Mandeb, and at US hubs (Cushing, Gulf Coast, SPR). Statements by
principals (US, Iran, IRGC, CENTCOM, Israel, Saudi Arabia, UAE, OPEC+,
Houthis) count as events. Analysis, recaps, memes, and engagement bait do not.

Rubric: <rubric table rendered here from config>

Categories: <list>

Return:
{
  "sub_scores": {"event_not_commentary": int, "flow_impact": int,
                 "primary_source": int, "specificity": int, "novelty_prior": int},
  "category": "<one of the categories>",
  "one_line": "<≤ 20 words, what happened, for the notification card>",
  "entities": {"vessels": [], "locations": [], "actors": []},
  "reasoning": "<≤ 40 words>"
}
```

User message: the post text, quoted text (if any), account handle, source tier,
and `posted_at`. Nothing else.

### Model routing

- **Triage model** (fast, cheap) classifies every post.
- If `materiality_score` lands within ±8 of the threshold → re-run with the
  **review model** (stronger) and use its result. Log both.
- Both model IDs come from env vars (§9). Check
  https://docs.claude.com/en/docs/about-claude/models for current IDs before
  filling in `.env.example`; do not rely on memory.
- Use `max_tokens=400`, temperature 0, and parse strictly. On JSON parse failure
  retry once with "Return only JSON." appended; on second failure store
  `classification_error` and **surface the post anyway at threshold** — a
  broken classifier must fail open, not silent.

## 8. Deduplication and event clustering

"Already Surfaced" is a list of **events**, not posts. Three posts about the
same tanker strike are one event; a denial or casualty count is an update to
that event; a fourth "confirmed" repost is a duplicate.

### Data model

- `events(id, category, title, first_seen_at, last_updated_at, summary,
  entities_json, status)` — `status` in `open | stale`.
- `event_posts(event_id, post_id, role)` — `role` in `origin | update | duplicate`.
- An event goes `stale` after 24h with no updates and is no longer a dedup
  candidate (a new strike two days later is a new event even in the same place).

### Procedure

For a post that passed the threshold:

1. Fetch candidate events: `status = open`, same category OR overlapping
   `entities` (vessel name, location, actor), `last_updated_at` within 24h.
   Cap at 8 candidates, most recent first.
2. If no candidates → `NEW_EVENT`.
3. Otherwise one LLM call (review model) with the post and the candidates'
   `title + summary + entities`, returning:

```
{"decision": "NEW_EVENT" | "UPDATE" | "DUPLICATE",
 "event_id": "<candidate id or null>",
 "what_changed": "<≤ 25 words, only for UPDATE>",
 "reasoning": "<≤ 40 words>"}
```

Instruction to the model: an UPDATE is a post that changes the *picture* —
confirmation by an official source, denial, casualties, vessel identified,
scale revised, location corrected. A DUPLICATE restates what the event summary
already contains, in any wording. When in doubt between UPDATE and DUPLICATE,
choose DUPLICATE (the user has the link and can read the thread).

4. On `UPDATE`, rewrite `events.summary` (LLM, ≤ 60 words, merging the new fact)
   and bump `last_updated_at`. On `NEW_EVENT`, create the event from the
   classifier's `one_line` and `entities`.

Do not use embeddings in v1. Volume is low (tens of posts/day past threshold)
and the LLM comparison is more precise than cosine similarity on short text.
Revisit if candidate fetch exceeds ~20 per post.

## 9. Configuration

`.env` (secrets — never commit):

```
ANTHROPIC_API_KEY=
TRIAGE_MODEL=          # fill from docs.claude.com models page
REVIEW_MODEL=
TELEGRAM_API_ID=       # if using telethon
TELEGRAM_API_HASH=
TELEGRAM_BOT_TOKEN=    # if using Bot API
X_BEARER_TOKEN=        # official API, optional
X_PUSH_API_KEY=        # third-party feed, optional
```

`config.yaml` (user-editable, commit the example):

```yaml
markets:
  crude_oil:
    threshold: 70
    rubric_weights:
      event_not_commentary: 3.0
      flow_impact: 3.0
      primary_source: 1.5
      specificity: 1.5
      novelty_prior: 1.0
accounts:
  - handle: TankerTrackers
    source: x
    tier: tracker
  - handle: UK_MTO
    source: x
    tier: official
  # ...
telegram_channels: []      # user fills in
x_api:
  poll_interval_seconds: 60
  daily_read_budget: 5000  # hard stop; log and pause when hit
ui:
  max_stacked: 5
  stale_after_hours: 24
```

## 10. Frontend behavior

Single page, dark theme, no framework. Requirements:

- **Tray** at top-right stacks up to `ui.max_stacked` cards; older cards
  collapse into a "+N more" pill. Newest on top.
- Each **card**: category badge, `one_line`, account handle + tier chip,
  `posted_at` as relative time ("2m ago") *and* latency ("+38s"), link to the
  original post (opens new tab), and for UPDATEs a "what changed" line under
  the event title.
- **Click anywhere on a card** = acknowledged. It moves to the feed below and
  is removed from the tray. Keyboard: `Esc` acknowledges the top card.
- **Feed** below the tray: reverse-chronological list of all surfaced items,
  grouped by event (event title as a header, posts nested). Toggle to show
  suppressed posts with their scores — this is how the user tunes the threshold.
- **Footer**: adapter status (live / reconnecting / budget-paused), p50/p95
  latency last hour, posts seen / surfaced today, LLM spend today (estimate from
  token counts).
- **Sound**: optional single chime on `NEW_EVENT` only, off by default.
- SSE reconnects automatically; on reconnect, fetch `/notifications?since=<id>`
  to backfill anything missed.

## 11. Testing

Build `tests/fixtures/` from **real posts from the first two weeks of
September 2026** — the US strikes on Iranian tankers near Kharg (Sept 5, 8),
the Saudi VLCC strikes near Khasab (Aug 31), the Houthi Jazan refinery attack
(Sept 8), IRGC "abandon your vessels" warning (Sept 8), UKMTO incident reports.
Include, for each event: the first OSINT post, a wire-service repost hours
later, an official confirmation, a denial, and an unrelated analysis post.

Required tests:

- `test_classify.py`: each fixture post's `materiality_score` lands on the
  expected side of the threshold; category matches. Analysis/recaps score < 50.
- `test_dedup.py`: sequences replay in order and produce the expected
  `NEW_EVENT / UPDATE / DUPLICATE` labels. The wire repost must be `DUPLICATE`;
  the official confirmation and the denial must be `UPDATE`.
- `test_adapters.py`: replies are dropped; quote tweets carry `quoted_text`;
  handle change with same ID is handled; unresolvable ID marks account inactive.
- A `replay` CLI (`python -m newsstream.replay fixtures/`) that pushes fixtures
  through the live pipeline at 10× speed so the UI can be exercised without
  live credentials.

Record LLM responses for fixtures with a cassette (e.g. `vcr.py`) so the test
suite runs offline and deterministically after the first recording.

## 12. Operational rules

- Never block ingest on the LLM. Persist first, classify async.
- Every LLM call logs: model, input tokens, output tokens, latency, post id.
- If the Anthropic API errors 3× in a row, switch to **fail-open**: surface
  every top-level post from `official` and `tracker` tier accounts unclassified,
  with a visible "unfiltered" badge, until calls succeed again.
- Daily read budget hit on X polling → pause that adapter, banner in UI, keep
  Telegram running.
- Log at INFO one line per post: `received | classified score=.. cat=.. |
  dedup=.. | latency=..s`. Debug logs include prompts/responses.

## 13. Roadmap (do not build in v1)

- **v2:** multiple markets with per-market rubrics and thresholds; OS-level
  notifications; Bluesky and Truth Social adapters; user feedback buttons
  ("shouldn't have surfaced" / "missed this") logged for prompt tuning.
- **v3:** account discovery — given the seed list and the market, search for
  accounts that (a) were first to post on past surfaced events, (b) are quoted
  by seed accounts, and propose them for the user to approve. Periodic
  re-scoring of accounts by how often their posts became `origin` posts.

## 14. OPEN questions (decide with the user before building)

- Which Telegram channels mirror the seed accounts? User to supply.
- ~~Which X source for v1: official pay-per-use polling, or a third-party push
  feed?~~ **DECIDED 2026-09-10:** TwitterAPI.io push feed is the default X
  source (`adapters/x_push.py`); official X API polling is a budget-capped
  fallback (`x_api.enabled: false` until needed). Costs for both in README.
- Should `UPDATE` notifications re-enter the tray, or only annotate the
  existing feed entry? Default: re-enter tray, smaller card.

## 15. Definition of done for v1

- `uv run newsstream` starts the service; `http://localhost:8000` shows the UI.
- Telegram adapter live against at least one channel; X adapter working with
  the user's chosen source.
- Fixture replay produces the expected surfaced/suppressed set; test suite
  green offline.
- Measured p95 latency from `posted_at` to card render under 90 s on the
  Telegram path (dominated by platform delivery) and under `poll_interval + 5 s`
  on the X polling path.
