"""Adapter tests (CLAUDE.md §11): replies dropped, quote text carried,
handle change on stable ID handled, unresolvable ID marks account inactive."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import httpx

from newsstream.adapters.telegram import bare_chat_id, bot_update_to_post, message_to_post
from newsstream.adapters.x_api import XApiAdapter, parse_timeline
from newsstream.adapters.x_push import (
    RULE_VALUE_MAX, XPushAdapter, build_rule_values, parse_ws_message,
)
from newsstream.pipeline.normalize import normalize

from .conftest import load_fixture

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


# ---------------- X timeline parsing ----------------

TIMELINE = {
    "data": [
        {"id": "300", "text": "newest plain post", "created_at": "2026-09-10T11:59:00.000Z"},
        {"id": "200", "text": "confirmed.", "created_at": "2026-09-10T11:50:00.000Z",
         "referenced_tweets": [{"type": "quoted", "id": "150"}]},
        {"id": "100", "text": "@someone i agree", "created_at": "2026-09-10T11:40:00.000Z",
         "referenced_tweets": [{"type": "replied_to", "id": "90"}]},
    ],
    "includes": {
        "tweets": [{"id": "150", "text": "original report: tanker struck off Kharg"}],
        "users": [{"id": "89234502", "username": "TankerTrackers_new"}],
    },
}


def test_replies_dropped_and_quotes_carried():
    posts, handle = parse_timeline(TIMELINE, "89234502", "TankerTrackers", received_at=NOW)
    assert [p.source_post_id for p in posts] == ["200", "300"]  # reply gone, oldest first
    quoted = posts[0]
    assert quoted.quoted_text == "original report: tanker struck off Kharg"
    assert posts[1].quoted_text is None
    assert all(not p.is_reply for p in posts)


def test_handle_refreshed_from_stable_id():
    posts, handle = parse_timeline(TIMELINE, "89234502", "TankerTrackers", received_at=NOW)
    assert handle == "TankerTrackers_new"           # rename picked up from the ID
    assert all(p.account_id == "89234502" for p in posts)
    assert "TankerTrackers_new" in posts[0].url


async def test_handle_change_same_id_is_one_account(repo):
    await repo.upsert_account("x", "89234502", "TankerTrackers", "tracker")
    await repo.upsert_account("x", "89234502", "TankerTrackers_new", "tracker")
    accounts = await repo.get_accounts("x")
    assert len(accounts) == 1
    assert accounts[0]["handle"] == "TankerTrackers_new"


async def test_unresolvable_id_marks_account_inactive(settings, repo):
    await repo.upsert_account("x", "89234502", "TankerTrackers", "tracker")
    adapter = XApiAdapter(settings, repo)
    req = httpx.Request("GET", "https://api.x.com/2/users/89234502/tweets")
    err = httpx.HTTPStatusError("gone", request=req, response=httpx.Response(404, request=req))
    acct = (await repo.get_accounts("x"))[0]
    await adapter._handle_http_error(acct, err)
    assert (await repo.get_accounts("x"))[0]["status"] == "inactive"
    assert "inactive" in adapter.status.detail


# ---------------- Telegram converters ----------------

def _tg_msg(text="mines reported near Hormuz TSS", reply_to=None):
    return SimpleNamespace(id=42, message=text, reply_to=reply_to,
                           date=datetime(2026, 9, 10, 11, 58, tzinfo=UTC))


def test_telegram_message_converted():
    post = message_to_post(_tg_msg(), 12345, "faytuks", received_at=NOW)
    assert post.source == "telegram"
    assert post.source_post_id == "12345:42"
    assert post.account_id == "12345"
    assert post.url == "https://t.me/faytuks/42"
    assert post.posted_at.tzinfo is not None


def test_telegram_reply_dropped():
    assert message_to_post(_tg_msg(reply_to=object()), 12345, "faytuks") is None


def test_bot_update_converted_and_reply_dropped():
    update = {"update_id": 1, "channel_post": {
        "message_id": 7, "date": 1789500000, "text": "UKMTO advisory 032-26",
        "chat": {"id": -1001234, "username": "ukmto_mirror"}}}
    post = bot_update_to_post(update, received_at=NOW)
    assert post.source_post_id == "-1001234:7"
    assert post.posted_at == datetime.fromtimestamp(1789500000, UTC)
    update["channel_post"]["reply_to_message"] = {"message_id": 3}
    assert bot_update_to_post(update) is None


def test_bare_chat_id():
    assert bare_chat_id(-1001234567) == 1234567
    assert bare_chat_id(555) == 555


# ---------------- X push (TwitterAPI.io) parsing ----------------

ALLOWED = {"uk_mto", "tankertrackers", "faytuksnetwork"}


def test_x_push_rule_matched_event_parsed():
    msg = {
        "event_type": "tweet", "rule_id": "r1", "rule_tag": "newsstream-0",
        "tweets": [
            {"id": "555", "text": "INCIDENT 033-26: vessel reports being hailed",
             "author": {"id": "717836935", "userName": "UK_MTO", "name": "UKMTO"},
             "createdAt": "Thu Sep 10 11:22:33 +0000 2026",
             "quoted_tweet": {"id": "500", "text": "original advisory"}},
            {"id": "556", "text": "reply text", "isReply": True,
             "author": {"id": "1", "userName": "UK_MTO"},
             "createdAt": "Thu Sep 10 11:23:00 +0000 2026"},
        ],
    }
    posts = parse_ws_message(msg, ALLOWED, received_at=NOW)
    assert len(posts) == 1  # reply dropped
    p = posts[0]
    assert (p.account_id, p.account_handle) == ("717836935", "UK_MTO")
    assert p.quoted_text == "original advisory"
    assert p.posted_at == datetime(2026, 9, 10, 11, 22, 33, tzinfo=UTC)
    assert p.url == "https://x.com/UK_MTO/status/555"


def test_x_push_fast_tweet_lane_and_handle_filter():
    fast = {"event_type": "fast_tweet", "timestamp": 1789500000000,
            "tweet": {"id": "777", "screen_name": "TankerTrackers", "user_id": "89234502",
                      "text": "vessel struck", "type": "post", "created_ms": 1789500000000,
                      "media": ["https://pbs.example/img.jpg"]}}
    posts = parse_ws_message(fast, ALLOWED, received_at=NOW)
    assert len(posts) == 1
    assert posts[0].account_id == "89234502"
    assert posts[0].media_urls == ["https://pbs.example/img.jpg"]
    assert posts[0].posted_at == datetime.fromtimestamp(1789500000, UTC)

    # fast_tweet events are not rule-attributed: unknown authors are filtered
    fast["tweet"]["screen_name"] = "SomeRandoAccount"
    assert parse_ws_message(fast, ALLOWED, received_at=NOW) == []
    # ...and reply-typed fast tweets are dropped
    fast["tweet"].update(screen_name="TankerTrackers", type="reply")
    assert parse_ws_message(fast, ALLOWED, received_at=NOW) == []


def _rules_transport(settings, calls, existing_rules):
    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path == "/oapi/tweet_filter/get_rules":
            return httpx.Response(200, json={"status": "success", "rules": existing_rules})
        return httpx.Response(200, json={"status": "success", "msg": "ok", "rule_id": "r1"})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler),
                             base_url="https://api.twitterapi.io")


async def test_sync_never_updates_an_active_up_to_date_rule(settings, repo):
    # Regression: the vendor rejects update_rule on active rules; calling it
    # unconditionally crash-looped every reconnect (live incident 2026-09-10).
    handles = [a.handle for a in settings.accounts if a.source == "x"]
    value = build_rule_values(handles)[0]
    calls = []
    existing = [{"rule_id": "r1", "tag": "newsstream-0", "value": value,
                 "interval_seconds": settings.x_push_interval, "is_effect": 1}]
    async with _rules_transport(settings, calls, existing) as rest:
        await XPushAdapter(settings, repo)._sync_rules(rest, handles)
    assert calls == [("GET", "/oapi/tweet_filter/get_rules")]


async def test_sync_activates_inactive_rule(settings, repo):
    handles = [a.handle for a in settings.accounts if a.source == "x"]
    value = build_rule_values(handles)[0]
    calls = []
    existing = [{"rule_id": "r1", "tag": "newsstream-0", "value": value,
                 "interval_seconds": settings.x_push_interval, "is_effect": 0}]
    async with _rules_transport(settings, calls, existing) as rest:
        await XPushAdapter(settings, repo)._sync_rules(rest, handles)
    assert ("POST", "/oapi/tweet_filter/update_rule") in calls


def test_x_push_rule_values_chunked_under_limit():
    handles = [f"account_number_{i:03d}" for i in range(30)]
    values = build_rule_values(handles)
    assert all(len(v) <= RULE_VALUE_MAX for v in values)
    assert len(values) > 1  # 30 long handles cannot fit one rule
    joined = " OR ".join(values)
    for h in handles:
        assert f"from:{h}" in joined


# ---------------- normalization ----------------

def test_normalize_drops_replies_and_resolves_tier(settings):
    post, _ = load_fixture("khasab_vlcc")[0]
    post.tier = "osint"  # pretend the adapter didn't know
    normalized = normalize(post, settings)
    assert normalized.tier == "tracker"  # TankerTrackers per config.yaml

    reply, _ = load_fixture("khasab_vlcc")[1]
    reply.is_reply = True
    assert normalize(reply, settings) is None
