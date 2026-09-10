"""Telegram adapter (CLAUDE.md §4): free, push-based. First-choice source.

Two modes, picked from available credentials:
- telethon (MTProto): works for any public channel. Preferred.
- Bot API long-poll: only sees channels the bot was added to as admin.

Channel usernames come from config.yaml `telegram_channels`; never guessed.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from newsstream.adapters.base import AdapterStatus, NormalizedPost, Sink, SourceAdapter
from newsstream.config import Settings

log = logging.getLogger("newsstream.telegram")


def bare_chat_id(chat_id: int) -> int:
    """Telethon events report channels as -100<id>; store the stable bare id."""
    s = str(chat_id)
    return int(s[4:]) if s.startswith("-100") else abs(chat_id)


def message_to_post(
    msg: Any, chat_id: int | str, chat_username: str, *, received_at: datetime | None = None
) -> NormalizedPost | None:
    """Pure converter for a telethon Message. Returns None for replies/empty."""
    if getattr(msg, "reply_to", None) is not None:
        return None
    text = getattr(msg, "message", None) or ""
    if not text.strip():
        return None
    posted_at = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=UTC)
    return NormalizedPost(
        source="telegram",
        source_post_id=f"{chat_id}:{msg.id}",
        account_id=str(chat_id),
        account_handle=chat_username,
        url=f"https://t.me/{chat_username}/{msg.id}",
        text=text,
        quoted_text=None,
        is_reply=False,
        posted_at=posted_at,
        received_at=received_at or datetime.now(UTC),
    )


def bot_update_to_post(update: dict, *, received_at: datetime | None = None) -> NormalizedPost | None:
    """Pure converter for a Bot API channel_post update."""
    msg = update.get("channel_post")
    if not msg:
        return None
    if msg.get("reply_to_message"):
        return None
    text = msg.get("text") or msg.get("caption") or ""
    if not text.strip():
        return None
    chat = msg["chat"]
    username = chat.get("username") or str(chat["id"])
    return NormalizedPost(
        source="telegram",
        source_post_id=f"{chat['id']}:{msg['message_id']}",
        account_id=str(chat["id"]),
        account_handle=username,
        url=f"https://t.me/{username}/{msg['message_id']}",
        text=text,
        quoted_text=None,
        is_reply=False,
        posted_at=datetime.fromtimestamp(msg["date"], UTC),
        received_at=received_at or datetime.now(UTC),
    )


class TelegramAdapter(SourceAdapter):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.status = AdapterStatus(name="telegram")
        self._client = None  # telethon client, if used

    async def run(self, sink: Sink) -> None:
        s = self.settings
        if not s.telegram_channels:
            self.status.state = "disabled"
            self.status.detail = "no telegram_channels configured (see config.yaml)"
            log.warning("telegram adapter idle: %s", self.status.detail)
            return
        if s.telegram_api_id and s.telegram_api_hash:
            await self._run_telethon(sink)
        elif s.telegram_bot_token:
            await self._run_bot(sink)
        else:
            self.status.state = "disabled"
            self.status.detail = "no TELEGRAM_API_ID/HASH or TELEGRAM_BOT_TOKEN set"
            log.warning("telegram adapter idle: %s", self.status.detail)

    async def _run_telethon(self, sink: Sink) -> None:
        from telethon import TelegramClient, events  # lazy: optional at runtime

        s = self.settings
        client = TelegramClient("newsstream", int(s.telegram_api_id), s.telegram_api_hash)
        self._client = client
        await client.start()

        chats = {}
        for ch in s.telegram_channels:
            try:
                entity = await client.get_entity(ch.channel)
                chats[entity.id] = getattr(entity, "username", None) or ch.channel
            except Exception as e:
                log.error("cannot resolve telegram channel %r: %s", ch.channel, e)
        if not chats:
            self.status.state = "error"
            self.status.detail = "no telegram channel resolved"
            return

        @client.on(events.NewMessage(chats=list(chats)))
        async def _on_message(event):  # noqa: ANN001
            chat_id = bare_chat_id(event.chat_id)
            username = chats.get(chat_id, str(chat_id))
            post = message_to_post(event.message, chat_id, username)
            if post:
                await sink(post)

        self.status.state = "live"
        self.status.detail = f"telethon, {len(chats)} channel(s)"
        log.info("telegram (telethon) live on %d channel(s)", len(chats))
        await client.run_until_disconnected()

    async def _run_bot(self, sink: Sink) -> None:
        base = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}"
        offset = 0
        self.status.state = "live"
        self.status.detail = "bot API long-poll"
        async with httpx.AsyncClient(timeout=70) as http:
            while True:
                try:
                    r = await http.get(
                        f"{base}/getUpdates",
                        params={"offset": offset, "timeout": 50,
                                "allowed_updates": '["channel_post"]'},
                    )
                    r.raise_for_status()
                    self.status.state = "live"
                    for update in r.json().get("result", []):
                        offset = update["update_id"] + 1
                        post = bot_update_to_post(update)
                        if post:
                            await sink(post)
                except (httpx.HTTPError, KeyError, ValueError) as e:
                    self.status.state = "reconnecting"
                    self.status.detail = str(e)[:120]
                    log.warning("telegram bot poll error, retrying in 5s: %s", e)
                    await asyncio.sleep(5)

    async def resolve_account(self, handle: str) -> tuple[str, str]:
        if self._client is None:
            raise RuntimeError("telethon not connected")
        entity = await self._client.get_entity(handle)
        return str(entity.id), getattr(entity, "username", None) or handle
