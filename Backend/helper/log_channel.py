"""Operator log channel, delivered by an isolated MTProto bot client.

The log bot has its own Pyrofork/Wzgram client and its own MTProto connection. It does
not share StreamBot, its update workers, announcement sends, or media-transfer slots.
Events are queued, batched and paced in a background worker; callers never wait for
Telegram delivery. The client is started lazily only when the log channel is configured
and the first test/log send is requested.
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime
from html import escape
from typing import Iterable, Optional, Tuple

from wzgram import Client
from wzgram.enums import ParseMode
from wzgram.errors import FloodWait

from Backend.config import Telegram
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

MAX_LEN = 3800            # Telegram's limit is 4096; keep headroom for the footer
MIN_INTERVAL = 1.2        # seconds between posts to one channel
QUEUE_MAX = 2000
SEP = "━━━━━━━━━━━━━━━━━━"

_queue: Optional[asyncio.Queue] = None
_worker: Optional[asyncio.Task] = None
_dropped = 0
_client = None
_client_token = ""
_client_lock: Optional[asyncio.Lock] = None
_TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")


# ── configuration ────────────────────────────────────────────────────────────
def _settings() -> Tuple[str, str]:
    s = SettingsManager.current()
    return str(getattr(s, "log_channel", "") or "").strip(), str(getattr(s, "log_bot_token", "") or "").strip()


def enabled() -> bool:
    channel, token = _settings()
    return bool(channel and token)


def valid_token(token: str) -> bool:
    return bool(_TOKEN_RE.match(token or ""))


def _chat(value: str):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


# ── isolated MTProto transport ───────────────────────────────────────────────
async def _get_client(token: str):
    """Return the dedicated log-bot client, replacing it if its token changed."""
    global _client, _client_token, _client_lock
    if not valid_token(token):
        raise ValueError("Invalid log bot token")
    if _client_lock is None:
        _client_lock = asyncio.Lock()
    async with _client_lock:
        if _client is not None and _client_token == token:
            return _client
        if _client is not None:
            try:
                await _client.stop()
            except Exception as exc:
                LOGGER.warning("[LogChannel] Could not stop previous MTProto client: %s", type(exc).__name__)
            _client = None
            _client_token = ""

        client = Client(
            name="log-channel",
            api_id=Telegram.API_ID,
            api_hash=Telegram.API_HASH,
            bot_token=token,
            sleep_threshold=20,
            no_updates=True,
            in_memory=True,
        )
        try:
            await client.start()
        except Exception:
            try:
                await client.stop()
            except Exception:
                pass
            raise
        _client = client
        _client_token = token
        LOGGER.info("[LogChannel] Dedicated MTProto log client connected.")
        return _client


async def _send_mtproto(channel: str, token: str, text: str) -> None:
    client = await _get_client(token)
    await client.send_message(
        chat_id=_chat(channel),
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def send_now(text: str, channel: str = "", token: str = "") -> Tuple[bool, str]:
    """Send one message immediately (used by the settings "Send test" button)."""
    cfg_channel, cfg_token = _settings()
    channel, token = (channel or cfg_channel).strip(), (token or cfg_token).strip()
    if not channel or not token:
        return False, "Set both the log channel and the log bot token first."
    if not valid_token(token):
        return False, "That does not look like a bot token (123456:ABC…)."
    try:
        await _send_mtproto(channel, token, text)
        return True, ""
    except FloodWait as exc:
        wait = int(getattr(exc, "value", getattr(exc, "x", 1)) or 1)
        return False, f"Telegram rate-limited the log bot; retry after {wait} seconds."
    except Exception as exc:
        return False, f"{type(exc).__name__}: {str(exc)[:140]}"


async def bot_username(token: str) -> str:
    try:
        client = await _get_client(token)
        me = getattr(client, "me", None) or await client.get_me()
        username = getattr(me, "username", None)
        return f"@{username}" if username else ""
    except Exception as exc:
        LOGGER.warning("[LogChannel] MTProto bot validation failed: %s", type(exc).__name__)
        return ""


async def _send_text(channel: str, token: str, text: str) -> bool:
    for attempt in range(4):
        try:
            await _send_mtproto(channel, token, text)
            return True
        except FloodWait as exc:
            wait = int(getattr(exc, "value", getattr(exc, "x", 1)) or 1)
            if attempt == 3:
                LOGGER.warning("[LogChannel] Delivery deferred by Telegram FloodWait (%ss).", wait)
                return False
            await asyncio.sleep(min(wait + 1, 120))
        except Exception as exc:
            description = str(exc)
            if "parse entities" in description.lower() and "<" in text:
                try:
                    await _send_mtproto(channel, token, re.sub(r"<[^>]+>", "", text))
                    return True
                except Exception as retry_exc:
                    exc = retry_exc
            LOGGER.warning("[LogChannel] Delivery failed: %s: %s", type(exc).__name__, str(exc)[:140])
            return False
    return False


async def stop() -> None:
    """Stop the isolated log client during application shutdown."""
    global _client, _client_token
    client, _client = _client, None
    _client_token = ""
    if client is not None:
        try:
            await client.stop()
            LOGGER.info("[LogChannel] Dedicated MTProto log client stopped.")
        except Exception as exc:
            LOGGER.warning("[LogChannel] Error stopping MTProto log client: %s", type(exc).__name__)


async def _drain() -> None:
    global _dropped
    while True:
        text = await _queue.get()
        parts = [text]
        size = len(text)
        # Merge whatever else is already waiting so a burst becomes one message.
        while not _queue.empty():
            nxt = _queue.get_nowait()
            if size + len(nxt) + 2 > MAX_LEN:
                _queue.put_nowait(nxt)
                break
            parts.append(nxt)
            size += len(nxt) + 2
        if _dropped:
            parts.append(f"⚠️ <i>{_dropped} log event(s) were dropped because the queue was full.</i>")
            _dropped = 0
        channel, token = _settings()
        if channel and token:
            await _send_text(channel, token, "\n\n".join(parts))
        await asyncio.sleep(MIN_INTERVAL)


def log(text: str) -> None:
    """Queue one formatted (HTML) message. Silent no-op when logging is disabled."""
    global _queue, _worker, _dropped
    try:
        if not text or not enabled():
            return
        loop = asyncio.get_running_loop()
        if _queue is None:
            _queue = asyncio.Queue(maxsize=QUEUE_MAX)
        if _worker is None or _worker.done():
            _worker = loop.create_task(_drain())
        try:
            _queue.put_nowait(text[:MAX_LEN])
        except asyncio.QueueFull:
            _dropped += 1
    except RuntimeError:
        pass  # no running loop (import time / shutdown)
    except Exception as exc:
        LOGGER.debug("[LogChannel] enqueue failed: %s", exc)


# ── formatting helpers ───────────────────────────────────────────────────────
def _e(value) -> str:
    return escape(str(value if value not in (None, "") else "—"))


def _now() -> str:
    return datetime.utcnow().strftime("%d %b %Y · %H:%M UTC")


def _card(emoji: str, title: str, rows: Iterable[Tuple[str, str, object]], footer: bool = True) -> str:
    lines = [f"{emoji} <b>{_e(title)}</b>", SEP]
    for icon, label, value in rows:
        if value is None:
            continue
        lines.append(f"{icon} <b>{_e(label)}:</b> {value if isinstance(value, _Raw) else _e(value)}")
    if footer:
        lines += ["", f"🕒 <i>{_now()}</i>"]
    return "\n".join(lines)


class _Raw(str):
    """Already-escaped HTML (links, <code>)."""


def code(value) -> _Raw:
    return _Raw(f"<code>{_e(value)}</code>")


def user_link(user_id, name=None, username=None) -> _Raw:
    label = _e(name or (f"@{username}" if username else f"User {user_id}"))
    link = f'<a href="tg://user?id={int(user_id)}">{label}</a>' if user_id else label
    extra = f" (@{escape(str(username))})" if username and name else ""
    return _Raw(f"{link}{extra}")


# ── events ───────────────────────────────────────────────────────────────────
_TASK_STYLE = {
    "start": ("🚀", "Started"),
    "finish": ("✅", "Finished"),
    "stop": ("🛑", "Stopped"),
    "cancel": ("❌", "Cancelled"),
    "error": ("⚠️", "Failed"),
}


def log_task(event: str, task: str, rows: Iterable[Tuple[str, str, object]] = (), emoji: str = "🛠️") -> None:
    """Log lifecycle milestones only; never stream per-item scan progress here."""
    normalized = {"completed": "finish", "success": "finish", "cancel": "stop", "cancelled": "stop", "error": "stop", "failed": "stop"}.get(event, event)
    if normalized not in ("start", "stop", "finish"):
        return
    icon, label = _TASK_STYLE[normalized]
    log(_card(icon, f"{emoji} {task} {label}", list(rows)))



def log_service_event(event: str, *, version: str, build_id: str = "", detail: str = "") -> None:
    """Log process lifecycle events with the exact running version/build id."""
    normalized = str(event or "").strip().lower()
    if normalized not in {"restarting", "online", "offline"}:
        return
    labels = {
        "restarting": ("♻️", "Service Restarting"),
        "online": ("🟢", "Service Back Online"),
        "offline": ("🔴", "Service Going Offline"),
    }
    icon, title = labels[normalized]
    log(_card(icon, title, [
        ("🏷️", "Version", code(version)),
        ("🧩", "Version ID", code(build_id) if build_id else None),
        ("ℹ️", "Details", detail or None),
    ]))


def log_new_user(user_id: int, name: Optional[str], username: Optional[str]) -> None:
    log(_card("🆕", "New User Registered", [
        ("👤", "User", user_link(user_id, name, username)),
        ("🆔", "ID", code(user_id)),
        ("🔗", "Username", f"@{username}" if username else None),
    ]))


def log_token_created(name: str, user_id=None, expires=None) -> None:
    log(_card("🔑", "Access Token Created", [
        ("🏷️", "Name", name),
        ("🆔", "User ID", code(user_id) if user_id else None),
        ("⏳", "Expires", expires),
    ]))


def log_stream_start(*, user, user_id, title, file_name, ip, device, plan=None, route=None) -> None:
    log(_card("▶️", "Live Stream Started", [
        ("👤", "User", user_link(user_id, user) if user_id else (user or "Unknown")),
        ("🆔", "ID", code(user_id) if user_id else None),
        ("🎬", "Title", title),
        ("📄", "File", file_name),
        ("🌐", "IP", code(ip) if ip else None),
        ("📱", "Device", device),
        ("💳", "Plan", plan),
        ("🛰️", "Route", route),
    ]))


def log_stream_stop(*, user, user_id, title, file_name, reason="request ended") -> None:
    """Log a playback connection ending, coalescing range-request churn upstream."""
    log(_card("⏹️", "Playback Stopped", [
        ("👤", "User", user_link(user_id, user) if user_id else (user or "Unknown")),
        ("🆔", "ID", code(user_id) if user_id else None),
        ("🎬", "Title", title),
        ("📄", "File", file_name),
        ("ℹ️", "Connection", reason),
    ]))


def _media_line(info: dict) -> str:
    title = info.get("title") or "Unknown"
    year = str(info.get("year") or info.get("release_year") or "").strip()[:4]
    out = escape(str(title))
    if year and year not in str(title):
        out += f" ({escape(year)})"
    if info.get("media_type") == "tv":
        try:
            out += f" S{int(info.get('season_number')):02d}E{int(info.get('episode_number')):02d}"
        except (TypeError, ValueError):
            pass
    return out


def _short(name: str, limit: int = 140) -> str:
    name = str(name or "").strip()
    return name if len(name) <= limit else name[: limit - 1] + "…"


def log_media_assigned(file_name: str, info: dict, origin: str = "Live ingestion", duplicate: bool = False) -> None:
    """``file name > assigned title`` for one live-ingested file (name, year, S/E and API used)."""
    api = info.get("meta_source") or "Unknown"
    icon = "♻️" if duplicate else ("📺" if info.get("media_type") == "tv" else "🎬")
    head = "Duplicate Skipped" if duplicate else "Media Assigned"
    log(
        f"{icon} <b>{head}</b> · <i>{escape(origin)}</i>\n"
        f"📄 <code>{escape(_short(file_name))}</code>\n"
        f"➡️ <b>{_media_line(info)}</b>\n"
        f"🔌 <b>API:</b> {escape(str(api))} · 🏷️ {escape(str(info.get('quality') or '—'))}"
    )


def log_verify_change(action: str, file_name: str, old: str, new: str = "", api: str = "Unknown") -> None:
    """One re-assignment / skip-channel move made by Verify Media, including resolver."""
    if action in ("unmatched", "skip"):
        body = f"🚫 <b>No confident match</b> · was <b>{escape(old)}</b>\n📤 Sent to skip channel; original removed"
        icon = "🧹"
    elif action == "duplicate":
        body = f"♻️ <b>{escape(old)}</b> → <b>{escape(new)}</b>\n🗑️ Already had this quality; wrong entry dropped"
        icon = "🧬"
    else:
        body = f"🔀 <b>{escape(old)}</b> ➡️ <b>{escape(new)}</b>"
        icon = "🧭"
    heading = "Verify · Sent to Skip" if action in ("unmatched", "skip") else "Verify · Re-assigned"
    log(
        f"{icon} <b>{heading}</b>\n📄 <code>{escape(_short(file_name))}</code>\n{body}\n"
        f"🔌 <b>API:</b> {escape(str(api or 'Unknown'))}"
    )
