"""Bounded Telegram metadata requests shared by manual and automatic checks."""
import asyncio
import hashlib
import time
from weakref import WeakKeyDictionary

from wzgram.errors import FloodWait

from Backend.logger import LOGGER

REQUEST_TIMEOUT = 20.0
CHECK_TIMEOUT = 60.0
MAX_FLOOD_RETRIES = 1
_clients = WeakKeyDictionary()


class CheckUnavailable(Exception):
    """An inconclusive check: never evidence that a file is dead."""


def check_reference(value):
    # Do not log the complete encoded stream URL/token.
    return hashlib.sha256(str(value).encode()).hexdigest()[:12]


async def get_check_message(client, chat_id, msg_id, *, source):
    """Serialize checker requests per client and retain FloodWait cooldowns.

    Call inside the complete-check deadline, which also bounds lock/cooldown
    waits and retries. Other application uses of the client are unaffected.
    """
    try:
        raw_chat = str(chat_id)
        if chat_id is None or msg_id is None:
            raise ValueError("missing message coordinates")
        chat_id = int(raw_chat if raw_chat.startswith("-100") else f"-100{raw_chat}")
        msg_id = int(msg_id)
    except (TypeError, ValueError) as exc:
        LOGGER.warning("[%s] Invalid message coordinates: chat=%s message=%s", source, chat_id, msg_id)
        raise CheckUnavailable("invalid message coordinates") from exc

    state = _clients.setdefault(client, {"lock": asyncio.Lock(), "until": 0.0})
    label = getattr(client, "name", type(client).__name__)
    started = time.monotonic()
    outcome = "unknown"
    LOGGER.info("[%s] Request start: client=%s chat=%s message=%s", source, label, chat_id, msg_id)
    try:
        async with state["lock"]:
            for attempt in range(MAX_FLOOD_RETRIES + 1):
                delay = state["until"] - time.monotonic()
                if delay > 0:
                    LOGGER.info("[%s] Telegram cooldown: client=%s remaining=%.1fs", source, label, delay)
                    await asyncio.sleep(delay)
                try:
                    async with asyncio.timeout(REQUEST_TIMEOUT):
                        message = await client.get_messages(chat_id, msg_id, replies=0)
                except FloodWait as exc:
                    delay = max(1, int(exc.value)) + 1
                    state["until"] = max(state["until"], time.monotonic() + delay)
                    LOGGER.warning("[%s] FloodWait: client=%s chat=%s message=%s wait=%ss attempt=%s",
                                   source, label, chat_id, msg_id, delay, attempt + 1)
                    if attempt == MAX_FLOOD_RETRIES:
                        raise CheckUnavailable("FloodWait retry limit") from exc
                    continue
                except TimeoutError as exc:
                    LOGGER.warning("[%s] Request timeout: client=%s chat=%s message=%s limit=%ss",
                                   source, label, chat_id, msg_id, REQUEST_TIMEOUT)
                    raise CheckUnavailable("request timeout") from exc
                except Exception as exc:
                    LOGGER.warning("[%s] Request failed: client=%s chat=%s message=%s error=%s: %s",
                                   source, label, chat_id, msg_id, type(exc).__name__, exc, exc_info=True)
                    raise CheckUnavailable(type(exc).__name__) from exc

                restrictions = getattr(message, "restrictions", None) or getattr(message, "restriction_reason", None)
                if restrictions:
                    details = [{key: getattr(reason, key, None) for key in ("platform", "reason", "text")}
                               if not isinstance(reason, dict) else reason for reason in restrictions]
                    LOGGER.warning("[%s] Restricted message: client=%s chat=%s message=%s restrictions=%s",
                                   source, label, chat_id, msg_id, details)
                    outcome = "restricted"
                    raise CheckUnavailable("restricted message")
                outcome = "empty" if message is None or getattr(message, "empty", False) else "received"
                return message
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    finally:
        LOGGER.info("[%s] Request end: client=%s chat=%s message=%s outcome=%s elapsed=%.2fs",
                    source, label, chat_id, msg_id, outcome, time.monotonic() - started)
