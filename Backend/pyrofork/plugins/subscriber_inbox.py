import asyncio
from time import monotonic

from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend import db
from Backend.config import Telegram
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

_inbox_queue = asyncio.Queue(maxsize=200)
_inbox_task = None
_inbox_registered = {}
_inbox_counts = {"forwarded": 0, "failed": 0, "dropped": 0}
_inbox_control = asyncio.Lock()


@Client.on_message(filters.command("inbox") & filters.private & filters.user(Telegram.OWNER_ID), group=-4)
async def subscriber_inbox_menu(client, message):
    enabled = SettingsManager.current().forward_subscriber_messages
    await message.reply_text(
        f"📥 Subscriber inbox: {'ON' if enabled else 'OFF'}\n"
        "Forwards incoming private messages from users with a registered addon token (including expired subscriptions).\n\n"
        f"Queued: {_inbox_queue.qsize()}/200\nForwarded: {_inbox_counts['forwarded']}\n"
        f"Failed: {_inbox_counts['failed']}\nDropped: {_inbox_counts['dropped']}\n\n"
        "Counters reset on restart. Pending messages are not saved across restarts. "
        "Turning off discards pending messages; turning on does not backfill. "
        "Messages that Telegram does not allow forwarding are skipped.",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Turn OFF" if enabled else "Turn ON", callback_data="inbox:off" if enabled else "inbox:on"
                    )
                ]
            ]
        ),
    )
    message.stop_propagation()


@Client.on_callback_query(filters.regex(r"^inbox:(on|off)$"))
async def subscriber_inbox_toggle(client, query):
    global _inbox_task
    if (
        not query.from_user
        or query.from_user.id != Telegram.OWNER_ID
        or not query.message
        or query.message.chat.id != Telegram.OWNER_ID
    ):
        return await query.answer("Owner only.", show_alert=True)
    await query.answer()
    async with _inbox_control:
        enabled = query.data == "inbox:on"
        try:
            await SettingsManager.update(db, {"forward_subscriber_messages": enabled})
        except Exception:
            return await query.message.reply_text("Could not save inbox setting. Please retry.")
        if not enabled:
            if _inbox_task and not _inbox_task.done():
                _inbox_task.cancel()
                try:
                    await _inbox_task
                except asyncio.CancelledError:
                    pass
            _inbox_task = None
            while not _inbox_queue.empty():
                _inbox_queue.get_nowait()
                _inbox_queue.task_done()
                _inbox_counts["dropped"] += 1
        _inbox_registered.clear()
    await subscriber_inbox_menu(client, query.message)


@Client.on_message(filters.private & filters.incoming & ~filters.service, group=-10)
async def subscriber_inbox_capture(client, message):
    global _inbox_task
    user = message.from_user
    if (
        not SettingsManager.current().forward_subscriber_messages
        or not user
        or user.is_bot
        or user.id == Telegram.OWNER_ID
    ):
        return
    # IDs only: never download or retain subscribers' media in RAM.
    try:
        _inbox_queue.put_nowait((user.id, message.chat.id, message.id))
    except asyncio.QueueFull:
        _inbox_counts["dropped"] += 1
        return
    if _inbox_task is None or _inbox_task.done():
        _inbox_task = asyncio.create_task(_forward_subscriber_inbox(client))


async def _forward_subscriber_inbox(client):
    while not _inbox_queue.empty() and SettingsManager.current().forward_subscriber_messages:
        uid, chat_id, message_id = _inbox_queue.get_nowait()
        completed = False
        try:
            cached = _inbox_registered.get(uid)
            if cached is None or cached[0] < monotonic():
                registered = await db.has_registered_token(uid)
                if len(_inbox_registered) >= 1024:
                    _inbox_registered.clear()
                _inbox_registered[uid] = (monotonic() + 60, registered)
            else:
                registered = cached[1]
            if not registered:
                completed = True
                continue
            while SettingsManager.current().forward_subscriber_messages:
                try:
                    # One Telegram forward request; retains the original attribution.
                    await client.forward_messages(Telegram.OWNER_ID, chat_id, message_id)
                    _inbox_counts["forwarded"] += 1
                    completed = True
                    break
                except FloodWait as exc:
                    await asyncio.sleep(max(1, exc.value) + 1)
            if not completed:
                _inbox_counts["dropped"] += 1
                completed = True
            await asyncio.sleep(1.1)
        except asyncio.CancelledError:
            if not completed:
                _inbox_counts["dropped"] += 1
            raise
        except Exception as exc:
            _inbox_counts["failed"] += 1
            LOGGER.warning(f"Subscriber inbox forwarding failed: {type(exc).__name__}")
            await asyncio.sleep(1.1)
        finally:
            _inbox_queue.task_done()
