import asyncio
from time import monotonic

from wzgram import Client, filters, enums
from wzgram.errors import FloodWait
from wzgram.types import InlineKeyboardButton, InlineKeyboardMarkup, ForceReply

from Backend import db
from Backend.config import Telegram
from Backend.logger import LOGGER
from Backend.helper.subscriber_tools import subscriber_picker

_dm_draft = None
_dm_job = None


@Client.on_message(filters.command("message") & filters.private & filters.user(Telegram.OWNER_ID), group=-3)
async def subscriber_message_menu(client, message):
    global _dm_draft
    _dm_draft = None
    if _dm_job and not _dm_job["task"].done():
        await message.reply_text(
            f"Sending: {_dm_job['sent']} sent · {_dm_job['failed']} failed",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Stop sending", callback_data="dm:stop")]]),
        )
    else:
        if _dm_job:
            await message.reply_text(
                f"Last delivery: {_dm_job['sent']} sent · {_dm_job['failed']} failed/unconfirmed · {_dm_job['total']-_dm_job['sent']-_dm_job['failed']} not attempted"
            )
        await message.reply_text(
            "✉️ Message subscribers\nAll registered users includes expired subscriptions. Text messages only. Preview before sending.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("All registered subscribers", callback_data="dm:scope:all")],
                    [InlineKeyboardButton("Select user", callback_data="dm:users:0")],
                ]
            ),
        )
    message.stop_propagation()


@Client.on_callback_query(filters.regex(r"^dm:"))
async def subscriber_message_action(client, query):
    global _dm_draft, _dm_job
    if (
        not query.from_user
        or query.from_user.id != Telegram.OWNER_ID
        or not query.message
        or query.message.chat.id != Telegram.OWNER_ID
    ):
        return await query.answer("Owner only.", show_alert=True)
    await query.answer()
    fields = query.data.split(":")
    action = fields[1]
    scope = fields[2] if len(fields) > 2 else ""
    if action == "stop":
        if _dm_job:
            _dm_job["stop"].set()
        return await query.message.reply_text("Stop requested. A message already being sent may still arrive.")
    if _dm_job and not _dm_job["task"].done():
        return await query.message.reply_text("A delivery is running. Use /message for status or to stop it.")
    if action == "home":
        return await subscriber_message_menu(client, query.message)
    if action == "users":
        return await subscriber_picker(query.message, scope, "dm")
    if action == "scope" and (scope == "all" or (scope.isdigit() and int(scope) > 0)):
        prompt = await query.message.reply_text(
            "Reply with the text to send (up to 3500 characters). Use /message to cancel.",
            reply_markup=ForceReply(selective=True),
        )
        _dm_draft = dict(scope=scope, prompt=prompt.id, until=monotonic() + 600)
        return
    if action == "send":
        draft = _dm_draft
        if not draft or draft.get("preview") != query.message.id or draft["until"] < monotonic():
            return await query.message.reply_text("Preview expired. Start again with /message.")
        _dm_draft = None  # Consume before the first await: prevents double-send taps.
        job = dict(sent=0, failed=0, stop=asyncio.Event(), total=len(draft["users"]))
        _dm_job = job
        job["task"] = asyncio.create_task(_deliver_subscriber_message(client, dict(draft), job))
        await query.message.reply_text(
            f"Sending to {job['total']} users. Use /message to check or stop. Restarting the service stops delivery; it will not automatically resend."
        )


@Client.on_message(filters.private & filters.user(Telegram.OWNER_ID) & filters.text & ~filters.regex(r"^/"), group=-3)
async def subscriber_message_reply(client, message):
    global _dm_draft
    draft = _dm_draft
    if not draft or not message.reply_to_message or message.reply_to_message.id != draft["prompt"]:
        return
    if draft["until"] < monotonic():
        _dm_draft = None
        await message.reply_text("Draft expired. Use /message again.")
        message.stop_propagation()
    text = message.text.strip()
    if not text or len(text.encode("utf-16-le")) // 2 > 3500:
        await message.reply_text("Use 1–3500 characters. Reply to the original prompt to retry.")
        message.stop_propagation()
    users = await db.get_registered_user_ids(None if draft["scope"] == "all" else int(draft["scope"]))
    if not users:
        await message.reply_text("No registered recipients found.")
        message.stop_propagation()
    draft.update(text=text, users=users)
    await message.reply_text(text, parse_mode=enums.ParseMode.DISABLED, disable_web_page_preview=True)
    label = "All registered subscribers" if draft["scope"] == "all" else f"User {draft['scope']}"
    confirm = await message.reply_text(
        f"Preview above\nRecipient: {label}\nTotal: {len(users)}\nSend this exact text?",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Send", callback_data="dm:send"),
                    InlineKeyboardButton("Cancel", callback_data="dm:home"),
                ]
            ]
        ),
    )
    draft["preview"] = confirm.id
    message.stop_propagation()


async def _deliver_subscriber_message(client, draft, job):
    async def wait(seconds):
        try:
            await asyncio.wait_for(job["stop"].wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    try:
        for uid in draft["users"]:
            if job["stop"].is_set():
                break
            while not job["stop"].is_set():
                try:
                    await client.send_message(
                        uid, draft["text"], parse_mode=enums.ParseMode.DISABLED, disable_web_page_preview=True
                    )
                    job["sent"] += 1
                    break
                except FloodWait as exc:
                    await wait(max(1, exc.value) + 1)
                except Exception as exc:
                    # Do not retry uncertain network outcomes, which can duplicate a DM.
                    job["failed"] += 1
                    LOGGER.warning(f"Subscriber message failed: {type(exc).__name__}")
                    break
            await wait(1.0)
    finally:
        remaining = job["total"] - job["sent"] - job["failed"]
        summary = f"✉️ Delivery {'stopped' if remaining else 'finished'}\n✅ Sent: {job['sent']}\n⚠️ Failed/unconfirmed: {job['failed']}\n⏳ Not attempted: {remaining}"
        try:
            await client.send_message(Telegram.OWNER_ID, summary, parse_mode=enums.ParseMode.DISABLED)
        except Exception:
            LOGGER.warning("Could not send subscriber delivery summary; use /message.")
