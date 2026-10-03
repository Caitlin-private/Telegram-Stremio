from asyncio import sleep as asleep

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER


def is_skip_channel(message: Message) -> bool:
    skip = SettingsManager.current().skip_channel
    if not skip:
        return False
    ref = str(skip).strip()
    if ref.lstrip("@-").replace("-100", "").isdigit():
        return ref.replace("-100", "").lstrip("@") == str(message.chat.id).replace("-100", "")
    username = (getattr(message.chat, "username", None) or "").lower()
    return bool(username) and ref.lstrip("@").lower() == username


async def route_to_skip_channel(client: Client, message: Message) -> None:
    settings = SettingsManager.current()
    skip = settings.skip_channel
    if not skip:
        return

    skip_chat = int(skip) if str(skip).lstrip("-").replace("-100", "").isdigit() else skip

    try:
        copied = await message.copy(skip_chat)
    except FloodWait as e:
        await asleep(e.value)
        try:
            copied = await message.copy(skip_chat)
        except Exception as e2:
            LOGGER.error(f"[SkipChannel] Copy failed for message {message.id}: {e2}")
            return
    except Exception as e:
        LOGGER.error(f"[SkipChannel] Could not copy message {message.id} to skip channel: {e}")
        return

    # Reply to the copy, never the source, and never post a traceback/log dump.
    from Backend.helper.metadata.parse import analyze_metadata_failure
    from Backend.helper.pyro import clean_filename
    media = message.document or message.video
    title = message.caption or getattr(media, "file_name", None) or "Unnamed media"
    try:
        reason = analyze_metadata_failure(clean_filename(title))
        text = f"Metadata failed for file: {title[:1000]} (ID: {message.id})\nReason: {reason}"
        try:
            await client.send_message(skip_chat, text[:4000], reply_to_message_id=copied.id, parse_mode=ParseMode.DISABLED)
        except FloodWait as wait:
            await asleep(wait.value)
            await client.send_message(skip_chat, text[:4000], reply_to_message_id=copied.id, parse_mode=ParseMode.DISABLED)
    except Exception as exc:
        LOGGER.warning(f"[SkipChannel] Could not send reason for message {message.id}: {exc}")

    if settings.delete_on_metadata_fail:
        try:
            from Backend.helper.task_manager import delete_message
            await delete_message(message.chat.id, message.id)
        except Exception as e:
            LOGGER.warning(f"[SkipChannel] Could not delete original message {message.id}: {e}")
