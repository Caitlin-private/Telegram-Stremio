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


async def route_to_skip_channel(client: Client, message: Message, reason=None, force_delete=False, retry_call=None) -> None:
    async def call(operation, *args, **kwargs):
        if retry_call is not None:
            return await retry_call(operation, *args, **kwargs)
        return await operation(*args, **kwargs)
    settings = SettingsManager.current()
    skip = settings.skip_channel
    if not skip:
        if force_delete:
            raise RuntimeError('Configure a skip channel before moving resolution-filtered files.')
        return

    if is_skip_channel(message):
        raise ValueError('The source and skip channel must be different.')

    skip_chat = int(skip) if str(skip).lstrip("-").replace("-100", "").isdigit() else skip

    try:
        copied = await call(message.copy, skip_chat)
    except FloodWait as e:
        await asleep(e.value)
        try:
            copied = await call(message.copy, skip_chat)
        except Exception as e2:
            LOGGER.error(f"[SkipChannel] Copy failed for message {message.id}: {e2}")
            if force_delete or isinstance(e2, FloodWait):
                raise
            return
    except Exception as e:
        LOGGER.error(f"[SkipChannel] Could not copy message {message.id} to skip channel: {e}")
        if force_delete or retry_call is not None:
            raise
        return

    # Reply to the copy, never the source, and never post a traceback/log dump.
    from Backend.helper.metadata.parse import analyze_metadata_failure
    from Backend.helper.pyro import clean_filename
    media = message.document or message.video
    title = message.caption or getattr(media, "file_name", None) or "Unnamed media"
    try:
        supplied_reason = reason is not None
        reason = reason or analyze_metadata_failure(clean_filename(title))
        heading = 'Media skipped' if supplied_reason else 'Metadata failed for file'
        text = f"{heading}: {title[:1000]} (ID: {message.id})\nReason: {reason}"
        try:
            await call(client.send_message, skip_chat, text[:4000], reply_to_message_id=copied.id, parse_mode=ParseMode.DISABLED)
        except FloodWait as wait:
            await asleep(wait.value)
            await call(client.send_message, skip_chat, text[:4000], reply_to_message_id=copied.id, parse_mode=ParseMode.DISABLED)
    except Exception as exc:
        LOGGER.warning(f"[SkipChannel] Could not send reason for message {message.id}: {exc}")
        if force_delete or retry_call is not None or isinstance(exc, FloodWait):
            raise

    if force_delete:
        # Only remove the source after both the copy and its reason succeeded.
        # Unlike the legacy deletion helper, failures must reach the durable queue.
        try:
            deleted = await call(client.delete_messages, message.chat.id, message.id)
        except FloodWait as wait:
            await asleep(wait.value)
            deleted = await call(client.delete_messages, message.chat.id, message.id)
        if deleted is False:
            raise RuntimeError('Could not delete the original resolution-filtered message.')
        return

    if settings.delete_on_metadata_fail:
        if retry_call is not None:
            if await call(client.delete_messages, message.chat.id, message.id) is False:
                raise RuntimeError('Could not delete rejected scan message.')
            return
        try:
            from Backend.helper.task_manager import delete_message
            await delete_message(message.chat.id, message.id)
        except Exception as e:
            LOGGER.warning(f"[SkipChannel] Could not delete original message {message.id}: {e}")
