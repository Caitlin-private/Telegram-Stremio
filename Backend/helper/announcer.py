from asyncio import create_task, Lock, sleep
from datetime import datetime
from html import escape

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageDeleteForbidden, MessageIdInvalid
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend import db
from Backend.helper.bot_media import media_payload
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot, get_streambot_url
from Backend.helper.pyro import artwork_available, defer_artwork
from Backend.helper.poster_provider import _poster_url, _apply_fanart

_announcement_lock = Lock()


#----- Accept either a numeric channel id (-100...) or an @username
def _resolve_chat(value: str):
    value = str(value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


#----- Atomically claim a title so it is announced at most once; returns True if newly claimed
async def _claim(media_type: str, tmdb_id) -> bool:
    if not tmdb_id:
        return False
    result = await db.dbs["tracking"]["announced"].update_one(
        {"_id": f"{media_type}:{tmdb_id}"},
        {"$setOnInsert": {"at": datetime.utcnow()}},
        upsert=True,
    )
    return result.upserted_id is not None


async def _store_announcement_msg(media_type: str, tmdb_id, chat_id, message_id: int) -> None:
    if not tmdb_id or not message_id:
        return
    try:
        await db.dbs["tracking"]["announced"].update_one(
            {"_id": f"{media_type}:{tmdb_id}"},
            {"$set": {"chat_id": chat_id, "message_id": message_id, "at": datetime.utcnow()}},
            upsert=True,
        )
    except Exception as e:
        LOGGER.warning(f"Failed to store announcement message id: {e}")


def _build_caption(info: dict) -> str:
    # Limit decoded UTF-16 length before escaping for Telegram's photo caption.
    def text(value, limit):
        value = ' '.join(str(value or '').split())
        if len(value.encode('utf-16-le')) // 2 > limit:
            value = value.encode('utf-16-le')[:(limit - 1) * 2].decode('utf-16-le', errors='ignore').rstrip() + '…'
        return escape(value)

    is_tv = info.get("media_type") in ("tv", "series")
    title = text(info.get('title_english') or info.get('title') or 'Unknown', 180)
    lines = ["✨ <b>New on Caitlin Stream</b>", "", f"{'📺' if is_tv else '🎬'} <b>{title}</b>"]
    details = ['TV Series' if is_tv else 'Movie']
    year = info.get('year') or info.get('release_year')
    if year:
        details.append(text(year, 20))
    if info.get("rate"):
        try:
            rating = float(info['rate'])
            if 0 < rating <= 10:
                details.append(f'⭐ {rating:.1f}/10')
        except (TypeError, ValueError):
            pass
    lines.append(' · '.join(details))
    genres = info.get("genres") or []
    if isinstance(genres, str):
        genres = [genres]
    if genres:
        lines.append(f"🎭 {text(', '.join(str(g) for g in genres[:4]), 100)}")
    desc = text(info.get('description'), 420)
    if desc:
        lines += ["", desc]
    lines += ['', '<i>Choose a player below, or open View media for files.</i>']
    return "\n".join(lines)


def _build_markup(info: dict):
    rows = []
    base = SettingsManager.current().base_url
    imdb_id = str(info.get("imdb_id") or "").strip()
    stremio_type = "series" if info.get("media_type") == "tv" else "movie"
    if base and imdb_id:
        rows.append([
            InlineKeyboardButton("▶️ Stremio", url=f"{base}/open/stremio/{stremio_type}/{imdb_id}"),
            InlineKeyboardButton("📱 Nuvio", url=f"{base}/open/nuvio/{stremio_type}/{imdb_id}"),
        ])
    bot_url = get_streambot_url()
    if bot_url and bot_url != "https://t.me/":
        bot_buttons = [InlineKeyboardButton("🤖 Get Addon", url=bot_url)]
        payload = media_payload(info.get("media_type"), info.get("tmdb_id"))
        if payload:
            bot_buttons.append(InlineKeyboardButton("📂 View media", url=f"{bot_url}?start={payload}"))
        rows.append(bot_buttons)
    return InlineKeyboardMarkup(rows) if rows else None


async def _announce(info: dict) -> None:
    # Prevent concurrent photo attempts from racing past the same cooldown.
    async with _announcement_lock:
        await _announce_serial(info)


async def _announce_serial(info: dict) -> None:
    settings = SettingsManager.current()
    chat = _resolve_chat(settings.announcement_channel)
    if not settings.announce_new_content or chat is None:
        return
    if not await _claim(info.get("media_type"), info.get("tmdb_id")):
        return

    caption = _build_caption(info)
    markup = _build_markup(info)

    try:
        sent = None
        if artwork_available(StreamBot):
            try:
                artwork = {'poster': _poster_url(info.get('imdb_id'), info.get('poster') or info.get('backdrop'),
                                                 info.get('tmdb_id'), info.get('media_type'))}
                await _apply_fanart(artwork, info)
                if artwork.get('poster'):
                    sent = await StreamBot.send_photo(chat, artwork['poster'], caption=caption,
                                           parse_mode=ParseMode.HTML, reply_markup=markup)
            except FloodWait as e:
                defer_artwork(StreamBot, e.value)
            except Exception:
                sent = None
        if sent is None:
            while True:
                try:
                    sent = await StreamBot.send_message(chat, caption, parse_mode=ParseMode.HTML,
                                             reply_markup=markup, disable_web_page_preview=True)
                    break
                except FloodWait as e:
                    defer_artwork(StreamBot, e.value)
                    await sleep(max(1, e.value) + 1)
        if sent is not None:
            await _store_announcement_msg(info.get("media_type"), info.get("tmdb_id"), chat, sent.id)
    except Exception as e:
        # A failed delivery must not permanently consume the deduplication claim.
        try:
            await db.dbs['tracking']['announced'].delete_one({
                '_id': f'{info.get("media_type")}:{info.get("tmdb_id")}',
                'message_id': {'$exists': False},
            })
        except Exception as cleanup_error:
            LOGGER.warning(f'Could not release announcement claim: {cleanup_error}')
        LOGGER.error(f"Announcement failed for '{info.get('title')}': {e}")


#----- Fire-and-forget announcement for a freshly added title
def announce_new_media(info: dict) -> None:
    try:
        create_task(_announce(dict(info)))
    except RuntimeError:
        LOGGER.warning("Announcement skipped: no running event loop.")


#----- Delete the announcement message when media is removed from the library
async def delete_announcement(media_type: str, tmdb_id) -> None:
    if not tmdb_id:
        return
    key = f"{media_type}:{tmdb_id}"
    try:
        doc = await db.dbs["tracking"]["announced"].find_one_and_delete({"_id": key})
    except Exception as e:
        LOGGER.warning(f"Failed to lookup announcement for {key}: {e}")
        return
    if not doc:
        return
    chat_id = doc.get("chat_id")
    message_id = doc.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await StreamBot.delete_messages(chat_id, message_id)
        LOGGER.info(f"Deleted announcement message {message_id} for {key}")
    except (MessageDeleteForbidden, MessageIdInvalid) as e:
        LOGGER.warning(f"Could not delete announcement {message_id} for {key}: {e}")
    except FloodWait as e:
        LOGGER.warning(f"FloodWait deleting announcement for {key}: {e.value}s")
    except Exception as e:
        LOGGER.warning(f"Failed to delete announcement message for {key}: {e}")


def delete_announcement_async(media_type: str, tmdb_id) -> None:
    try:
        create_task(delete_announcement(media_type, tmdb_id))
    except RuntimeError:
        LOGGER.warning("Announcement delete skipped: no running event loop.")
