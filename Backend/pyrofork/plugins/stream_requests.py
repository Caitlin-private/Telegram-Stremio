"""Telegram admin actions for Stremio stream-side content requests."""
from html import escape

from wzgram import Client, filters
from wzgram.enums import ParseMode
from wzgram.types import CallbackQuery

from Backend.config import Telegram
from Backend.helper.requests_manager import get_request, set_status
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.announcer import _build_caption, _build_markup, announcement_image_url, announcement_photo
from Backend.logger import LOGGER


def _admin_ids() -> set[int]:
    values = [Telegram.OWNER_ID, *(getattr(Telegram, "APPROVER_IDS", []) or []),
              *(SettingsManager.current().approver_ids or [])]
    result = set()
    for value in values:
        try:
            value = int(value)
            if value > 0:
                result.add(value)
        except (TypeError, ValueError):
            continue
    return result


def _episode_suffix(doc: dict) -> str:
    if doc.get("season_number") is None or doc.get("episode_number") is None:
        return ""
    return f"S{int(doc['season_number']):02d}E{int(doc['episode_number']):02d}"


def _announcement_info(doc: dict) -> dict:
    return {
        "media_type": doc.get("media_type", "movie"),
        "tmdb_id": doc.get("tmdb_id"),
        "imdb_id": doc.get("imdb_id"),
        "title": doc.get("title") or "Unknown",
        "year": doc.get("year"),
        "poster": doc.get("poster") or "",
        "backdrop": doc.get("backdrop") or doc.get("poster") or "",
        "description": doc.get("description") or "",
        "rate": doc.get("rate") or 0,
        "genres": doc.get("genres") or [],
    }


async def _notify_requester_added(client: Client, doc: dict) -> None:
    requester_ids = sorted({int(uid) for uid in (doc.get("requester_user_ids") or [])
                             if str(uid).isdigit() and int(uid) > 0})
    if not requester_ids:
        LOGGER.warning("[STREAM REQUEST] Completed request has no Telegram requester ID; cannot DM requester.")
        return

    info = _announcement_info(doc)
    caption = "🎉 <b>Requested Media Added</b>\n\n" + _build_caption(info)
    episode = _episode_suffix(doc)
    if episode:
        caption += f"\n\n📺 <b>Requested episode:</b> <code>{episode}</code>"
    markup = _build_markup(info)

    image = None
    try:
        image_url = await announcement_image_url(info)
        if image_url:
            image = await announcement_photo(image_url)
    except Exception as exc:
        LOGGER.warning(f"[STREAM REQUEST] Could not prepare requester poster: {exc}")

    for user_id in requester_ids:
        try:
            if image is not None:
                image.seek(0)
                await client.send_photo(user_id, image, caption=caption,
                                        parse_mode=ParseMode.HTML, reply_markup=markup)
            else:
                await client.send_message(user_id, caption, parse_mode=ParseMode.HTML,
                                          reply_markup=markup, disable_web_page_preview=True)
        except Exception as exc:
            LOGGER.warning(f"[STREAM REQUEST] Could not notify requester {user_id}: {exc}")
    if image is not None:
        image.close()


async def _notify_requester_declined(client: Client, doc: dict) -> None:
    requester_ids = sorted({int(uid) for uid in (doc.get("requester_user_ids") or [])
                             if str(uid).isdigit() and int(uid) > 0})
    if not requester_ids:
        LOGGER.warning("[STREAM REQUEST] Declined request has no Telegram requester ID; cannot DM requester.")
        return
    title = escape(str(doc.get("title") or "Unknown"))
    year = escape(str(doc.get("year") or ""))
    episode = _episode_suffix(doc)
    label = f"<b>{title}</b>" + (f" ({year})" if year else "")
    message = f"❌ <b>Content Request Declined</b>\n\n{label}"
    if episode:
        message += f"\n📺 <b>Requested episode:</b> <code>{episode}</code>"
    message += "\n\nYour request was declined by an admin."
    for user_id in requester_ids:
        try:
            await client.send_message(user_id, message, parse_mode=ParseMode.HTML,
                                      disable_web_page_preview=True)
        except Exception as exc:
            LOGGER.warning(f"[STREAM REQUEST] Could not send decline notice to {user_id}: {exc}")


async def _update_admin_copies(client: Client, doc: dict, status: str, admin_name: str) -> None:
    episode = _episode_suffix(doc)
    title = escape(str(doc.get("title") or "Unknown"))
    year = escape(str(doc.get("year") or ""))
    label = f"<b>{title}</b>" + (f" ({year})" if year else "")
    state = "✅ <b>Marked done</b>" if status == "uploaded" else "❌ <b>Declined</b>"
    text = f"{state} by {escape(admin_name)}\n\n{label}"
    if episode:
        text += f"\n📺 Episode: <code>{episode}</code>"
    for item in (doc.get("admin_messages") or []):
        try:
            await client.edit_message_text(chat_id=int(item["chat_id"]),
                                           message_id=int(item["message_id"]),
                                           text=text, parse_mode=ParseMode.HTML,
                                           reply_markup=None, disable_web_page_preview=True)
        except Exception as exc:
            LOGGER.debug(f"[STREAM REQUEST] Could not update admin copy: {exc}")


@Client.on_callback_query(filters.regex(r"^sreq:(done|decline):([a-fA-F0-9]{24})$"))
async def stream_request_admin_action(client: Client, callback_query: CallbackQuery):
    actor = callback_query.from_user
    if not actor or actor.id not in _admin_ids():
        return await callback_query.answer("You are not authorized to manage content requests.", show_alert=True)

    action, request_id = callback_query.matches[0].group(1), callback_query.matches[0].group(2)
    doc = await get_request(request_id)
    if not doc or doc.get("status") != "pending":
        return await callback_query.answer("This request has already been processed or no longer exists.", show_alert=True)

    new_status = "uploaded" if action == "done" else "denied"
    updated = await set_status(request_id, new_status, only_pending=True)
    if not updated:
        return await callback_query.answer("Another admin has already processed this request.", show_alert=True)

    await callback_query.answer("Request completed." if action == "done" else "Request declined.")
    admin_name = actor.first_name or actor.username or f"Admin {actor.id}"
    await _update_admin_copies(client, doc, new_status, admin_name)
    if action == "done":
        await _notify_requester_added(client, doc)
    else:
        await _notify_requester_declined(client, doc)
