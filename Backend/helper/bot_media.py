"""Private title search/cards shared by text search and announcement deep links."""

import re


def media_payload(kind, tmdb_id):
    if kind not in ("movie", "tv") or not re.fullmatch(r"-?[0-9]+", str(tmdb_id)):
        return None
    return f"media_{kind}_{tmdb_id}"


def parse_media_payload(payload):
    match = re.fullmatch(r"media_(movie|tv)_(-?[0-9]{1,20})", payload)
    return (match[1], int(match[2])) if match else None


async def search_titles(db, query, limit=5):
    return await db.search_media_titles(query, limit)


async def registered_token(message):
    from Backend import db
    from Backend.config import Telegram
    from Backend.helper.settings_manager import SettingsManager
    from Backend.fastapi.security.tokens import verify_token

    if not message.from_user:
        return None
    uid = message.from_user.id
    user = await db.get_user(uid)
    token = await db.get_api_token_by_user(uid)
    owner = uid == Telegram.OWNER_ID
    # Do not register arbitrary senders or give expired users a fresh access token.
    if not token and (owner or (SettingsManager.current().subscription and db.is_subscription_active(user))):
        token = await db.ensure_api_token_for_user(uid, message.from_user.first_name)
    if not token or not token.get("token"):
        await message.reply_text("Please use /start to register or renew your subscription, then search again.")
        return None
    verified = await verify_token(token["token"])
    if verified.get("subscription_expired") or verified.get("limit_exceeded"):
        await message.reply_text(
            "Your access has expired or your usage limit has been reached. Use /start or contact the administrator."
        )
        return None
    return token["token"]


async def send_card(message, doc, token):
    from wzgram.enums import ParseMode
    from wzgram.errors import FloodWait
    from Backend.helper.announcer import _build_caption, _build_markup, announcement_image_url, announcement_photo
    from Backend.helper.settings_manager import SettingsManager
    base = SettingsManager.current().base_url
    if not base:
        await message.reply_text("The server URL is not configured. Please contact the administrator.")
        return
    info = {**doc, "year": doc.get("release_year"), "rate": doc.get("rating")}
    caption = _build_caption(info)
    markup = _build_markup(info, include_addon=False)
    try:
        poster = await announcement_image_url(info)
        if poster:
            photo = await announcement_photo(poster)
            try:
                await message.reply_photo(photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=markup)
                return
            finally:
                photo.close()
    except FloodWait:
        raise
    except Exception:
        pass
    await message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True)


async def resolve_payload(db, payload):
    ref = parse_media_payload(payload)
    if ref:
        kind, tmdb_id = ref
        found = await db.find_media_doc(kind, tmdb_id)
        if found:
            return {**found[0], "media_type": kind, "db_index": found[1]}
        return None
    # Previously published file links now show cards too; never forward a file.
    if not re.fullmatch(r"file_[A-Za-z0-9_-]{32}", payload):
        return None
    return await db.find_media_by_announcement_file(payload[5:])


async def show_link(message, payload):
    from Backend import db

    token = await registered_token(message)
    if not token:
        return
    doc = await resolve_payload(db, payload)
    if not doc:
        await message.reply_text("This title is no longer available. Send a movie or series name to search.")
        return
    await send_card(message, doc, token)
