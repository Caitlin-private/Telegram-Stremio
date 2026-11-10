"""Stremio stream-side request handling.

A request prompt is shown only when the title itself is absent from the library.
Missing episodes/seasons of an already-indexed series do not create a title request.
"""
import asyncio
import re
from html import escape

from wzgram.enums import ParseMode
from wzgram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend import db
from Backend.config import Telegram
from Backend.helper.metadata.providers.cinemeta import get_detail
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot

_request_locks: dict[tuple, asyncio.Lock] = {}


def _media_kind(media_type: str) -> str:
    return "tv" if str(media_type or "").lower() in {"tv", "series"} else "movie"


async def _resolve_stream_media(media_id: str, media_type: str) -> dict | None:
    from Backend.fastapi.routes.stremio_routes import _parse_stremio_id
    parsed = _parse_stremio_id(media_id)
    kind = _media_kind(media_type)
    imdb_id = parsed.get("imdb_id")
    tmdb_id = parsed.get("tmdb_id")
    kitsu_id = parsed.get("kitsu_id")

    if imdb_id:
        try:
            detail = await get_detail(imdb_id, "series" if kind == "tv" else "movie")
        except Exception as exc:
            LOGGER.warning(f"[STREAM REQUEST] Cinemeta lookup failed for {imdb_id}: {exc}")
            detail = None
        if detail and detail.get("title"):
            year = (detail.get("releaseDetailed") or {}).get("year")
            return {
                "media_type": kind,
                "imdb_id": detail.get("id") or imdb_id,
                "tmdb_id": detail.get("moviedb_id"),
                "kitsu_id": kitsu_id,
                "title": str(detail.get("title")).strip(),
                "year": year,
                "poster": detail.get("poster") or "",
                "backdrop": detail.get("background") or "",
                "description": detail.get("plot") or "",
                "rate": (detail.get("rating") or {}).get("star", 0) if isinstance(detail.get("rating"), dict) else 0,
                "genres": detail.get("genre") or [],
                "season_num": parsed.get("season_num"),
                "episode_num": parsed.get("episode_num"),
            }

    if tmdb_id:
        try:
            from Backend.helper.metadata import get_tmdb_client, tmdb_api_key, format_tmdb_image
            if tmdb_api_key():
                client = get_tmdb_client()
                item = await (client.tv(int(tmdb_id)).details() if kind == "tv"
                              else client.movie(int(tmdb_id)).details())
                title = (getattr(item, "name", None) if kind == "tv"
                         else getattr(item, "title", None))
                release = (getattr(item, "first_air_date", None) if kind == "tv"
                           else getattr(item, "release_date", None))
                if title:
                    return {
                        "media_type": kind,
                        "imdb_id": None,
                        "tmdb_id": int(tmdb_id),
                        "kitsu_id": kitsu_id,
                        "title": str(title).strip(),
                        "year": getattr(release, "year", None),
                        "poster": format_tmdb_image(getattr(item, "poster_path", None), "w500"),
                        "backdrop": format_tmdb_image(getattr(item, "backdrop_path", None), "original"),
                        "description": getattr(item, "overview", None) or "",
                        "rate": getattr(item, "vote_average", 0) or 0,
                        "genres": [g.name for g in (getattr(item, "genres", None) or [])],
                        "season_num": parsed.get("season_num"),
                        "episode_num": parsed.get("episode_num"),
                    }
        except Exception as exc:
            LOGGER.warning(f"[STREAM REQUEST] TMDB lookup failed for {tmdb_id}: {exc}")
    return None


async def _series_episode_state(meta: dict):
    """Return True when the exact requested episode has streams, False when missing, None on lookup failure."""
    season_number = meta.get("season_num")
    episode_number = meta.get("episode_num")
    if meta.get("media_type") != "tv" or season_number is None or episode_number is None:
        return None
    try:
        doc = await db.get_media_details(
            imdb_id=meta.get("imdb_id"),
            kitsu_id=meta.get("kitsu_id"),
            tmdb_id=meta.get("tmdb_id"),
            media_type="tv",
        )
        if not doc:
            return None
        for season in (doc.get("seasons") or []):
            if int(season.get("season_number") or 0) != int(season_number):
                continue
            for episode in (season.get("episodes") or []):
                if int(episode.get("episode_number") or 0) == int(episode_number):
                    return any(q.get("id") for q in (episode.get("telegram") or []))
        return False
    except Exception as exc:
        LOGGER.error(f"[STREAM REQUEST] Episode existence check failed closed: {exc}")
        return None


async def _already_in_library(meta: dict) -> bool:
    """Fail closed and check the identity before offering a duplicate request."""
    try:
        kind = meta["media_type"]
        if meta.get("imdb_id"):
            # IMDb ids are cross-type identities, so inspect both collections.
            for collection_type in ("movie", "tv"):
                found = await db.get_media_details(
                    imdb_id=meta["imdb_id"], media_type=collection_type
                )
                if found:
                    return True
        if meta.get("tmdb_id"):
            # TMDB numeric IDs are type-scoped; never compare a movie id to a TV id.
            found = await db.find_media_doc(kind, int(meta["tmdb_id"]))
            if found:
                return True
        if meta.get("kitsu_id"):
            found = await db.get_media_details(kitsu_id=int(meta["kitsu_id"]), media_type="tv")
            if found:
                return True
        # Last-resort exact title/year check catches legacy documents with
        # synthetic or missing provider IDs. Search both media types.
        if meta.get("title"):
            found = await db.search_documents(query=meta["title"], page=1, page_size=12)
            target = re.sub(r"[^\w]+", " ", str(meta["title"]).casefold()).strip()
            wanted_year = str(meta.get("year") or "").strip()[:4]
            for item in (found.get("results") or []):
                candidate = re.sub(r"[^\w]+", " ", str(item.get("title") or "").casefold()).strip()
                if candidate != target:
                    continue
                candidate_year = str(item.get("release_year") or "").strip()[:4]
                if wanted_year and candidate_year and candidate_year != wanted_year:
                    continue
                return True
        return False
    except Exception as exc:
        LOGGER.error(f"[STREAM REQUEST] Library existence check failed closed: {exc}")
        return True


async def _request_scope(meta: dict) -> str | None:
    """Return 'episode', 'title', or None when the requested content is already present/uncertain."""
    has_episode = (meta.get("media_type") == "tv" and meta.get("season_num") is not None
                   and meta.get("episode_num") is not None)
    if has_episode:
        state = await _series_episode_state(meta)
        if state is True:
            return None
        if state is False:
            return "episode"
        # No parent document: request the series title. If a DB lookup failed,
        # _already_in_library fails closed and prevents an unsafe duplicate.
    return None if await _already_in_library(meta) else "title"


async def stream_request_prompt(token: str, media_id: str, media_type: str) -> dict | None:
    meta = await _resolve_stream_media(media_id, media_type)
    if not meta:
        return None
    scope = await _request_scope(meta)
    if scope is None:
        return None
    from urllib.parse import quote
    from Backend.helper.settings_manager import SettingsManager
    is_episode_request = scope == "episode"
    label = meta["title"] + (f" ({meta['year']})" if meta.get("year") else "")
    episode_label = f"S{int(meta['season_num']):02d}E{int(meta['episode_num']):02d}" if is_episode_request else ""
    return {
        "name": f"📩 Request {episode_label}" if is_episode_request else "📩 Request this title",
        "title": (f"{label} {episode_label} is missing. Tap to request this episode only." if is_episode_request
                  else f"{label} is not in the library. Tap to request it."),
        "externalUrl": f"{SettingsManager.current().base_url}/stremio/{token}/request-stream/{quote(media_id, safe=':')}?type={'series' if meta['media_type'] == 'tv' else 'movie'}",
        "behaviorHints": {"notWebReady": True},
    }


async def _notify_owner_admins(meta: dict, user_id, request_id: str, *, episode_scoped: bool = False) -> None:
    recipients = []
    for raw in [Telegram.OWNER_ID, *(getattr(Telegram, "APPROVER_IDS", []) or [])]:
        try:
            chat_id = int(raw)
        except (TypeError, ValueError):
            continue
        if chat_id > 0 and chat_id not in recipients:
            recipients.append(chat_id)
    if not recipients:
        return

    # Markdown inline-code isolates the exact canonical title so it can be copied
    # without surrounding labels, year text, or Markdown punctuation.
    title = str(meta.get("title") or "Unknown").replace("\\", "\\\\").replace("`", "\\`")
    uid = str(user_id if user_id is not None else "Unknown").replace("`", "\\`")
    year = str(meta.get("year") or "Unknown").replace("`", "\\`")
    message = (
        "🆕 *New Stremio content request*\n\n"
        f"User ID: `{uid}`\n"
        "Title:\n"
        f"`{title}`\n"
        f"Year: `{year}`"
    )
    if episode_scoped and meta.get("season_num") is not None and meta.get("episode_num") is not None:
        message += f"\nEpisode: `S{int(meta['season_num']):02d}E{int(meta['episode_num']):02d}`"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Done", callback_data=f"sreq:done:{request_id}"),
        InlineKeyboardButton("❌ Decline", callback_data=f"sreq:decline:{request_id}"),
    ]])
    from Backend.helper.requests_manager import add_admin_message
    for chat_id in recipients:
        try:
            sent = await StreamBot.send_message(chat_id, message, parse_mode=ParseMode.MARKDOWN,
                                                reply_markup=keyboard, disable_web_page_preview=True)
            await add_admin_message(request_id, chat_id, sent.id)
        except Exception as exc:
            LOGGER.warning(f"[STREAM REQUEST] Admin notification failed for {chat_id}: {exc}")


async def queue_stream_request(media_id: str, media_type: str, token_data: dict | None) -> dict:
    meta = await _resolve_stream_media(media_id, media_type)
    if not meta:
        return {"ok": False, "reason": "metadata_unresolved"}

    scope = await _request_scope(meta)
    if scope is None:
        return {"ok": True, "reason": "already_available", "title": meta["title"]}
    episode_scoped = scope == "episode"
    key = (meta["media_type"], meta.get("imdb_id") or meta.get("tmdb_id") or media_id,
           meta.get("season_num") if episode_scoped else None,
           meta.get("episode_num") if episode_scoped else None)
    lock = _request_locks.setdefault(key, asyncio.Lock())
    async with lock:
        # Re-check under the lock so concurrent Stremio retries cannot duplicate
        # the same DB request/owner notification or request content uploaded meanwhile.
        scope = await _request_scope(meta)
        if scope is None:
            return {"ok": True, "reason": "already_available", "title": meta["title"]}
        episode_scoped = scope == "episode"
        from Backend.helper.requests_manager import submit_request
        user_id = (token_data or {}).get("user_id")
        result = await submit_request(
            media_type=meta["media_type"],
            tmdb_id=meta.get("tmdb_id"),
            imdb_id=meta.get("imdb_id"),
            title=meta["title"],
            year=meta.get("year"),
            poster=meta.get("poster") or "",
            backdrop=meta.get("backdrop") or "",
            description=meta.get("description") or "",
            rate=meta.get("rate") or 0,
            genres=meta.get("genres") or [],
            client_ip=f"stremio-user:{user_id or 'unknown'}",
            library_checked=True,
            season_number=meta.get("season_num") if episode_scoped else None,
            episode_number=meta.get("episode_num") if episode_scoped else None,
            requester_user_id=user_id,
        )
        if result.get("reason") == "created" and result.get("request_id"):
            await _notify_owner_admins(meta, user_id, result["request_id"], episode_scoped=episode_scoped)
        return result

