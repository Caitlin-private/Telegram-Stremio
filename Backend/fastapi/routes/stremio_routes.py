import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote, unquote

import PTN
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant

import httpx

from Backend import __version__, db
from Backend.helper.database import META_PROJECTION
from Backend.config import Telegram
from Backend.helper.analytics import client_ip_from, record_client
from Backend.fastapi.security.tokens import can_stream_session, verify_token
from Backend.helper.cf_stream import cf_enabled, cf_stream_url
from Backend.fastapi.themes import DEFAULT_THEME, DEFAULT_STYLE, get_theme
from Backend.helper.poster_provider import _abs_media_url, _poster_url, _apply_fanart
from Backend.helper.global_search import global_search, is_global_search_enabled
from Backend.helper.metadata.providers.cinemeta import get_detail, get_season
from Backend.helper.metadata import COMBINED_SEASON, COMBINED_EPISODE_BASE, tmdb_api_key
from Backend.helper.database import convert_objectid_to_str
from Backend.helper.local_collections import (
    ensure_library_collection_index,
    get_cached_movie_collection_index,
    get_tmdb_ids_for_collection,
    tmdb_image_url,
)
from Backend.helper.split_files import parse_combined_episodes, combined_name_key
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.stream_formatter import format_stream
from Backend.helper.series_media import SERIES_PREFIX, series_id, original_id
from Backend.helper.subtitles import get_subtitles_for, stremio_subtitle_entries
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot, get_streambot_url

router = APIRouter(prefix="/stremio", tags=["Stremio Addon"])
templates = Jinja2Templates(directory="Backend/fastapi/templates")

#----- Addon configuration
ADDON_NAME = "Caitlin Stream"
ADDON_VERSION = __version__
PAGE_SIZE = 15

def build_proxy_url(original_url: str) -> str | None:
    settings = SettingsManager.current()
    base = settings.http_proxy_url
    if not base:
        return None
    if settings.mediaflow_proxy:
        url = f"{base.rstrip('/')}/proxy/stream?d={quote(original_url, safe='')}"
        if settings.mediaflow_password:
            url += f"&api_password={quote(settings.mediaflow_password, safe='')}"
        return url
    return f"{base}{original_url}"

_membership_cache: dict = {}
_MEMBERSHIP_TTL = 60
_MEMBERSHIP_CACHE_MAX = 5000


#----- Drop cached membership results for one user or all users
def invalidate_membership_cache(user_id: int | None = None) -> None:
    if user_id is None:
        _membership_cache.clear()
        return
    for key in [k for k in _membership_cache if k[1] == user_id]:
        _membership_cache.pop(key, None)


#----- Effective (mode, allowed_tokens) for a title, honouring per-item overrides
def _effective_visibility(catalog: dict, item: dict) -> tuple:
    if item.get("visibility") in ("public", "tokens", "owner"):
        return item["visibility"], (item.get("allowed_tokens") or [])
    return (catalog.get("visibility") or "public"), (catalog.get("allowed_tokens") or [])


#----- Whether a token may see content with the given visibility
def _token_can_view(mode: str, allowed_tokens: list, token_data: Optional[dict] = None) -> bool:
    if not token_data:
        return mode == "public"
    user_id = token_data.get("user_id")
    try:
        if user_id is not None and int(user_id) == int(Telegram.OWNER_ID):
            return True
    except (TypeError, ValueError):
        pass
    if mode == "owner":
        return False
    if mode == "tokens":
        return token_data.get("token") in (allowed_tokens or [])
    if SettingsManager.current().subscription:
        return not token_data.get("subscription_expired")
    return True


#----- Mongo filter that hides owner-only / restricted titles from a token
def _visibility_query(token_data: Optional[dict] = None) -> dict:
    if not token_data:
        return {"$or": [
            {"visibility": {"$exists": False}},
            {"visibility": "public"},
        ]}
    user_id = token_data.get("user_id")
    try:
        if user_id is not None and int(user_id) == int(Telegram.OWNER_ID):
            return {}
    except (TypeError, ValueError):
        pass
    return {"$or": [
        {"visibility": {"$exists": False}},
        {"visibility": "public"},
        {"visibility": "tokens", "allowed_tokens": token_data.get("token")},
    ]}


#----- Hide titles locked to a single catalog from default listings / search
def _not_exclusive_clause(allow_searchable: bool = False) -> dict:
    ors = [{"exclusive_catalog_id": {"$exists": False}}, {"exclusive_catalog_id": None}]
    if allow_searchable:
        ors.append({"exclusive_searchable": True})
    return {"$or": ors}


#----- Combine non-empty Mongo filters under a single $and
def _merge_filters(*filters) -> dict:
    parts = [f for f in filters if f]
    if not parts:
        return {}
    return parts[0] if len(parts) == 1 else {"$and": parts}


def _display_library_collection_name(name: Optional[str]) -> str:
    """Keep tile names compact by removing TMDB's generic trailing 'Collection'."""
    original = str(name or "Movie Collection").strip()
    display = re.sub(r"\s+collection$", "", original, flags=re.IGNORECASE).strip()
    if not display or display.casefold() in {"the", "a", "an"}:
        return original
    return display


async def _build_library_collection_groups(token_data: Optional[dict] = None) -> list[dict]:
    """Return franchise groups represented by visible, locally stored movies only."""
    token_data = token_data or {}
    # The first request kicks off a persisted background backfill. Wait briefly for a
    # small initial batch, never for a full-library TMDB scan.
    await ensure_library_collection_index(wait_seconds=2.0)
    collection_index = await get_cached_movie_collection_index()
    if not collection_index:
        return []

    eligible_ids = list(collection_index.keys())
    query = _merge_filters(
        {"tmdb_id": {"$in": eligible_ids}},
        _visibility_query(token_data),
        _not_exclusive_clause(),
    )
    by_tmdb_id: dict[int, dict] = {}
    storage_keys = [key for key in db.dbs if str(key).startswith("storage_")]
    storage_keys.sort(
        key=lambda key: int(str(key).split("_", 1)[1]) if str(key).split("_", 1)[1].isdigit() else 0,
        reverse=True,
    )

    for db_key in storage_keys:
        try:
            cursor = db.dbs[db_key]["movie"].find(query)
            async for item in cursor:
                try:
                    tmdb_id = int(item.get("tmdb_id"))
                except (TypeError, ValueError):
                    continue
                if tmdb_id not in collection_index or tmdb_id in by_tmdb_id:
                    continue
                if not _token_can_view(
                    item.get("visibility") or "public",
                    item.get("allowed_tokens") or [],
                    token_data,
                ):
                    continue
                by_tmdb_id[tmdb_id] = convert_objectid_to_str(item)
        except Exception as exc:
            LOGGER.warning(f"[Library Collections] Could not query {db_key}: {exc}")

    grouped: dict[int, dict] = {}
    for tmdb_id, item in by_tmdb_id.items():
        metadata = collection_index.get(tmdb_id) or {}
        try:
            collection_id = int(metadata.get("collection_id"))
        except (TypeError, ValueError):
            continue
        if collection_id <= 0:
            continue
        group = grouped.setdefault(collection_id, {
            "collection_id": collection_id,
            "title": _display_library_collection_name(metadata.get("collection_name")),
            "poster_path": metadata.get("poster_path"),
            "backdrop_path": metadata.get("backdrop_path"),
            "items": [],
        })
        group["items"].append(item)
        if not group.get("poster_path") and metadata.get("poster_path"):
            group["poster_path"] = metadata.get("poster_path")
        if not group.get("backdrop_path") and metadata.get("backdrop_path"):
            group["backdrop_path"] = metadata.get("backdrop_path")

    def _year_key(item: dict):
        try:
            year = int(item.get("release_year"))
        except (TypeError, ValueError):
            year = 9999
        return (year, (item.get("title") or "").casefold())

    groups = []
    for collection_id, group in grouped.items():
        # Only expose franchises where at least 2 titles exist in the library
        # to avoid cluttering Nuvio with single-movie "collections".
        if len(group["items"]) < 2:
            continue

        group["items"].sort(key=_year_key)
        local_poster = next((it.get("poster") for it in group["items"] if it.get("poster")), None)
        local_backdrop = next((it.get("backdrop") for it in group["items"] if it.get("backdrop")), None)
        group["poster"] = tmdb_image_url(group.get("poster_path")) or (
            tmdb_image_url(local_poster) if isinstance(local_poster, str) and local_poster.startswith("/") else local_poster
        )
        group["backdrop"] = tmdb_image_url(group.get("backdrop_path"), "w1280") or (
            tmdb_image_url(local_backdrop, "w1280") if isinstance(local_backdrop, str) and local_backdrop.startswith("/") else local_backdrop
        )
        group["catalog_id"] = f"library_collection_{collection_id}"
        group["folder_id"] = f"library-collection-{collection_id}"
        groups.append(group)

    groups.sort(key=lambda group: (str(group.get("title") or "").casefold(), group["collection_id"]))
    return groups


def _library_collection_folder(group: dict, addon_id: str) -> dict:
    """Nuvio folder representation for one dynamic, DB-backed movie franchise."""
    catalog_id = group["catalog_id"]
    source = {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": catalog_id}
    folder = {
        "id": group["folder_id"],
        "title": group["title"],
        "sources": [dict(source)],
        "hideTitle": False,
        "tileShape": "landscape",
        "catalogSources": [{"type": "movie", "addonId": addon_id, "catalogId": catalog_id}],
        "focusGifEnabled": False,
    }
    if group.get("poster"):
        folder["coverImageUrl"] = group["poster"]
    if group.get("backdrop"):
        folder["heroBackdropUrl"] = group["backdrop"]
    return folder


_TMDB_TRENDING_CACHE: dict = {}
_TMDB_TRENDING_LOCKS: dict = {}
_TMDB_TRENDING_TTL = 600  # 10 minutes cache


async def _get_tmdb_trending_ids(media_type: str, time_window: str = "day") -> list[int]:
    """Fetch daily trending TMDB IDs (cached for 10 minutes)."""
    api_key = tmdb_api_key()
    if not api_key:
        return []

    cache_key = (media_type, time_window)
    now = time.monotonic()
    cached = _TMDB_TRENDING_CACHE.get(cache_key)
    if cached and now - cached[0] < _TMDB_TRENDING_TTL:
        return list(cached[1])

    lock = _TMDB_TRENDING_LOCKS.setdefault(cache_key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = _TMDB_TRENDING_CACHE.get(cache_key)
        if cached and now - cached[0] < _TMDB_TRENDING_TTL:
            return list(cached[1])

        ids: list[int] = []
        try:
            url = f"https://api.themoviedb.org/3/trending/{media_type}/{time_window}"
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params={"api_key": api_key, "page": 1})
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data.get("results") or []:
                        raw_id = item.get("id")
                        if raw_id:
                            try:
                                ids.append(int(raw_id))
                            except (TypeError, ValueError):
                                pass
        except Exception as exc:
            LOGGER.warning(f"[TMDB Trending] Fetch failed for {media_type}: {exc}")
            if cached:
                return list(cached[1])
            return []

        if ids:
            _TMDB_TRENDING_CACHE[cache_key] = (time.monotonic(), ids)
        return ids


async def _get_db_matches_in_tmdb_order(
    media_type: str, tmdb_ids: list[int], token_data: dict, genre_filter: Optional[str] = None
) -> list[dict]:
    """Match local MongoDB records for TMDB IDs while preserving exact TMDB trending rank."""
    if not tmdb_ids:
        return []

    collection_name = "tv" if media_type == "series" else "movie"
    query_parts = [
        {"tmdb_id": {"$in": tmdb_ids}},
        _visibility_query(token_data),
        _not_exclusive_clause(),
    ]
    if genre_filter:
        query_parts.append({"genres": {"$in": [genre_filter]}})
    query = _merge_filters(*query_parts)

    by_tmdb_id: dict[int, dict] = {}
    storage_keys = [key for key in db.dbs if str(key).startswith("storage_")]
    storage_keys.sort(key=lambda k: int(str(k).split("_", 1)[1]) if str(k).split("_", 1)[1].isdigit() else 0, reverse=True)

    for db_key in storage_keys:
        try:
            cursor = db.dbs[db_key][collection_name].find(query, META_PROJECTION)
            async for item in cursor:
                try:
                    t_id = int(item.get("tmdb_id"))
                except (TypeError, ValueError):
                    continue
                if t_id in by_tmdb_id:
                    continue
                if not _token_can_view(
                    item.get("visibility") or "public",
                    item.get("allowed_tokens") or [],
                    token_data,
                ):
                    continue
                by_tmdb_id[t_id] = convert_objectid_to_str(item)
        except Exception as exc:
            LOGGER.warning(f"[TMDB Trending] DB error querying {db_key}: {exc}")

    # Preserve the exact ranking from TMDB
    ordered = [by_tmdb_id[tid] for tid in tmdb_ids if tid in by_tmdb_id]
    return ordered


def _parse_stremio_id(id: str):
    id = original_id(id)
    parts = id.split(":")
    is_kitsu = parts and parts[0].lower() == "kitsu"
    imdb_id = None
    kitsu_id = None
    season_num = None
    episode_num = None
    absolute_episode = None

    if is_kitsu:
        try:
            kitsu_id = int(parts[1]) if len(parts) > 1 else None
        except (TypeError, ValueError):
            kitsu_id = None
        if len(parts) == 3:
            try:
                absolute_episode = int(parts[2])
            except (TypeError, ValueError):
                absolute_episode = None
            episode_num = absolute_episode
        elif len(parts) >= 4:
            try:
                season_num = int(parts[2]) if parts[2] not in ("", "null", "None") else None
            except (TypeError, ValueError):
                season_num = None
            try:
                episode_num = int(parts[3]) if parts[3] not in ("", "null", "None") else None
            except (TypeError, ValueError):
                episode_num = None
            if season_num is None and episode_num is not None:
                absolute_episode = episode_num
    else:
        imdb_id = parts[0] if parts else id
        try:
            season_num = int(parts[1]) if len(parts) > 1 and parts[1] not in ("", "null", "None") else None
        except (TypeError, ValueError):
            season_num = None
        try:
            episode_num = int(parts[2]) if len(parts) > 2 and parts[2] not in ("", "null", "None") else None
        except (TypeError, ValueError):
            episode_num = None

    return {
        "imdb_id": imdb_id,
        "kitsu_id": kitsu_id,
        "season_num": season_num,
        "episode_num": episode_num,
        "absolute_episode": absolute_episode,
        "is_kitsu": is_kitsu,
    }


async def _title_allowed(imdb_id: str = None, token_data: dict = None, kitsu_id: int = None) -> bool:
    doc = await db.get_media_details(imdb_id=imdb_id, kitsu_id=kitsu_id, projection={"visibility": 1, "allowed_tokens": 1})
    if not doc:
        return True
    visible = await db.get_media_details(imdb_id=imdb_id, kitsu_id=kitsu_id,
        can_view=lambda d: _token_can_view(d.get('visibility') or 'public', d.get('allowed_tokens') or [], token_data))
    return visible is not None


#----- Available catalog genres
GENRES = [
    "Action", "Adventure", "Animation", "Biography", "Comedy",
    "Crime", "Documentary", "Drama", "Family", "Fantasy",
    "History", "Horror", "Music", "Mystery", "Romance",
    "Sci-Fi", "Sport", "Thriller", "War", "Western"
]


def _year_label(item: dict) -> str:
    """Single year or range for Stremio releaseInfo (e.g. 1999-2024)."""
    start = item.get("release_year")
    end = item.get("release_year_end")
    if not start:
        return ""
    try:
        start_i = int(start)
    except (TypeError, ValueError):
        return str(start)
    if end:
        try:
            end_i = int(end)
            if end_i > start_i:
                return f"{start_i}-{end_i}"
        except (TypeError, ValueError):
            pass
    return str(start_i)


#----- Map an internal media item into a Stremio meta object
def _display_title(item: dict) -> str:
    """Prefer English title when available, else canonical title."""
    eng = (item.get("title_english") or "").strip()
    title = (item.get("title") or "").strip()
    if eng and eng.lower() != title.lower():
        return eng
    return eng or title or "Unknown"


def _safe_moviedb_id(item: dict):
    """Never emit null/None for moviedb_id (breaks some Stremio clients)."""
    v = item.get("tmdb_id")
    if v is None or str(v).strip().lower() in ("", "null", "none"):
        return ""
    try:
        return int(v)
    except (TypeError, ValueError):
        return str(v)


def convert_to_stremio_meta(item: dict) -> dict:
    media_type = "series" if item.get("media_type") == "tv" else "movie"
    imdb = item.get("imdb_id") or ""
    meta = {
        "id": series_id(imdb) if media_type == 'series' else imdb,
        "type": media_type,
        "name": _display_title(item),
        "poster": _poster_url(imdb, item.get("poster"), item.get('tmdb_id'), media_type),
        "logo": item.get("logo") or "",
        "year": _year_label(item) or item.get("release_year") or "",
        "releaseInfo": _year_label(item) or "",
        "imdb_id": imdb,
        "moviedb_id": _safe_moviedb_id(item),
        "background": _abs_media_url(item.get("backdrop")),
        "genres": item.get("genres") or [],
        "imdbRating": str(item.get("rating") or "") if item.get("rating") not in (None, "") else "",
        "description": item.get("description") or "",
        "cast": item.get("cast") or [],
        "runtime": item.get("runtime") or "",
    }
    return meta


#----- Format a movie release date as an ISO string, or None
def format_released_date(media):
    year = media.get("release_year")
    if year:
        try:
            y = int(str(year)[:4])
            return datetime(y, 1, 1).isoformat() + "Z"
        except Exception:
            return None
    return None


#----- Build a Stremio stream display name/title from a filename
def format_stream_details(filename: str, quality: str, size: str, is_split: bool = False) -> tuple[str, str]:
    size_emoji = "📦" if is_split else "💾"
    try:
        parsed = PTN.parse(filename)
    except Exception:
        return (f"{ADDON_NAME} {quality}", f"📁 {filename}\n{size_emoji} {size}")

    codec_parts = []
    if parsed.get("codec"):
        codec_parts.append(f"🎥 {parsed.get('codec')}")
    if parsed.get("bitDepth"):
        codec_parts.append(f"🌈 {parsed.get('bitDepth')}bit")
    if parsed.get("audio"):
        codec_parts.append(f"🔊 {parsed.get('audio')}")
    if parsed.get("encoder"):
        codec_parts.append(f"👤 {parsed.get('encoder')}")

    codec_info = " ".join(codec_parts) if codec_parts else ""

    resolution = parsed.get("resolution", quality)
    quality_type = parsed.get("quality", "")
    stream_name = f"{ADDON_NAME} {resolution} {quality_type}".strip()

    stream_title_parts = [
        f"📁 {filename}",
        f"{size_emoji} {size}",
    ]
    if codec_info:
        stream_title_parts.append(codec_info)

    stream_title = "\n".join(stream_title_parts)
    return (stream_name, stream_title)


def parse_size_to_bytes(size_str: str) -> int:
    if not size_str:
        return 0
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", size_str.strip())
    if not match:
        return 0
    value, unit = float(match.group(1)), match.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(value * multipliers.get(unit, 1))


def get_resolution_priority(stream_name: str) -> int:
    from Backend.helper.ingestion_rules import resolution_hint
    hint = resolution_hint(stream_name)
    if hint:
        return int(hint[:-1])
    return 480 if re.search(r'(?i)(?<![a-z0-9])sd(?![a-z0-9])', stream_name) else 1


#----- Canonical quality label used by per-token quality filtering
def stream_res_label(stream_name: str) -> str:
    return {2160: "4K", 1080: "1080p", 720: "720p", 540: "540p", 480: "480p", 360: "360p"}.get(
        get_resolution_priority(stream_name), "other"
    )


#----- Manifest describing the addon's catalogs/resources for this token
@router.get("/{token}/manifest.json")
async def get_manifest(token: str, token_data: dict = Depends(verify_token)):
    if SettingsManager.current().hide_catalog:
        resources = ["stream", "subtitles"]
        catalogs = []
    else:
        resources = ["catalog", "meta", "stream", "subtitles"]
        catalogs = [
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Latest",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "movie",
                "id": "top_movies",
                "name": "Popular",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            },
            {
                "type": "series",
                "id": "latest_series",
                "name": "Latest",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "series",
                "id": "top_series",
                "name": "Popular",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"},
                    {"name": "search", "isRequired": False}
                ],
                "extraSupported": ["genre", "skip", "search"]
            },
            {
                "type": "movie",
                "id": "trending_movies",
                "name": "Trending",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            },
            {
                "type": "series",
                "id": "trending_series",
                "name": "Trending",
                "extra": [
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip"}
                ],
                "extraSupported": ["genre", "skip"]
            }
        ]

        try:
            custom_catalogs = await db.get_custom_catalogs(slim=True)
            for catalog in custom_catalogs:
                visible_items = [
                    i for i in (catalog.get("items") or [])
                    if _token_can_view(*_effective_visibility(catalog, i), token_data)
                ]
                has_movie = any(i.get("media_type") == "movie" for i in visible_items)
                has_series = any(i.get("media_type") == "tv" for i in visible_items)
                if not has_movie and not has_series:
                    continue
                catalog_id = str(catalog.get("_id"))
                catalog_name = catalog.get("name") or "Custom Catalog"
                if has_movie:
                    catalogs.append({
                        "type": "movie",
                        "id": f"custom_{catalog_id}",
                        "name": catalog_name,
                        "extra": [{"name": "search", "isRequired": False}, {"name": "skip"}],
                        "extraSupported": ["search", "skip"],
                    })
                if has_series:
                    catalogs.append({
                        "type": "series",
                        "id": f"custom_{catalog_id}",
                        "name": catalog_name,
                        "extra": [{"name": "search", "isRequired": False}, {"name": "skip"}],
                        "extraSupported": ["search", "skip"],
                    })
        except Exception:
            pass

        try:
            order = await db.get_catalog_order()
            token_order = (token_data.get("config") or {}).get("catalog_order") or []
            effective = token_order or order
            if effective:
                rank = {k: i for i, k in enumerate(effective)}

                def _crank(c):
                    key = f"{c.get('id')}::{c.get('type')}"
                    return rank.get(key, rank.get(c.get("id"), len(effective) + 1))

                catalogs.sort(key=_crank)
            hidden = set((token_data.get("config") or {}).get("hidden_catalogs") or [])
            if hidden:
                catalogs = [
                    c for c in catalogs
                    if c.get("id") not in hidden and f"{c.get('id')}::{c.get('type')}" not in hidden
                ]
        except Exception:
            pass


    addon_name = ADDON_NAME
    addon_desc = "Streams movies and series from your Telegram."
    addon_version = ADDON_VERSION

    #----- Show expiry info in the addon: token's own expiry first, else the subscription
    try:
        expiry_obj = token_data.get("expires_at")
        if expiry_obj is None and SettingsManager.current().subscription:
            user_id = token_data.get("user_id")
            if user_id:
                user = await db.get_user(int(user_id))
                if user and user.get("subscription_status") == "active":
                    expiry_obj = user.get("subscription_expiry")

        if expiry_obj:
            expiry_str = expiry_obj.strftime("%d %b %Y").lstrip("0")
            addon_desc = (
                f"📅 Access active until {expiry_str}.\n"
                f"Streams movies and series from your Telegram."
            )
            epoch_tag = format(int(expiry_obj.timestamp()) & 0xFFFF, "x")
            addon_version = f"{ADDON_VERSION}-{epoch_tag}"
    except Exception:
        pass

    return {
        "id": f"telegram.media.{token[:8]}",
        "version": addon_version,
        "name": addon_name,
        "logo": f"{SettingsManager.current().base_url.rstrip('/')}/static/caitlinstream-logo.png?v={ADDON_VERSION}",
        "description": addon_desc,
        "types": ["movie", "series"],
        "resources": resources,
        "catalogs": catalogs,
        "idPrefixes": ["tt", "tg", "kitsu"],
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": False
        },
        "config": [
            {
                "key": "manifest_url",
                "title": "Your Addon URL (copy to reinstall)",
                "type": "text",
                "default": f"{SettingsManager.current().base_url}/stremio/{token}/manifest.json"
            }
        ]
    }


def _sports_collection() -> dict:
    """Nuvio collection definition for Sports Streams (community.sports.fly)."""
    return {
        "id": "sports-highfly-collection",
        "title": "Live Sports 🏟️",
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True,
        "folders": [
            {
                "id": "sports-folder-live-now",
                "title": "Live Now 🔴",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1516450360452-9312f5e86fc7?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1516450360452-9312f5e86fc7?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_live"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_live"}]
            },
            {
                "id": "sports-folder-today",
                "title": "Today's Matches 📅",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1522778119026-d647f0596c20?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1522778119026-d647f0596c20?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_today"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_today"}]
            },
            {
                "id": "sports-folder-football",
                "title": "Football / Soccer ⚽",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1574629810360-7efbbe195018?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1574629810360-7efbbe195018?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_football"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_football"}]
            },
            {
                "id": "sports-folder-cricket",
                "title": "Cricket 🏏",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1531415074968-036ba1b575da?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1531415074968-036ba1b575da?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_cricket"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_cricket"}]
            },
            {
                "id": "sports-folder-basketball",
                "title": "Basketball 🏀",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1574623452334-1e0ac2b3ccb4?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1574623452334-1e0ac2b3ccb4?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_basketball"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_basketball"}]
            },
            {
                "id": "sports-folder-fight",
                "title": "Fight (UFC / Boxing) 🥊",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1517438322307-e67111335449?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1517438322307-e67111335449?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_fight"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_fight"}]
            },
            {
                "id": "sports-folder-motor-sports",
                "title": "Motor Sports (F1 / MotoGP) 🏎️",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1568605117036-5fe5e7bab0b7?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1568605117036-5fe5e7bab0b7?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_motor_sports"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_motor_sports"}]
            },
            {
                "id": "sports-folder-tennis",
                "title": "Tennis 🎾",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1622279457486-62dcc4a431d6?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1622279457486-62dcc4a431d6?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_tennis"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_tennis"}]
            },
            {
                "id": "sports-folder-american-football",
                "title": "American Football (NFL) 🏈",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1587280501635-68a0e82cd5ff?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1587280501635-68a0e82cd5ff?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_american_football"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_american_football"}]
            },
            {
                "id": "sports-folder-hockey",
                "title": "Hockey 🏒",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1588497859490-85d1c17db96d?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1588497859490-85d1c17db96d?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_hockey"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_hockey"}]
            },
            {
                "id": "sports-folder-baseball",
                "title": "Baseball (MLB) ⚾",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1593784991095-a205069470b6?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1593784991095-a205069470b6?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_baseball"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_baseball"}]
            },
            {
                "id": "sports-folder-rugby",
                "title": "Rugby 🏉",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1544698310-74ea9d1c8258?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1544698310-74ea9d1c8258?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_rugby"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_rugby"}]
            },
            {
                "id": "sports-folder-golf",
                "title": "Golf ⛳",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1535131749006-b7f58c99034b?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1535131749006-b7f58c99034b?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_golf"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_golf"}]
            },
            {
                "id": "sports-folder-darts",
                "title": "Darts 🎯",
                "tileShape": "landscape",
                "hideTitle": False,
                "focusGifEnabled": False,
                "coverImageUrl": "https://images.unsplash.com/photo-1614036417651-efe5912149d8?q=80&w=1280&auto=format&fit=crop",
                "heroBackdropUrl": "https://images.unsplash.com/photo-1614036417651-efe5912149d8?q=80&w=1920&auto=format&fit=crop",
                "sources": [{"type": "sport", "addonId": "community.sports.fly", "provider": "addon", "catalogId": "sports_darts"}],
                "catalogSources": [{"type": "sport", "addonId": "community.sports.fly", "catalogId": "sports_darts"}]
            }
        ]
    }


async def build_nuvio_collection_data(token: str, collection_type: str = "full", token_data: Optional[dict] = None) -> list:
    addon_id = f"telegram.media.{token[:8]}"

    if token_data is None and token:
        try:
            token_data = await db.get_api_token(token)
        except Exception:
            token_data = None

    # Retired collection types intentionally return empty without touching MongoDB.
    if collection_type in {"explore", "library", "local"}:
        return []
    if collection_type in {"sports", "sports_only"}:
        return [_sports_collection()]

    # Map catalog names to catalog IDs for this token, separated by media type
    catalog_movies_map = {}
    catalog_series_map = {}
    auto_key_movies_map = {}
    auto_key_series_map = {}

    try:
        custom_catalogs = await db.get_custom_catalogs(slim=True)
        for catalog in custom_catalogs:
            # Check visibility and items
            raw_items = catalog.get("items") or []
            visible_items = [
                i for i in raw_items
                if _token_can_view(*_effective_visibility(catalog, i), token_data)
            ]

            has_movie = any(i.get("media_type") == "movie" for i in visible_items)
            has_series = any(i.get("media_type") == "tv" for i in visible_items)

            cid = f"custom_{catalog.get('_id')}"
            name = (catalog.get("name") or "").lower().strip()
            auto_key = (catalog.get("auto_key") or "").lower().strip()

            if has_movie:
                catalog_movies_map[name] = cid
                if auto_key:
                    auto_key_movies_map[auto_key] = cid
            if has_series:
                catalog_series_map[name] = cid
                if auto_key:
                    auto_key_series_map[auto_key] = cid
    except Exception:
        pass

    # Helper to get catalog ID for movies and series only if they actually have content
    def get_cat_id(name_query: str, auto_key: str = "", media_type: str = "movie", fallback: Optional[str] = None, strict: bool = True):
        cmap = catalog_movies_map if media_type == "movie" else catalog_series_map
        amap = auto_key_movies_map if media_type == "movie" else auto_key_series_map
        if auto_key and auto_key.lower() in amap:
            return amap[auto_key.lower()]
        for name, cid in cmap.items():
            if name_query.lower() in name:
                return cid
        return None if strict else fallback

    hotstar_movie = get_cat_id("hotstar", "auto_hotstar", "movie", strict=True)
    hotstar_series = get_cat_id("hotstar", "auto_hotstar", "series", strict=True)
    netflix_movie = get_cat_id("netflix", "auto_netflix", "movie", strict=True)
    netflix_series = get_cat_id("netflix", "auto_netflix", "series", strict=True)
    prime_movie = get_cat_id("prime", "auto_prime_video", "movie", strict=True)
    prime_series = get_cat_id("prime", "auto_prime_video", "series", strict=True)
    apple_movie = get_cat_id("apple", "auto_apple_tv", "movie", strict=True)
    apple_series = get_cat_id("apple", "auto_apple_tv", "series", strict=True)
    hbo_movie = get_cat_id("hbo", "auto_hbo", "movie", strict=True)
    hbo_series = get_cat_id("hbo", "auto_hbo", "series", strict=True)
    hulu_movie = get_cat_id("hulu", "auto_hulu", "movie", strict=True)
    hulu_series = get_cat_id("hulu", "auto_hulu", "series", strict=True)
    mx_movie = get_cat_id("mx", "auto_mx_player", "movie", strict=True)
    mx_series = get_cat_id("mx", "auto_mx_player", "series", strict=True)
    sony_movie = get_cat_id("sony", "auto_sonyliv", "movie", strict=True)
    sony_series = get_cat_id("sony", "auto_sonyliv", "series", strict=True)
    zee5_movie = get_cat_id("zee", "auto_zee5", "movie", strict=True)
    zee5_series = get_cat_id("zee", "auto_zee5", "series", strict=True)
    crunchyroll_movie = get_cat_id("crunchyroll", "auto_crunchyroll", "movie", strict=True)
    crunchyroll_series = get_cat_id("crunchyroll", "auto_crunchyroll", "series", strict=True)
    bollywood_movie = get_cat_id("bollywood", "auto_bollywood", "movie", fallback="latest_movies", strict=False)
    bollywood_series = get_cat_id("bollywood", "auto_bollywood", "series", fallback="latest_series", strict=False)

    ott_collection = {
        "id": "collection-erfs5gwk-community",
        "title": "Streaming Services",
        "folders": [
          {
            "id": "68ba6673-3480-4601-980e-4f69862777c3",
            "title": "Trending Now",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": "trending_movies"},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": "trending_series"},
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": "latest_movies"},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": "latest_series"}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/2774e91b86.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/2774e91b86.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": "trending_movies"},
              {"type": "series", "addonId": addon_id, "catalogId": "trending_series"},
              {"type": "movie", "addonId": addon_id, "catalogId": "latest_movies"},
              {"type": "series", "addonId": addon_id, "catalogId": "latest_series"}
            ],
            "focusGifEnabled": True
          },
          {
            "id": "folder-8ZBZOMW9",
            "title": "JioHotstar",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hotstar_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hotstar_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/focused/disney-plus.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/disney-plus.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/04d275e49b.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hotstar_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hotstar_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/disney-plus.webp"
          },
          {
            "id": "folder-WFGY1XPD",
            "title": "Netflix",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": netflix_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": netflix_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/59916efcd2.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/netflix.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/73a7509d9f.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": netflix_movie},
              {"type": "series", "addonId": addon_id, "catalogId": netflix_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/netflix.webp"
          },
          {
            "id": "folder-AT8KYHZO",
            "title": "Amazon Prime Video",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": prime_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": prime_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/focused/prime-video.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/prime-video.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/cover/prime-video.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": prime_movie},
              {"type": "series", "addonId": addon_id, "catalogId": prime_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/prime-video.webp"
          },
          {
            "id": "folder-IQTF27K8",
            "title": "Apple TV",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": apple_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": apple_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/291311c88b.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/apple-tv.webp",
            "coverImageUrl": "https://imkaptain.github.io/nuvio-assets/assets/images/4d8f0a8f.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": apple_movie},
              {"type": "series", "addonId": addon_id, "catalogId": apple_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/apple-tv.webp"
          },
          {
            "id": "folder-OA4T8NYO",
            "title": "HBO Max",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hbo_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hbo_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/6b439cb84a.webp",
            "titleLogoUrl": "https://imkaptain.github.io/nuvio-assets/assets/images/6e4c10d3.webp",
            "coverImageUrl": "https://imkaptain.github.io/nuvio-assets/assets/images/b59babf1.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hbo_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hbo_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://raw.githubusercontent.com/bramst0ne/prism-wallpapers/main/collections/providers/1899-hbo-max/backdrops/t2_flat_1080p.jpg"
          },
          {
            "id": "folder-LN9SPMQN",
            "title": "Hulu",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": hulu_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": hulu_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/focused/hulu.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/hulu.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/cover/hulu.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": hulu_movie},
              {"type": "series", "addonId": addon_id, "catalogId": hulu_series}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/hulu.webp"
          },
          {
            "id": "85a10640-79f2-435f-9d63-348e921e37f4",
            "title": "MX Player",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": mx_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": mx_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/739cc2d16c.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/9cfdcd25ba.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": mx_movie},
              {"type": "series", "addonId": addon_id, "catalogId": mx_series}
            ],
            "focusGifEnabled": True
          },
          {
            "id": "f6409d11-7630-4fa4-b6e6-48774e4df89e",
            "title": "Sony LIV",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": sony_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": sony_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/6b309267fc.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/cf8b51931f.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": sony_movie},
              {"type": "series", "addonId": addon_id, "catalogId": sony_series}
            ],
            "focusGifEnabled": True
          },
          {
            "id": "008c0c7c-cfe1-43f4-9630-8afd72b7cb5f",
            "title": "Zee5",
            "sources": [
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": zee5_movie},
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": zee5_series}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/364ce2abf4.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/364ce2abf4.webp",
            "catalogSources": [
              {"type": "movie", "addonId": addon_id, "catalogId": zee5_movie},
              {"type": "series", "addonId": addon_id, "catalogId": zee5_series}
            ],
            "focusGifEnabled": True
          },
          {
            "id": "cf7a3d79-0cc9-42c5-b460-d2e3b838ecfc",
            "title": "Crunchyroll",
            "sources": [
              {"type": "series", "addonId": addon_id, "provider": "addon", "catalogId": crunchyroll_series},
              {"type": "movie", "addonId": addon_id, "provider": "addon", "catalogId": crunchyroll_movie}
            ],
            "hideTitle": False,
            "tileShape": "landscape",
            "focusGifUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/faff458914.webp",
            "titleLogoUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/title/crunchyroll.webp",
            "coverImageUrl": "https://cdn.jsdelivr.net/gh/weebzone/Telegram-Stremio@master/assets/nuvio/f6456696da.webp",
            "catalogSources": [
              {"type": "series", "addonId": addon_id, "catalogId": crunchyroll_series},
              {"type": "movie", "addonId": addon_id, "catalogId": crunchyroll_movie}
            ],
            "focusGifEnabled": True,
            "heroBackdropUrl": "https://cdn.jsdelivr.net/gh/luckynumb3rs/stremio-perfect-setup/collections/streaming/backdrop/crunchyroll.webp"
          }
        ],
        "pinToTop": True,
        "viewMode": "TABBED_GRID",
        "showAllTab": True,
        "focusGlowEnabled": True
    }

    # Auto-hide OTT folders whose catalogs are not enabled/synced in database
    cleaned_folders = []
    for f in ott_collection.get("folders") or []:
        sources = [s for s in f.get("sources") or [] if s.get("catalogId")]
        catalog_sources = [cs for cs in f.get("catalogSources") or [] if cs.get("catalogId")]
        if sources or f.get("id") == "68ba6673-3480-4601-980e-4f69862777c3":  # Keep Trending Now
            folder_copy = dict(f)
            folder_copy["sources"] = sources or f.get("sources")
            folder_copy["catalogSources"] = catalog_sources or f.get("catalogSources")
            cleaned_folders.append(folder_copy)
    cleaned_folders.sort(key=lambda f: f.get("id") == "68ba6673-3480-4601-980e-4f69862777c3")
    ott_collection["folders"] = cleaned_folders

    if collection_type == "ott":
        return [ott_collection]

    # Keep collection endpoints deliberately lightweight: only curated OTT folders
    # and the dedicated live-sports collection are exposed. Legacy explore/library
    # URLs remain valid but no longer trigger franchise scans or return other groups.
    # The combined view contains exactly OTT/streaming platforms + live sports.
    return [ott_collection, _sports_collection()]


#----- Dynamic Nuvio Collection JSON for this token
@router.get("/{token}/nuvio-collection.json")
async def get_nuvio_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "full", token_data)


@router.get("/{token}/nuvio-ott-collection.json")
async def get_nuvio_ott_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "ott", token_data)


@router.get("/{token}/nuvio-explore-collection.json")
async def get_nuvio_explore_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "explore", token_data)


@router.get("/{token}/nuvio-library-collection.json")
@router.get("/{token}/nuvio-library-collections.json")
async def get_nuvio_library_collections(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "library", token_data)


@router.get("/{token}/nuvio-sports-collection.json")
@router.get("/{token}/nuvio-sport-collection.json")
async def get_nuvio_sports_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "sports", token_data)


@router.get("/{token}/nuvio-ott-sports-collection.json")
@router.get("/{token}/nuvio-collections-sports.json")
@router.get("/{token}/nuvio-collection-sports.json")
async def get_nuvio_ott_sports_collection(token: str, token_data: dict = Depends(verify_token)):
    return await build_nuvio_collection_data(token, "full_sports", token_data)


#----- Catalog listing (latest/popular/custom, with genre/search/skip)
@router.get("/{token}/catalog/{media_type}/{id}/{extra:path}.json")
@router.get("/{token}/catalog/{media_type}/{id}.json")
async def get_catalog(token: str, media_type: str, id: str, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    if SettingsManager.current().hide_catalog:
        raise HTTPException(status_code=404, detail="Catalog disabled")

    if media_type not in ["movie", "series"]:
        raise HTTPException(status_code=404, detail="Invalid catalog type")

    genre_filter = None
    search_query = None
    stremio_skip = 0

    if extra:
        params = extra.replace("&", "/").split("/")
        for param in params:
            if param.startswith("genre="):
                genre_filter = unquote(param.removeprefix("genre="))
            elif param.startswith("search="):
                search_query = unquote(param.removeprefix("search="))
            elif param.startswith("skip="):
                try:
                    stremio_skip = int(param.removeprefix("skip="))
                except ValueError:
                    stremio_skip = 0

    page = (stremio_skip // PAGE_SIZE) + 1

    try:
        if id.startswith("library_collection_"):
            if media_type != "movie":
                return {"metas": []}
            try:
                collection_id = int(id.removeprefix("library_collection_"))
            except (TypeError, ValueError):
                return {"metas": []}

            collection_tmdb_ids = await get_tmdb_ids_for_collection(collection_id)
            if not collection_tmdb_ids:
                return {"metas": []}

            collection_query_parts = [
                {"tmdb_id": {"$in": collection_tmdb_ids}},
                _visibility_query(token_data),
                _not_exclusive_clause(),
            ]
            if genre_filter:
                collection_query_parts.append({"genres": {"$in": [genre_filter]}})
            collection_query = _merge_filters(*collection_query_parts)
            by_tmdb_id = {}
            storage_keys = [key for key in db.dbs if str(key).startswith("storage_")]
            storage_keys.sort(
                key=lambda key: int(str(key).split("_", 1)[1]) if str(key).split("_", 1)[1].isdigit() else 0,
                reverse=True,
            )
            for db_key in storage_keys:
                try:
                    cursor = db.dbs[db_key]["movie"].find(collection_query)
                    async for item in cursor:
                        try:
                            tmdb_id = int(item.get("tmdb_id"))
                        except (TypeError, ValueError):
                            continue
                        if tmdb_id in by_tmdb_id:
                            continue
                        if not _token_can_view(
                            item.get("visibility") or "public",
                            item.get("allowed_tokens") or [],
                            token_data,
                        ):
                            continue
                        if search_query:
                            q = search_query.casefold()
                            candidates = (
                                item.get("title") or "",
                                item.get("title_english") or "",
                                item.get("original_title") or "",
                            )
                            if not any(q in str(candidate).casefold() for candidate in candidates):
                                continue
                        item = convert_objectid_to_str(item)
                        by_tmdb_id[tmdb_id] = item
                except Exception as exc:
                    LOGGER.warning(f"[Library Collections] Catalog query failed for {db_key}: {exc}")

            def _collection_sort_key(item):
                try:
                    year = int(item.get("release_year"))
                except (TypeError, ValueError):
                    year = 9999
                return year, (item.get("title") or "").casefold()

            local_items = sorted(by_tmdb_id.values(), key=_collection_sort_key)
            start = max(stremio_skip, 0)
            items = local_items[start:start + PAGE_SIZE]
        elif id.startswith("custom_") or id.startswith("auto_"):
            catalog_id = id.removeprefix("custom_")
            catalog = await db.get_custom_catalog(catalog_id)
            if not catalog:
                return {"metas": []}

            db_media_type = "tv" if media_type == "series" else "movie"
            visible_items = [
                it for it in (catalog.get("items") or [])
                if it.get("media_type") == db_media_type
                and _token_can_view(*_effective_visibility(catalog, it), token_data)
            ]
            visible_items.sort(key=lambda it: it.get("updated_on") or it.get("added_at") or datetime.min, reverse=True)
            if search_query:
                ids = [int(it["tmdb_id"]) for it in visible_items if it.get("tmdb_id") is not None]
                found = await db.search_documents(
                    query=search_query, page=page, page_size=PAGE_SIZE,
                    extra_filter=_merge_filters({"tmdb_id": {"$in": ids}}, _visibility_query(token_data)),
                    media_type=db_media_type,
                )
                items = [it for it in found.get("results", []) if it.get("media_type") == db_media_type]
            else:
                start = (page - 1) * PAGE_SIZE
                items = await db.get_documents(visible_items[start:start + PAGE_SIZE])
                items = [it for it in items if _token_can_view(it.get("visibility") or "public", it.get("allowed_tokens") or [], token_data)]
        elif search_query:
            search_results = await db.search_documents(
                query=search_query, page=page, page_size=PAGE_SIZE,
                extra_filter=_merge_filters(_visibility_query(token_data), _not_exclusive_clause(allow_searchable=True)),
                media_type="tv" if media_type == "series" else "movie",
            )
            all_items = search_results.get("results", [])
            db_media_type = "tv" if media_type == "series" else "movie"
            items = [item for item in all_items if item.get("media_type") == db_media_type]
        elif id in {"trending_movies", "trending_series"}:
            expected_type = "movie" if id == "trending_movies" else "series"
            if media_type != expected_type:
                return {"metas": []}

            items = []
            tmdb_type = "movie" if media_type == "movie" else "tv"
            trending_ids = await _get_tmdb_trending_ids(tmdb_type, "day")
            if trending_ids:
                db_matches = await _get_db_matches_in_tmdb_order(
                    media_type, trending_ids, token_data, genre_filter=genre_filter
                )
                if search_query:
                    q = search_query.lower()
                    db_matches = [
                        it for it in db_matches
                        if q in (it.get("title") or "").lower()
                        or q in (it.get("name") or "").lower()
                        or q in (it.get("original_title") or "").lower()
                    ]
                start = max(stremio_skip, 0)
                items = db_matches[start:start + PAGE_SIZE]

            # Fallback to latest uploaded media if TMDB is unconfigured, empty, or returns no DB matches
            if not items and stremio_skip == 0:
                LOGGER.info(f"[TMDB Trending] Falling back to latest {media_type} for catalog {id}")
                sort_params = [("updated_on", "desc")]
                vis_filter = _merge_filters(_visibility_query(token_data), _not_exclusive_clause())
                if media_type == "movie":
                    data = await db.sort_movies(sort_params, page, PAGE_SIZE, genre_filter=genre_filter, extra_filter=vis_filter, projection=META_PROJECTION)
                    items = data.get("movies", [])
                else:
                    data = await db.sort_tv_shows(sort_params, page, PAGE_SIZE, genre_filter=genre_filter, extra_filter=vis_filter, projection=META_PROJECTION)
                    items = data.get("tv_shows", [])
        else:
            if "latest" in id:
                sort_params = [("updated_on", "desc")]
            elif "top" in id:
                sort_params = [("rating", "desc")]
            else:
                sort_params = [("updated_on", "desc")]

            vis_filter = _merge_filters(_visibility_query(token_data), _not_exclusive_clause())
            if media_type == "movie":
                data = await db.sort_movies(sort_params, page, PAGE_SIZE, genre_filter=genre_filter, extra_filter=vis_filter, projection=META_PROJECTION)
                items = data.get("movies", [])
            else:
                data = await db.sort_tv_shows(sort_params, page, PAGE_SIZE, genre_filter=genre_filter, extra_filter=vis_filter, projection=META_PROJECTION)
                items = data.get("tv_shows", [])
    except Exception:
        return {"metas": []}

    metas = [convert_to_stremio_meta(item) for item in items]
    if SettingsManager.current().fanart_enabled:
        await asyncio.gather(*(_apply_fanart(m, it) for m, it in zip(metas, items)))
    return JSONResponse({"metas": metas, "cacheMaxAge": 0, "staleRevalidate": 0, "staleError": 0},
                        headers={"Cache-Control": "private, no-store"})


@router.get("/{token}/meta/{media_type}/{id}.json")
async def get_meta(token: str, media_type: str, id: str, token_data: dict = Depends(verify_token)):
    if SettingsManager.current().hide_catalog:
        raise HTTPException(status_code=404, detail="Catalog disabled")

    parsed = _parse_stremio_id(id)
    imdb_id = parsed["imdb_id"] if not parsed["is_kitsu"] else None
    kitsu_id = parsed["kitsu_id"]

    media = await db.get_media_details(imdb_id=imdb_id, kitsu_id=kitsu_id, media_type=media_type,
        projection={"telegram": 0, "seasons.episodes.telegram": 0},
        can_view=lambda d: _token_can_view(d.get('visibility') or 'public', d.get('allowed_tokens') or [], token_data))
    if not media:
        return {"meta": {}}

    if not _token_can_view(media.get("visibility") or "public", media.get("allowed_tokens") or [], token_data):
        return {"meta": {}}

    meta_id = id
    if parsed["is_kitsu"] and kitsu_id is not None:
        meta_id = f"kitsu:{kitsu_id}"
    elif id.startswith(SERIES_PREFIX):
        meta_id = series_id(media.get('imdb_id') or imdb_id)
    elif media.get("imdb_id"):
        meta_id = media.get("imdb_id")

    meta_obj = {
        "id": meta_id,
        "type": "series" if media.get("media_type") == "tv" else "movie",
        "name": _display_title(media),
        "description": media.get("description") or "",
        "year": _year_label(media) or (str(media.get("release_year")) if media.get("release_year") else ""),
        "imdbRating": str(media.get("rating") or "") if media.get("rating") not in (None, "") else "",
        "genres": media.get("genres") or [],
        "poster": _poster_url(media.get("imdb_id") or imdb_id, media.get("poster"), media.get('tmdb_id'), media.get('media_type')),
        "logo": media.get("logo") or "",
        "background": _abs_media_url(media.get("backdrop")),
        "imdb_id": media.get("imdb_id") or (id if not parsed["is_kitsu"] else ""),
        "releaseInfo": _year_label(media) or "",
        "moviedb_id": _safe_moviedb_id(media),
        "cast": media.get("cast") or [],
        "runtime": media.get("runtime") or "",
    }

    await _apply_fanart(meta_obj, media)

    if media.get("media_type") == "movie":
        released_date = format_released_date(media)
        if released_date:
            meta_obj["released"] = released_date

    if media_type == "series":
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        videos = []
        seasons = media.get("seasons") or []

        def _snum(s):
            try:
                return int(s.get("season_number"))
            except (TypeError, ValueError):
                return None

        def _enum(e):
            try:
                return int(e.get("episode_number"))
            except (TypeError, ValueError):
                return None

        # A title can exist in more than one storage bucket on older
        # installations.  Database lookup merges those copies, but keep the
        # response defensive as well: normalize legacy string numbers and do
        # not emit duplicate/invalid video IDs that Nuvio will discard.
        seen_video_ids = set()
        valid_seasons = [s for s in seasons if _snum(s) is not None]
        for season in sorted(valid_seasons, key=lambda s: _snum(s)):
            s_num = _snum(season)
            episodes = season.get("episodes") or []
            for episode in sorted(
                episodes,
                key=lambda e: _enum(e) if _enum(e) is not None else 10**9,
            ):
                e_num = _enum(episode)
                abs_ep = episode.get("absolute_episode")
                if e_num is None:
                    # Some pre-normalization anime records only retained the
                    # absolute episode number.  It is still a valid playable
                    # episode, so use it as the relative fallback rather
                    # than returning an E00 entry that clients hide.
                    try:
                        e_num = int(abs_ep) if abs_ep is not None else None
                    except (TypeError, ValueError):
                        e_num = None
                if e_num is None:
                    continue
                if parsed["is_kitsu"] and kitsu_id is not None:
                    # Absolute numbering may restart for a sequel; include
                    # season and episode to keep IDs unique across seasons.
                    episode_id = f"kitsu:{kitsu_id}:{s_num}:{e_num}"
                else:
                    episode_id = f"{meta_id}:{s_num}:{e_num}"
                if episode_id in seen_video_ids:
                    continue
                seen_video_ids.add(episode_id)
                ep_title = (
                    episode.get("title")
                    or episode.get("episode_title")
                    or (f"E{e_num:02d}" if s_num != 0 else f"Combined {e_num}")
                )
                videos.append({
                    "id": episode_id,
                    "title": ep_title,
                    "season": s_num,
                    "episode": e_num,
                    "overview": (
                        episode.get("overview")
                        or episode.get("episode_overview")
                        or "No description available for this episode yet."
                    ),
                    "released": episode.get("released") or episode.get("episode_released") or yesterday,
                    "thumbnail": _abs_media_url(
                        episode.get("episode_backdrop") or episode.get("thumbnail")
                    ) or "https://raw.githubusercontent.com/weebzone/Colab-Tools/refs/heads/main/no_episode_backdrop.png",
                    "imdb_id": episode.get("imdb_id") or media.get("imdb_id") or (id if not parsed["is_kitsu"] else ""),
                })
        meta_obj["videos"] = videos
        if not videos:
            LOGGER.warning(f"[META] series {id} has no episode entries in DB")
    return JSONResponse({"meta": meta_obj, "cacheMaxAge": 0, "staleRevalidate": 0, "staleError": 0},
                        headers={"Cache-Control": "private, no-store"})


#----- Subtitles for a title/episode, sourced from subtitle files in the channels
@router.get("/{token}/subtitles/{media_type}/{id}/{extra:path}.json")
@router.get("/{token}/subtitles/{media_type}/{id}.json")
async def get_subtitles(token: str, media_type: str, id: str, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    try:
        parts = original_id(id).split(":")
        imdb_id = parts[0]
        season = int(parts[1]) if len(parts) > 1 else None
        episode = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        return {"subtitles": []}

    db_media_type = "tv" if media_type == "series" else "movie"
    subs = await get_subtitles_for(imdb_id, db_media_type, season, episode)
    if not subs:
        return {"subtitles": []}
    return {"subtitles": stremio_subtitle_entries(subs, token, SettingsManager.current().base_url)}


async def _kitsu_title_year(kitsu_id: int) -> tuple:
    try:
        from Backend.helper.metadata.providers.kitsu import get_anizip_mappings, _get_client, KITSU_URL
        client = await _get_client()
        resp = await client.get(f"{KITSU_URL}/anime/{int(kitsu_id)}")
        resp.raise_for_status()
        data = (resp.json() or {}).get("data") or {}
        attrs = data.get("attributes") or {}
        titles = attrs.get("titles") or {}
        title = (
            titles.get("en")
            or titles.get("en_jp")
            or attrs.get("canonicalTitle")
            or titles.get("ja_jp")
            or ""
        )
        year = None
        start = attrs.get("startDate") or ""
        if start and len(str(start)) >= 4:
            try:
                year = int(str(start)[:4])
            except (TypeError, ValueError):
                year = None
        if not title:
            doc = await get_anizip_mappings(int(kitsu_id)) or {}
            title = (doc.get("title") or {}).get("en") or (doc.get("title") or {}).get("x-jat") or ""
        return title, year
    except Exception as e:
        LOGGER.warning(f"[KITSU] title lookup failed for {kitsu_id}: {e}")
        return "", None


#----- Autoplay-next keeps the same quality and route (Cloudflare/Proxy/Direct)
def _binge_group(quality: str, suffix: str = "") -> str:
    route = suffix.strip("() ").lower()
    return f"telegram-stremio-{quality}" + (f"-{route}" if route else "")


def _streams_from_global_results(token: str, global_results: list) -> list:
    streams = []
    for r in global_results:
        is_split = bool(r.get("is_split"))
        _, stream_title = format_stream_details(r["title"], r["quality"], r["size"], is_split=is_split)
        stream_name = sort_name = f"🌐 GLOBAL {r['quality']}"
        stream_title = f"{stream_title}\n📡 {r['source_chat']}"
        if is_split:
            kind = "zip parts" if r.get("is_zip") else "parts"
            stream_title += f" · 📦 {r.get('part_count', 0)} {kind}"
        size_bytes = parse_size_to_bytes(r.get("size", ""))
        custom = format_stream(SettingsManager.current(), r["title"], size_bytes, addon_name="🌐 Global",
                               is_split=is_split, parts=r.get("part_count", 0), channel=r.get("source_chat"))
        if custom:
            stream_name, stream_title = custom
        url = f"{SettingsManager.current().base_url}/dl/{token}/{r['token']}/{quote(r['title'])}"
        if cf_enabled():
            cf_url = cf_stream_url(token, r['token'], quote(r['title']))
            if SettingsManager.current().cf_stream_mode == "cloudflare":
                url = cf_url
            else:
                streams.append({"name": f"{stream_name} (Cloudflare)", "title": stream_title, "url": cf_url, "size_bytes": size_bytes, "sort_name": sort_name,
                                "behaviorHints": {"bingeGroup": _binge_group(f"global-{r['quality']}", "(Cloudflare)")}})
        streams.append({"name": stream_name, "title": stream_title, "url": url, "size_bytes": size_bytes, "sort_name": sort_name,
                        "behaviorHints": {"bingeGroup": _binge_group(f"global-{r['quality']}")}})
    return streams


async def _global_streams_for(
    token: str,
    imdb_id: str = None,
    media_type: str = "series",
    season_num: Optional[int] = None,
    episode_num: Optional[int] = None,
    kitsu_id: Optional[int] = None,
    absolute_episode: Optional[int] = None,
    is_anime: bool = False,
) -> list:
    expected_title = ""
    year = None
    cinemeta_videos = []

    if kitsu_id is not None:
        expected_title, year = await _kitsu_title_year(int(kitsu_id))
        is_anime = True
    elif imdb_id:
        imdb_media_type = "tvSeries" if media_type == "series" else "movie"
        detail = await get_detail(imdb_id=imdb_id, media_type=imdb_media_type)
        if not detail or not detail.get("title"):
            return []
        expected_title = detail["title"]
        year = (detail.get("releaseDetailed") or {}).get("year") or None
        cinemeta_videos = detail.get("videos") or []
        if season_num is not None and episode_num is not None and absolute_episode is None:
            try:
                await get_season(imdb_id=imdb_id, season_id=season_num, episode_id=episode_num)
            except Exception:
                pass

    if not expected_title:
        return []

    search_season = season_num
    search_episode = episode_num
    if is_anime and absolute_episode is not None:
        search_season = None
        search_episode = absolute_episode
    elif is_anime and season_num is None and episode_num is not None:
        search_season = None
        search_episode = episode_num

    abs_ep = absolute_episode
    map_source = None
    if (
        abs_ep is None
        and media_type == "series"
        and imdb_id
        and season_num is not None
        and episode_num is not None
    ):
        try:
            from Backend.helper.metadata.episode_maps import absolute_from_imdb_episode

            mapped = await absolute_from_imdb_episode(
                imdb_id,
                int(season_num),
                int(episode_num),
                title=expected_title,
                videos=cinemeta_videos,
            )
            if mapped and mapped.get("is_anime"):
                is_anime = True
                map_source = mapped.get("source")
                abs_ep = mapped.get("absolute_episode")
                if abs_ep is None and int(season_num) == 1:
                    abs_ep = int(episode_num)
        except Exception as e:
            LOGGER.warning(f"[GLOBAL SEARCH] anime map lookup failed for {imdb_id}: {e}")

    mapped_from_sxx = (
        abs_ep is not None
        and absolute_episode is None
        and season_num is not None
        and episode_num is not None
    )
    if mapped_from_sxx:
        search_season = None
        search_episode = int(abs_ep)
        LOGGER.info(
            f"[GLOBAL SEARCH] Anime mapped S{int(season_num):02d}E{int(episode_num):02d} "
            f"→ absolute {int(abs_ep)} for '{expected_title}'"
            + (f" (via {map_source})" if map_source else "")
            + "; trying absolute first"
        )

    auth_channels = SettingsManager.current().auth_channels
    try:
        global_results = await global_search(
            expected_title,
            auth_channels,
            year=year,
            season=search_season,
            episode=search_episode,
        )
    except Exception as e:
        LOGGER.error(f"[GLOBAL SEARCH] search failed for '{expected_title}': {e}")
        global_results = []

    if not global_results and mapped_from_sxx:
        LOGGER.info(
            f"[GLOBAL SEARCH] Absolute {int(abs_ep)} empty for '{expected_title}'; "
            f"falling back to S{int(season_num):02d}E{int(episode_num):02d}"
        )
        try:
            global_results = await global_search(
                expected_title,
                auth_channels,
                year=year,
                season=season_num,
                episode=episode_num,
            )
        except Exception as e:
            LOGGER.error(f"[GLOBAL SEARCH] SxxExx fallback failed for '{expected_title}': {e}")
            global_results = []

    return _streams_from_global_results(token, global_results)


#----- Cached check that a user is still in the subscription group (fail-open)
async def _is_subscription_member(user_id: int) -> bool:
    group_id = SettingsManager.current().subscription_group_id
    if not group_id:
        return True

    cache_key = (group_id, user_id)
    cached = _membership_cache.get(cache_key)
    now_ts = time.monotonic()
    if cached and (now_ts - cached[0]) < _MEMBERSHIP_TTL:
        return cached[1]

    try:
        member = await StreamBot.get_chat_member(group_id, user_id)
        result = member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    except UserNotParticipant:
        result = False
    except Exception as e:
        LOGGER.warning(f"[SUBSCRIPTION] Membership check failed for user {user_id}: {e}")
        return True

    if len(_membership_cache) >= _MEMBERSHIP_CACHE_MAX:
        for k in [k for k, v in _membership_cache.items() if (now_ts - v[0]) >= _MEMBERSHIP_TTL]:
            _membership_cache.pop(k, None)
        if len(_membership_cache) >= _MEMBERSHIP_CACHE_MAX:
            _membership_cache.clear()

    _membership_cache[cache_key] = (now_ts, result)
    return result


#----- Resolve playable streams for a title (Telegram library or Global Search)
@router.get("/{token}/stream/{media_type}/{id}.json")
async def get_streams(
    token: str,
    media_type: str,
    id: str,
    request: Request,
    token_data: dict = Depends(verify_token)
):
    #----- Capture the real app/device from the addon-protocol UA (not the spoofed video UA)
    asyncio.create_task(record_client(
        token,
        token_data.get("name") if token_data else None,
        client_ip_from(request),
        request.headers.get("user-agent", ""),
    ))

    if token_data.get("subscription_expired"):
        return {
            "streams": [
                {
                    "name": "🚫 Plan Expired",
                    "title": "Your plan is expired.\nRenew it from the bot to continue watching.",
                    "url": get_streambot_url()
                }
            ]
        }

    #----- Subscription users must currently be members of the configured group.
    #----- Admin, lifetime and admin-set token-expiry grants skip this check.
    if (SettingsManager.current().subscription
            and not token_data.get("is_admin")
            and not token_data.get("subscription_exempt")
            and not token_data.get("expires_at")):
        user_id = token_data.get("user_id")
        if user_id and not await _is_subscription_member(int(user_id)):
            return {
                "streams": [
                    {
                        "name": "📢 Join Required",
                        "title": "First join the channel to stream it.\nThen wait for 2 min for verification",
                        "url": get_streambot_url()
                    }
                ]
            }

    if token_data.get("limit_exceeded"):
        limit_type = token_data["limit_exceeded"]

        title = (
            "🚫 Daily Limit Reached – Upgrade Required"
            if limit_type == "daily"
            else "🚫 Monthly Limit Reached – Upgrade Required"
        )

        return {
            "streams": [
                {
                    "name": "Limit Reached",
                    "title": title,
                    "url": f"tg://user?id={Telegram.OWNER_ID}"
                }
            ]
        }

    try:
        parsed = _parse_stremio_id(id)
        imdb_id = parsed["imdb_id"]
        kitsu_id = parsed["kitsu_id"]
        season_num = parsed["season_num"]
        episode_num = parsed["episode_num"]
        absolute_episode = parsed["absolute_episode"]
        is_kitsu = parsed["is_kitsu"]
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid Stremio ID format")

    if is_kitsu and kitsu_id is None:
        raise HTTPException(status_code=400, detail="Invalid Kitsu ID format")

    if not await _title_allowed(imdb_id=imdb_id, token_data=token_data, kitsu_id=kitsu_id):
        return {"streams": []}

    media_details = await db.get_media_details(
        imdb_id=imdb_id,
        season_number=season_num,
        episode_number=episode_num,
        kitsu_id=kitsu_id,
        absolute_episode=absolute_episode,
        media_type=media_type,
        can_view=lambda d: _token_can_view(d.get('visibility') or 'public', d.get('allowed_tokens') or [], token_data),
    )

    streams = []

    is_combined = season_num == COMBINED_SEASON and episode_num is not None and episode_num >= COMBINED_EPISODE_BASE

    if media_details and "telegram" in media_details:
        fmt_meta = {
            "queryType": media_type, "type": media_type, "isAnime": bool(media_details.get("is_anime")),
            "title": media_details.get("title"), "year": media_details.get("release_year"),
            "season": season_num, "episode": episode_num,
        }
        from Backend.helper.multipart_video import with_video_part
        for quality in media_details.get("telegram", []):
            quality = with_video_part(quality)
            if quality.get('video_part') and not SettingsManager.current().allow_multipart_video:
                continue
            if quality.get("id"):
                stream_offset = len(streams)
                filename = quality.get("name", "")
                quality_str = quality.get("quality", "HD")
                size = quality.get("size", "")
                size_bytes = parse_size_to_bytes(size)

                combined = parse_combined_episodes(filename) if is_combined else None
                episode_start = combined.get("start") or 0 if combined else 0
                name_key = combined_name_key(filename) if combined else ""

                stream_name, stream_title = format_stream_details(
                    filename, quality_str, size, is_split=bool(quality.get("group_key"))
                )

                sort_name = stream_name
                original_url = f"{SettingsManager.current().base_url}/dl/{token}/{quality.get('id')}/video.mkv"
                proxy_url = build_proxy_url(original_url)
                cf_url = cf_stream_url(token, quality.get('id'), "video.mkv") if cf_enabled() else None
                cf_only = cf_url and SettingsManager.current().cf_stream_mode == "cloudflare"

                def add(suffix, url, proxied=False):
                    name, title = stream_name, stream_title
                    custom = format_stream(
                        SettingsManager.current(), filename, size_bytes, addon_name=ADDON_NAME, proxied=proxied,
                        is_split=bool(quality.get("group_key")), meta=fmt_meta,
                    )
                    if custom:
                        name, title = custom
                    if combined:
                        label = "Full" if combined.get("start") is None else f"E{combined['start']:02d}-E{combined['end']:02d}"
                        if label.lower() not in name.lower():
                            name = f"{name} {label}"
                    if quality.get('video_part'):
                        label = f"[Multi-Part · Part {quality['video_part']}]"
                        name += f" {label}"
                        title = f"{label}\n{title}"
                    streams.append({"name": f"{name} {suffix}".strip(), "title": title, "url": url, "size_bytes": size_bytes,
                                    "episode_start": episode_start, "name_key": name_key, "sort_name": sort_name,
                                    "behaviorHints": {"bingeGroup": _binge_group(quality_str, suffix)}})

                if cf_url:
                    add("" if cf_only else "(Cloudflare)", cf_url)
                if not cf_only:
                    if SettingsManager.current().show_proxy_and_non_proxy_both and proxy_url:
                        add("(Proxy)", proxy_url, True)
                        add("(Direct)", original_url)
                    else:
                        add("", proxy_url or original_url, bool(proxy_url))
                for stream in streams[stream_offset:]:
                    stream['video_part'] = quality.get('video_part')
                    stream['video_group'] = quality.get('video_group')
    elif is_global_search_enabled() and can_stream_session(token_data):
        try:
            is_anime = bool(is_kitsu or (media_details and media_details.get("is_anime")))
            log_id = f"kitsu:{kitsu_id}" if is_kitsu else imdb_id
            LOGGER.info(f"{log_id}:{season_num}:{episode_num}:abs={absolute_episode}|{media_type}")
            streams.extend(
                await _global_streams_for(
                    token,
                    imdb_id=imdb_id,
                    media_type=media_type,
                    season_num=season_num,
                    episode_num=episode_num,
                    kitsu_id=kitsu_id,
                    absolute_episode=absolute_episode,
                    is_anime=is_anime,
                )
            )
        except Exception as e:
            LOGGER.error(f"[GLOBAL SEARCH] stream search failed for {id}: {e}")

    #----- Per-token quality filter (fall back to all if it would hide everything)
    config = token_data.get("config") or {}
    quality_filter = set(config.get("quality_filter") or [])
    if quality_filter and streams:
        filtered = [s for s in streams if stream_res_label(s.get("sort_name", s.get("name", ""))) in quality_filter]
        if filtered:
            streams = filtered

    if not streams:
        return {"streams": []}

    ascending = config.get("quality_sort") == "asc"
    if is_combined:
        streams.sort(key=lambda s: s.get("episode_start", 0))
        streams.sort(key=lambda s: s.get("name_key", ""))
        streams.sort(key=lambda s: get_resolution_priority(s.get("sort_name", s.get("name", ""))), reverse=not ascending)
    else:
        streams.sort(
            key=lambda s: (get_resolution_priority(s.get("sort_name", s.get("name", ""))), s.get("size_bytes", 0)),
            reverse=not ascending
        )
    from Backend.helper.multipart_video import part_sort_key
    streams.sort(key=part_sort_key)
    for stream in streams:
        stream.pop('video_part', None)
        stream.pop('video_group', None)
    # User override wins; an explicit disabled override hides the global notice.
    notice = token_data.get('stream_notice', SettingsManager.current().stream_notice)
    if isinstance(notice, dict) and notice.get('enabled') and notice.get('url'):
        streams.insert(0, {'name': notice.get('name') or '📢 Message',
                           'title': notice.get('message') or '',
                           'description': notice.get('message') or '',
                           'externalUrl': notice['url']})
    return {"streams": streams}

#----- Configure/install landing page rendered as HTML for a token
@router.get("/{token}/configure")
async def configure_addon(token: str, request: Request):
    manifest_url = f"{SettingsManager.current().base_url}/stremio/{token}/manifest.json"
    web_install_url = f"https://web.stremio.com/#/?addon_manifest={quote(manifest_url, safe='')}"

    token_doc = await db.get_api_token(token)
    user_name = "Unknown"
    expiry_str = "Never"
    status_color = "#ef4444"
    status_text = "Unknown"

    def _expired(when):
        ref = datetime.utcnow()
        try:
            if when.tzinfo is not None:
                ref = datetime.now(timezone.utc)
        except AttributeError:
            pass
        return when < ref

    def _fmt(when):
        try:
            return when.strftime("%d %b %Y").lstrip("0")
        except Exception:
            return "N/A"

    if token_doc:
        uid = token_doc.get("user_id")
        is_admin = bool(token_doc.get("is_admin"))
        try:
            is_admin = is_admin or (uid is not None and int(uid) == int(Telegram.OWNER_ID))
        except (TypeError, ValueError):
            pass

        user = None
        if uid:
            try:
                user = await db.get_user(int(uid))
            except Exception:
                user = None
        if user:
            user_name = user.get("first_name") or user.get("username") or f"User {uid}"
        elif uid:
            user_name = f"User {uid}"

        token_expiry = token_doc.get("expires_at")
        if is_admin:
            status_color, status_text, expiry_str = "#22c55e", "Admin", "Never"
        elif token_doc.get("subscription_exempt"):
            status_color, status_text, expiry_str = "#22c55e", "Active", "Never"
        elif token_expiry is not None:
            expiry_str = _fmt(token_expiry)
            if _expired(token_expiry):
                status_color, status_text = "#ef4444", "Expired"
            else:
                status_color, status_text = "#22c55e", "Active"
        elif SettingsManager.current().subscription:
            expiry = user.get("subscription_expiry") if user else None
            if user and user.get("subscription_status") == "active" and expiry and not _expired(expiry):
                status_color, status_text, expiry_str = "#22c55e", "Active", _fmt(expiry)
            else:
                status_color, status_text = "#ef4444", "Expired"
                expiry_str = _fmt(expiry) if expiry else "N/A"
        else:
            status_color, status_text, expiry_str = "#22c55e", "Active", "Never"

    return templates.TemplateResponse("stremio_configure.html", {
        "request": request,
        "theme": get_theme(request.session.get("theme", DEFAULT_THEME), request.session.get("style", DEFAULT_STYLE)),
        "manifest_url": manifest_url,
        "web_install_url": web_install_url,
        "user_name": user_name,
        "expiry_str": expiry_str,
        "status_text": status_text,
        "status_color": status_color,
    })


#----- Catalogs this token can see, in effective (token or global) order
async def _addon_catalogs_for_token(token_data: dict) -> list:
    entries = [
        {"id": "latest_movies", "name": "Latest Movies", "type": "movie"},
        {"id": "top_movies", "name": "Popular Movies", "type": "movie"},
        {"id": "latest_series", "name": "Latest Series", "type": "series"},
        {"id": "top_series", "name": "Popular Series", "type": "series"},
        {"id": "trending_movies", "name": "Trending Movies", "type": "movie"},
        {"id": "trending_series", "name": "Trending Series", "type": "series"},
    ]
    try:
        for c in await db.get_custom_catalogs(slim=True):
            items = [i for i in (c.get("items") or []) if _token_can_view(*_effective_visibility(c, i), token_data)]
            if not items:
                continue
            cid, name = f"custom_{c['_id']}", (c.get("name") or "Catalog")
            if any(i.get("media_type") == "movie" for i in items):
                entries.append({"id": cid, "name": name, "type": "movie"})
            if any(i.get("media_type") == "tv" for i in items):
                entries.append({"id": cid, "name": name, "type": "series"})
    except Exception:
        pass
    for e in entries:
        e["key"] = f"{e['id']}::{e['type']}"
    order = await db.get_catalog_order()
    tconf = token_data.get("config") or {}
    effective = tconf.get("catalog_order") or order
    if effective:
        rank = {k: i for i, k in enumerate(effective)}
        entries.sort(key=lambda e: rank.get(e["key"], rank.get(e["id"], len(effective) + 1)))
    return entries


#----- Read this token's addon config + catalog list (public, used by configure page)
@router.get("/{token}/addon-config")
async def get_addon_config(token: str):
    doc = await db.get_api_token(token)
    if not doc:
        raise HTTPException(status_code=404, detail="Invalid token")
    return {"config": doc.get("config") or {}, "catalogs": await _addon_catalogs_for_token(doc)}


#----- Persist this token's addon config (public, used by configure page)
@router.post("/{token}/addon-config")
async def save_addon_config(token: str, payload: dict):
    doc = await db.get_api_token(token)
    if not doc:
        raise HTTPException(status_code=404, detail="Invalid token")
    valid_q = {"360p", "480p", "540p", "720p", "1080p", "4K"}
    config = {
        "quality_sort": "asc" if payload.get("quality_sort") == "asc" else "desc",
        "quality_filter": [q for q in (payload.get("quality_filter") or []) if q in valid_q],
        "hidden_catalogs": [str(x) for x in (payload.get("hidden_catalogs") or [])],
        "catalog_order": [str(x) for x in (payload.get("catalog_order") or [])],
    }
    await db.set_token_config(token, config)
    return {"ok": True, "config": config}
