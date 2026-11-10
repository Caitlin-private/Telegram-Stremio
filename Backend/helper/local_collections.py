"""Dynamic franchise index for Nuvio's database-backed Library Collections.

The tracking database stores TMDB collection metadata independently of media rows.
This avoids schema migrations and prevents every Nuvio profile request from hitting TMDB.
Metadata backfill runs asynchronously and persists progress across restarts.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from Backend import db
from Backend.helper.metadata.providers.tmdb import get_tmdb_client, tmdb_api_key
from Backend.logger import LOGGER

_CACHE_COLLECTION = "library_collection_cache"
_CACHE_TTL_DAYS = 90
_SCAN_RETRY_SECONDS = 300
_MAX_CONCURRENT_REQUESTS = 4
_INITIAL_BATCH_SIZE = 16

_refresh_task: Optional[asyncio.Task] = None
_refresh_started_at = 0.0
_first_batch_ready: Optional[asyncio.Event] = None


def _value(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _image_url(path: Optional[str], size: str = "w500") -> Optional[str]:
    if not path:
        return None
    path = str(path).strip()
    if path.startswith(("https://", "http://")):
        return path
    return f"https://image.tmdb.org/t/p/{size}/{path.lstrip('/')}"


def _parse_collection_details(details: Any, tmdb_id: int) -> dict:
    """Normalize TMDB's movie details response into a cache document."""
    collection = _value(details, "belongs_to_collection")
    collection_id = _value(collection, "id") if collection else None
    try:
        collection_id = int(collection_id) if collection_id is not None else None
    except (TypeError, ValueError):
        collection_id = None

    collection_name = _value(collection, "name") if collection_id else None
    return {
        "tmdb_id": int(tmdb_id),
        "collection_id": collection_id,
        "collection_name": str(collection_name).strip() if collection_name else None,
        "poster_path": _value(collection, "poster_path") if collection_id else None,
        "backdrop_path": _value(collection, "backdrop_path") if collection_id else None,
        "checked_at": datetime.utcnow(),
    }


def _storage_keys() -> list[str]:
    keys = [key for key in db.dbs if str(key).startswith("storage_")]
    return sorted(
        keys,
        key=lambda key: int(str(key).split("_", 1)[1]) if str(key).split("_", 1)[1].isdigit() else 0,
        reverse=True,
    )


async def _get_movie_ids_from_storage() -> dict[int, dict]:
    """Get one lightweight record per positive TMDB movie ID; newer DBs win."""
    movies: dict[int, dict] = {}
    projection = {
        "tmdb_id": 1,
        "title": 1,
        "release_year": 1,
        "poster": 1,
        "backdrop": 1,
    }
    for db_key in _storage_keys():
        try:
            cursor = db.dbs[db_key]["movie"].find(
                {"tmdb_id": {"$gt": 0}}, projection
            )
            async for item in cursor:
                try:
                    tmdb_id = int(item.get("tmdb_id"))
                except (TypeError, ValueError):
                    continue
                if tmdb_id > 0 and tmdb_id not in movies:
                    movies[tmdb_id] = {
                        "tmdb_id": tmdb_id,
                        "title": item.get("title") or item.get("title_english") or str(tmdb_id),
                        "release_year": item.get("release_year"),
                        "poster": item.get("poster"),
                        "backdrop": item.get("backdrop"),
                    }
        except Exception as exc:
            LOGGER.warning(f"[Library Collections] Could not read movies from {db_key}: {exc}")
    return movies


async def _fetch_and_cache_one(cache, tmdb_id: int, semaphore: asyncio.Semaphore) -> bool:
    """Fetch one title's belongs_to_collection value; cache positive and negative results."""
    async with semaphore:
        try:
            details = await asyncio.wait_for(
                get_tmdb_client().movie(tmdb_id).details(), timeout=12.0
            )
            normalized = _parse_collection_details(details, tmdb_id)
            normalized["refresh_after"] = datetime.utcnow() + timedelta(days=_CACHE_TTL_DAYS)
            await cache.update_one(
                {"_id": str(tmdb_id)},
                {"$set": normalized},
                upsert=True,
            )
            return True
        except Exception as exc:
            # Do not persist transient errors as negative results; they can retry on a later scan.
            LOGGER.debug(f"[Library Collections] TMDB details lookup failed for {tmdb_id}: {exc}")
            return False


async def _run_workers(cache, items: list[tuple[int, dict]]) -> int:
    if not items:
        return 0
    queue: asyncio.Queue = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)
    completed = 0

    async def worker():
        nonlocal completed
        while True:
            try:
                tmdb_id, _movie = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if await _fetch_and_cache_one(cache, tmdb_id, semaphore):
                    completed += 1
            finally:
                queue.task_done()

    await asyncio.gather(*(worker() for _ in range(_MAX_CONCURRENT_REQUESTS)))
    return completed


async def _refresh_collection_cache() -> None:
    global _first_batch_ready, _refresh_started_at
    tracking = db.dbs.get("tracking")
    if tracking is None or not tmdb_api_key():
        if _first_batch_ready:
            _first_batch_ready.set()
        _refresh_started_at = time.monotonic()
        return

    cache = tracking[_CACHE_COLLECTION]
    try:
        await cache.create_index("tmdb_id")
        await cache.create_index("collection_id")
    except Exception as exc:
        LOGGER.debug(f"[Library Collections] Cache index setup skipped: {exc}")

    try:
        movies = await _get_movie_ids_from_storage()
        if not movies:
            if _first_batch_ready:
                _first_batch_ready.set()
            _refresh_started_at = time.monotonic()
            return

        cached_docs = await cache.find({}, {"tmdb_id": 1, "checked_at": 1}).to_list(length=None)
        cached: dict[int, dict] = {}
        for item in cached_docs:
            try:
                cached[int(item.get("tmdb_id"))] = item
            except (TypeError, ValueError):
                continue

        now = datetime.utcnow()
        cutoff = now - timedelta(days=_CACHE_TTL_DAYS)
        missing = []
        for tmdb_id, movie in movies.items():
            cached_item = cached.get(tmdb_id)
            checked_at = cached_item.get("checked_at") if cached_item else None
            if not cached_item or not isinstance(checked_at, datetime) or checked_at < cutoff:
                missing.append((tmdb_id, movie))

        # Stable ordering keeps the process deterministic across restarts.
        missing.sort(key=lambda pair: (str(pair[1].get("title") or "").casefold(), pair[0]))
        first_batch = missing[:_INITIAL_BATCH_SIZE]
        remaining = missing[_INITIAL_BATCH_SIZE:]

        # Publish the first few results early so the initial collection response can
        # expose any franchises already found without waiting for the entire library.
        if first_batch:
            await _run_workers(cache, first_batch[:4])
        if _first_batch_ready:
            _first_batch_ready.set()
        if len(first_batch) > 4:
            await _run_workers(cache, first_batch[4:])

        # Keep the first page/profile load responsive while gradually filling the persisted index.
        batch_size = 48
        for offset in range(0, len(remaining), batch_size):
            await _run_workers(cache, remaining[offset:offset + batch_size])

        LOGGER.info(
            f"[Library Collections] Metadata index refresh finished; "
            f"checked {len(missing)} TMDB movie records, cached {len(movies)} unique movies"
        )
    except Exception as exc:
        LOGGER.warning(f"[Library Collections] Metadata index refresh failed: {exc}")
        if _first_batch_ready:
            _first_batch_ready.set()
    finally:
        _refresh_started_at = time.monotonic()


async def ensure_library_collection_index(wait_seconds: float = 0.0) -> None:
    """Start a single asynchronous cache refresh; optionally wait briefly for first results."""
    global _refresh_task, _refresh_started_at, _first_batch_ready
    if not tmdb_api_key() or "tracking" not in db.dbs:
        return

    now = time.monotonic()
    if _refresh_task is None or _refresh_task.done():
        if now - _refresh_started_at >= _SCAN_RETRY_SECONDS:
            _refresh_started_at = now
            _first_batch_ready = asyncio.Event()
            _refresh_task = asyncio.create_task(_refresh_collection_cache())

    if wait_seconds > 0 and _first_batch_ready is not None and not _first_batch_ready.is_set():
        try:
            await asyncio.wait_for(_first_batch_ready.wait(), timeout=wait_seconds)
        except asyncio.TimeoutError:
            pass


async def get_cached_movie_collection_index() -> dict[int, dict]:
    """Return TMDB movie ID -> normalized collection metadata for known franchises."""
    await ensure_library_collection_index()
    tracking = db.dbs.get("tracking")
    if tracking is None:
        return {}
    try:
        cursor = tracking[_CACHE_COLLECTION].find({"collection_id": {"$exists": True, "$ne": None}})
        results = await cursor.to_list(length=None)
    except Exception as exc:
        LOGGER.debug(f"[Library Collections] Could not read cache: {exc}")
        return {}

    index: dict[int, dict] = {}
    for item in results:
        try:
            tmdb_id = int(item.get("tmdb_id"))
            collection_id = int(item.get("collection_id"))
        except (TypeError, ValueError):
            continue
        if tmdb_id > 0 and collection_id > 0 and item.get("collection_name"):
            index[tmdb_id] = item
    return index


async def get_tmdb_ids_for_collection(collection_id: int) -> list[int]:
    """Return cached local movie TMDB IDs for a franchise collection."""
    tracking = db.dbs.get("tracking")
    if tracking is None:
        return []
    try:
        cursor = tracking[_CACHE_COLLECTION].find(
            {"collection_id": int(collection_id)}, {"tmdb_id": 1}
        )
        results = await cursor.to_list(length=None)
    except Exception as exc:
        LOGGER.debug(f"[Library Collections] Could not resolve collection {collection_id}: {exc}")
        return []

    ids = set()
    for item in results:
        try:
            ids.add(int(item.get("tmdb_id")))
        except (TypeError, ValueError):
            continue
    return sorted(ids)


def tmdb_image_url(path: Optional[str], size: str = "w500") -> Optional[str]:
    """Public helper used to build collection tile artwork URLs."""
    return _image_url(path, size)
