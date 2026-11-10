"""Quick-scan database operations. Writers must hold the shared receiver lock."""
from collections import defaultdict
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime

from Backend.helper.encrypt import encode_source_ids


DEFERRED_DELETIONS = ContextVar("scan_deferred_deletions", default=None)


def qualities(document, collection):
    if collection == "movie":
        yield from document.get("telegram") or []
    else:
        for season in document.get("seasons") or []:
            for episode in season.get("episodes") or []:
                yield from episode.get("telegram") or []


async def existing_sources(db, channel, message_ids, catalog_pending=None):
    """Two indexed queries per storage DB, including multipart source matches."""
    ids = set(message_ids)
    if not ids:
        return set()
    hashes = await encode_source_ids(channel, ids)
    found = set()
    for index in range(1, db.current_db_index + 1):
        storage = db.dbs.get(f"storage_{index}")
        if storage is None:
            continue
        for collection, prefix in (("movie", "telegram"), ("tv", "seasons.episodes.telegram")):
            query = {"$or": [
                {f"{prefix}.id": {"$in": list(hashes)}},
                {f"{prefix}.parts": {"$elemMatch": {"chat_id": channel, "msg_id": {"$in": list(ids)}}}},
            ]}
            projection = {prefix: 1, 'tmdb_id': 1, 'auto_catalog': 1, 'updated_on': 1}
            async for document in storage[collection].find(query, projection):
                matched = False
                for quality in qualities(document, collection):
                    mid = hashes.get(quality.get("id"))
                    if mid is not None:
                        found.add(mid)
                        matched = True
                    for part in quality.get("parts") or []:
                        if part.get("chat_id") == channel and part.get("msg_id") in ids:
                            found.add(part["msg_id"])
                            matched = True
                catalog = document.get('auto_catalog') or {}
                if (matched and catalog_pending is not None and document.get('tmdb_id')
                        and (not catalog.get('synced') or catalog.get('source_updated_on') != document.get('updated_on'))):
                    # Recover the write-to-checkpoint crash window: a replayed
                    # source can be indexed already but still need its catalog.
                    item = {'tmdb_id': document['tmdb_id'], 'media_type': collection}
                    catalog_pending.setdefault(f'{collection}:{document["tmdb_id"]}', item)
        if found == ids:
            break
    return found


def title_key(document):
    # Do not group an unverified title alone: year and media type matter.
    identity = next(((field, document[field]) for field in ("imdb_id", "tmdb_id", "kitsu_id")
                     if document.get(field)), ("title", document.get("title"), document.get("release_year")))
    return document["media_type"], identity


async def merge_document(db, target, incoming, status):
    for field in ("imdb_id", "tmdb_id", "kitsu_id"):
        if incoming.get(field) and not target.get(field):
            target[field] = incoming[field]
    if incoming.get("is_anime"):
        target["is_anime"] = True
    personal = db._is_personal_tmdb(incoming.get("tmdb_id"))
    if incoming["media_type"] == "movie":
        for quality in incoming["telegram"]:
            target["telegram"] = await db._apply_quality_update(target.get("telegram") or [], quality, personal, status)
    else:
        for season in incoming["seasons"]:
            existing_season = next((s for s in target["seasons"] if s["season_number"] == season["season_number"]), None)
            if existing_season is None:
                existing_season = {**season, "episodes": []}
                target["seasons"].append(existing_season)
            for episode in season["episodes"]:
                existing_episode = next((e for e in existing_season["episodes"]
                    if e["episode_number"] == episode["episode_number"]), None)
                if existing_episode is None and episode.get("absolute_episode") is not None:
                    existing_episode = next((e for e in existing_season["episodes"]
                        if e.get("absolute_episode") == episode["absolute_episode"]), None)
                if existing_episode is None:
                    existing_episode = {**episode, "telegram": []}
                    existing_season["episodes"].append(existing_episode)
                if episode.get("absolute_episode") is not None and not existing_episode.get("absolute_episode"):
                    existing_episode["absolute_episode"] = episode["absolute_episode"]
                for quality in episode["telegram"]:
                    existing_episode["telegram"] = await db._apply_quality_update(
                        existing_episode.get("telegram") or [], quality, personal, status)
    target["updated_on"] = datetime.utcnow()


async def write_batch(db, rows, committed):
    """Read/merge/write once per title; acknowledge only durable group writes.

    Rows are already source-deduplicated under the same lock. Keeping existing
    titles in their current storage avoids moving a whole series during import.
    """
    groups = defaultdict(list)
    for row in rows:
        model = await db.insert_media(**row, prepare_only=True)
        document = model.dict()
        groups[title_key(document)].append((row, document))
    for items in groups.values():
        first = items[0][1]
        collection = "movie" if first["media_type"] == "movie" else "tv"
        existing, storage_key, _ = await db._find_existing_media(
            collection, first.get("imdb_id"), first.get("tmdb_id"), first.get("title"),
            first.get("release_year"), len(db.dbs) - 1, kitsu_id=first.get("kitsu_id"))
        target = deepcopy(existing or first)
        if existing is None:
            target["telegram" if collection == "movie" else "seasons"] = []
        deletions = []
        token = DEFERRED_DELETIONS.set(deletions)
        try:
            for row, document in items:
                await merge_document(db, target, document, row["status"])
            if existing is not None:
                result = await db.dbs[storage_key][collection].replace_one({"_id": existing["_id"]}, target)
                if not result.matched_count:
                    raise RuntimeError("Media changed during quick-scan write; retry the batch")
            else:
                # Retain quota failover without replaying metadata or merges.
                while True:
                    storage_key = f"storage_{db.current_db_index}"
                    target["db_index"] = db.current_db_index
                    try:
                        await db.dbs[storage_key][collection].insert_one(target)
                        break
                    except Exception as exc:
                        if not any(word in str(exc).lower() for word in ("storage", "quota")):
                            raise
                        if db.current_db_index >= len(db.dbs) - 1:
                            raise
                        db.current_db_index += 1
                        await db.update_current_db_index()
        finally:
            DEFERRED_DELETIONS.reset(token)
        # Never delete replaced Telegram files before their replacements commit.
        for kind, value in deletions:
            if kind == "part":
                await db._delete_split_part(value)
            else:
                await db._queue_quality_deletion(value)
        for row, _ in items:
            committed(row)
