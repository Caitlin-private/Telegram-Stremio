"""Combined library pagination and bounded bulk operations for the dashboard."""

import asyncio
import re
from datetime import datetime, timezone

MAX_BATCH = 100


def media_ref(item):
    if not isinstance(item, dict) or item.get("media_type") not in ("movie", "tv"):
        raise ValueError("Each item must specify movie or tv.")
    values = {}
    for field in ("tmdb_id", "db_index"):
        value = item.get(field)
        if isinstance(value, bool) or not re.fullmatch(r"-?\d+", str(value)):
            raise ValueError(f"{field} must be an integer.")
        values[field] = int(value)
    if values["tmdb_id"] == 0 or values["db_index"] < 1:
        raise ValueError("A nonzero title ID and positive database index are required.")
    return {"media_type": item["media_type"], **values}


def ref_key(item):
    return item["media_type"], item["db_index"], item["tmdb_id"]


def normalize_batch(payload):
    action = payload.get("action")
    if action not in ("delete", "add_to_catalogs"):
        raise ValueError("Choose delete or add_to_catalogs.")
    items = payload.get("items")
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_BATCH:
        raise ValueError(f"Select between 1 and {MAX_BATCH} titles.")
    refs = {}
    for item in items:
        ref = media_ref(item)
        refs[ref_key(ref)] = ref
    if action == "delete" and payload.get("confirm_delete") is not True:
        raise ValueError("Confirm deletion of the titles and their backing Telegram messages.")
    catalogs = payload.get("catalog_ids") or []
    if action == "add_to_catalogs":
        if not isinstance(catalogs, list) or not 1 <= len(catalogs) <= 20:
            raise ValueError("Select between 1 and 20 catalogues.")
        if any(not isinstance(c, str) or not re.fullmatch(r"[0-9a-fA-F]{24}", c) for c in catalogs):
            raise ValueError("Invalid catalogue ID.")
        catalogs = [c.lower() for c in catalogs]
    return action, list(refs.values()), list(dict.fromkeys(catalogs)) if action == "add_to_catalogs" else []


def library_filter(search="", custom=False):
    query = {"tmdb_id": {"$lt": 0}} if custom else {}
    if search.strip():
        query["$or"] = [{field: {"$regex": re.escape(search.strip()), "$options": "i"}}
                        for field in ("title", "title_english", "original_title")]
    return query


def _sort_key(doc, index, media_type):
    date = doc.get("updated_on")
    if isinstance(date, datetime):
        stamp = date.replace(tzinfo=timezone.utc).timestamp() if date.tzinfo is None else date.timestamp()
    else:
        stamp = float("-inf")
    return stamp, str(doc.get("_id", "")), index, media_type


async def list_library(db, media_type="all", page=1, page_size=24, search="", custom=False):
    if media_type not in ("all", "movie", "tv"):
        raise ValueError("Type must be all, movie or tv.")
    query = library_filter(search, custom)
    types = ("movie", "tv") if media_type == "all" else (media_type,)
    sources = [(int(key[8:]), kind, storage[kind])
               for key, storage in db.dbs.items() if key.startswith("storage_")
               for kind in types]
    counts = await asyncio.gather(*(collection.count_documents(query) for _, _, collection in sources))
    total = sum(counts)
    total_pages = (total + page_size - 1) // page_size
    page = max(1, min(page, total_pages or 1))
    skip = (page - 1) * page_size
    cursors = [collection.find(query).sort([("updated_on", -1), ("_id", -1)]).batch_size(page_size)
               for _, _, collection in sources]

    async def next_doc(cursor):
        try:
            return await cursor.__anext__()
        except StopAsyncIteration:
            return None

    items = []
    try:
        heads = list(await asyncio.gather(*(next_doc(cursor) for cursor in cursors)))
        # Merge sorted cursors rather than loading every database into memory.
        for position in range(skip + page_size):
            available = [i for i, doc in enumerate(heads) if doc is not None]
            if not available:
                break
            winner = max(available, key=lambda i: _sort_key(heads[i], sources[i][0], sources[i][1]))
            doc = heads[winner]
            if position >= skip:
                item = {**doc, "db_index": sources[winner][0], "media_type": sources[winner][1]}
                item.pop("_id", None)
                item["catalog_tags"] = []
                items.append(item)
            heads[winner] = await next_doc(cursors[winner])
    finally:
        await asyncio.gather(*(cursor.close() for cursor in cursors))

    if items:
        refs = [media_ref(item) for item in items]
        by_ref = {ref_key(item): item for item in items}
        cursor = db.dbs["tracking"]["custom_catalogs"].find(
            {"items": {"$elemMatch": {"$or": refs}}}, {"name": 1, "items": 1},
        )
        async for catalog in cursor:
            for ref in catalog.get("items", []):
                item = by_ref.get((ref.get("media_type"), ref.get("db_index"), ref.get("tmdb_id")))
                if item is not None:
                    tag = {"id": str(catalog["_id"]), "name": catalog.get("name") or "Catalogue"}
                    if tag not in item["catalog_tags"]:
                        item["catalog_tags"].append(tag)
    result = {"items": items, "total_count": total, "current_page": page, "total_pages": total_pages,
              "databases_checked": sorted({index for index, _, _ in sources})}
    if media_type != "all":
        result["movies" if media_type == "movie" else "tv_shows"] = items
    return result


async def bulk_library(db, payload, delete_one, add_one):
    action, refs, catalog_ids = normalize_batch(payload)
    # Validate the whole request before making any changes.
    catalogs = []
    for catalog_id in catalog_ids:
        catalog = await db.get_custom_catalog(catalog_id)
        if not catalog or catalog.get("auto"):
            raise ValueError("Select existing custom catalogues only.")
        catalogs.append(catalog)
    if len(catalogs) > 1 and any(c.get("exclusive") for c in catalogs):
        raise ValueError("An exclusive catalogue cannot be combined with other catalogues.")

    results = []
    for ref in refs:
        completed_catalogs = []
        try:
            if f"storage_{ref['db_index']}" not in db.dbs:
                raise ValueError("The selected database is no longer available.")
            doc = await db.get_document(ref["media_type"], ref["tmdb_id"], ref["db_index"])
            if not doc:
                raise ValueError("Title no longer exists; refresh the library.")
            if action == "delete":
                await delete_one(ref["tmdb_id"], ref["db_index"], ref["media_type"])
            else:
                exclusive = doc.get("exclusive_catalog_id")
                if exclusive and any(c != exclusive for c in catalog_ids):
                    raise ValueError("Remove this title from its exclusive catalogue before assigning another.")
                for catalog_id in catalog_ids:
                    await add_one(catalog_id, ref)
                    completed_catalogs.append(catalog_id)
            results.append({**ref, "ok": True, "catalog_ids": completed_catalogs})
        except Exception as exc:
            results.append({**ref, "ok": False, "error": str(getattr(exc, "detail", None) or exc),
                            "catalog_ids": completed_catalogs})
    succeeded = sum(result["ok"] for result in results)
    return {"results": results, "succeeded": succeeded, "failed": len(results) - succeeded}
