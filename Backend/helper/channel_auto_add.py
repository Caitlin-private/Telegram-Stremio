"""Persistent, opt-in capture of newly uploaded auth-channel media."""

import math
import secrets
from datetime import datetime
from hashlib import blake2b
from time import time

STATE_ID = "channel_auto_add"
UNTAGGED_ID = "756e746167676564636174616c"
METADATA_FIELDS = ("title", "year", "rate", "genres", "description", "poster", "backdrop")


def session_status(session, now=None):
    now = time() if now is None else now
    if not session or not session.get("enabled"):
        return "off"
    if session.get("expires_at") is not None and now >= session["expires_at"]:
        return "expired"
    return "scheduled" if now < session["starts_at"] else "active"


def accepts_message(session, uploaded_at, now=None):
    if session_status(session, now) != "active":
        return False
    # Telegram timestamps have second precision. Do not capture older backlog.
    if uploaded_at < math.floor(session["starts_at"]):
        return False
    return session.get("expires_at") is None or uploaded_at < session["expires_at"]


def _integer(value, label, minimum=0, maximum=10000):
    if isinstance(value, bool) or not str(value).isdigit():
        raise ValueError(f"{label} must be a whole number.")
    value = int(value)
    if not minimum <= value <= maximum:
        raise ValueError(f"{label} must be between {minimum} and {maximum}.")
    return value


def build_session(payload, now=None):
    now = time() if now is None else now
    duration = payload.get("duration_minutes", 5)
    if duration is not None:
        duration = _integer(duration, "Duration", 1, 1440)
    delay = _integer(payload.get("delay_minutes", 0), "Start delay", 0, 1440)
    media_type = payload.get("media_type", "movie")
    if media_type not in ("movie", "tv"):
        raise ValueError("Type must be movie or tv.")
    raw = payload.get("manual_metadata") or {}
    if not isinstance(raw, dict):
        raise ValueError("Custom metadata must be an object.")
    fields = {}
    for key in METADATA_FIELDS:
        value = raw.get(key)
        if value is None:
            continue
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise ValueError(f"Invalid {key}.")
        value = str(value).strip()
        if len(value) > 10000:
            raise ValueError(f"{key} is too long.")
        if value:
            fields[key] = value
    if "year" in fields:
        fields["year"] = _integer(fields["year"], "Year", 0, 9999)
    if "rate" in fields:
        try:
            rate = float(fields["rate"])
        except (ValueError, TypeError):
            raise ValueError("Rating must be between 0 and 10.")
        if not math.isfinite(rate) or not 0 <= rate <= 10:
            raise ValueError("Rating must be between 0 and 10.")
        fields["rate"] = rate
    catalogs = payload.get("catalog_ids") or []
    if not isinstance(catalogs, list) or any(not isinstance(c, str) for c in catalogs):
        raise ValueError("Catalogue IDs must be a list of strings.")
    catalogs = list(dict.fromkeys(c.strip() for c in catalogs if c.strip()))
    session = {
        "enabled": True, "session_id": secrets.token_hex(12),
        "starts_at": now + delay * 60,
        "expires_at": now + (delay + duration) * 60 if duration is not None else None,
        "duration_minutes": duration, "delay_minutes": delay,
        "media_type": media_type, "manual_metadata": fields, "catalog_ids": catalogs,
        "quality": str(payload.get("quality") or "").strip()[:100],
        "episode_title": str(payload.get("episode_title") or "").strip()[:1000],
    }
    for key in ("season_number", "episode_number"):
        value = payload.get(key)
        session[key] = None if value in (None, "") else _integer(value, key)
    session["untagged"] = not (fields or catalogs or session["episode_title"] or
                               any(session[k] is not None for k in ("season_number", "episode_number")))
    return session


async def get_session(db):
    session = await db.dbs["tracking"]["state"].find_one({"_id": STATE_ID}) or {}
    session.pop("_id", None)
    return {**session, "status": session_status(session)}


async def start_session(db, payload):
    session = build_session(payload)
    catalogs = []
    for catalog_id in session["catalog_ids"]:
        catalog = await db.get_custom_catalog(catalog_id)
        if not catalog or catalog.get("auto"):
            raise ValueError("Select existing custom catalogues only.")
        catalogs.append(catalog)
    if len(catalogs) > 1 and any(c.get("exclusive") for c in catalogs):
        raise ValueError("An exclusive catalogue cannot be combined with other catalogues.")
    await db.dbs["tracking"]["state"].replace_one(
        {"_id": STATE_ID}, {"_id": STATE_ID, **session}, upsert=True,
    )
    return {**session, "status": session_status(session)}


async def stop_session(db):
    await db.dbs["tracking"]["state"].update_one(
        {"_id": STATE_ID}, {"$set": {"enabled": False}}, upsert=True,
    )
    return {"status": "off", "enabled": False}


def capture_identity(session, channel, message_id, split_key=None):
    # Independent files never replace one another just because their names match.
    key = f"{channel}:{message_id}"
    if split_key:
        key = f"{session['session_id']}:{channel}:{split_key}"
    number = int.from_bytes(blake2b(key.encode(), digest_size=6).digest(), "big") + 1
    return -number, f"tgauto{number}"


def capture_metadata(session, filename, parsed, channel, message_id, split_key=None):
    fields = session["manual_metadata"]
    tmdb_id, imdb_id = capture_identity(session, channel, message_id, split_key)
    media_type = "movie" if session["untagged"] else session["media_type"]
    meta = {
        "tmdb_id": tmdb_id, "imdb_id": imdb_id, "media_type": media_type,
        "title": fields.get("title") or filename,
        "year": fields.get("year", 0), "rate": fields.get("rate", 0),
        "genres": [g.strip() for g in fields.get("genres", "").split(",") if g.strip()],
        "description": fields.get("description", ""),
        "poster": fields.get("poster", ""), "backdrop": fields.get("backdrop", ""),
        "logo": "", "cast": [], "runtime": "", "is_anime": False,
        "quality": session.get("quality") or parsed.get("quality") or "Unknown",
    }
    if media_type == "tv":
        season = session.get("season_number")
        episode = session.get("episode_number")
        season = season if season is not None else (parsed.get("season") or 1)
        episode = episode if episode is not None else (parsed.get("episode") or 1)
        meta.update({
            "season_number": season, "episode_number": episode,
            "episode_title": session.get("episode_title") or f"S{season:02d}E{episode:02d}",
            "episode_backdrop": meta["backdrop"], "episode_overview": "", "episode_released": "",
        })
    return meta


async def capture_message(db, message, session):
    from bson import ObjectId
    from Backend.helper.encrypt import encode_string
    from Backend.helper.manual_add import quality_from_height
    from Backend.helper.metadata import gradient_cover_path, parse_media_name
    from Backend.helper.pyro import get_readable_file_size
    from Backend.helper.split_files import parse_split_info

    file = message.video or message.document
    channel = int(str(message.chat.id).removeprefix("-100"))
    filename = file.file_name or f"file-{message.id}"
    split = parse_split_info(filename)
    split_key = split[0] if split else None
    try:
        parsed = parse_media_name(filename) or {}
    except Exception:
        parsed = {}
    detected_quality = quality_from_height(getattr(file, "height", 0) or 0)
    if detected_quality:
        parsed["quality"] = detected_quality
    meta = capture_metadata(session, filename, parsed, channel, message.id, split_key)
    encoded = await encode_string({"chat_id": channel, "msg_id": message.id})
    meta["encoded_string"] = encoded
    if split:
        meta["group_key"] = f"capture:{session['session_id']}:{channel}:{split_key}"
        meta["part_number"] = split[1]
    elif filename.lower().endswith(".zip"):
        meta["group_key"] = f"capture:{channel}:{message.id}.zip"
        meta["part_number"] = 1
    meta["poster"] = meta["poster"] or gradient_cover_path(meta["title"], portrait=True)
    meta["backdrop"] = meta["backdrop"] or gradient_cover_path(meta["title"])

    # Replayed updates must not replace (and delete) an already indexed source.
    existing = await db.get_media_ids_by_part(channel, message.id)
    if existing and existing[0] != meta["imdb_id"]:
        return
    if not existing:
        result = await db.insert_media(
            meta, channel=channel, msg_id=message.id, name=filename,
            size=get_readable_file_size(file.file_size or 0), raw_size=file.file_size or 0,
        )
        if not result:
            raise RuntimeError("Could not save captured media.")
    location = await db.find_media_doc(meta["media_type"], meta["tmdb_id"])
    if not location:
        raise RuntimeError("Captured media could not be located.")
    catalog_ids = list(session["catalog_ids"])
    if session["untagged"]:
        # A reserved ObjectId makes concurrent creation idempotent; normal catalogue APIs work.
        now = datetime.utcnow()
        await db.dbs["tracking"]["custom_catalogs"].update_one(
            {"_id": ObjectId(UNTAGGED_ID)},
            {"$setOnInsert": {
                "name": "Untagged", "visibility": "public", "visible": True,
                "allowed_tokens": [], "exclusive": False, "searchable": False,
                "items": [], "created_at": now, "updated_at": now,
            }}, upsert=True,
        )
        catalog_ids = [UNTAGGED_ID]
    for catalog_id in catalog_ids:
        catalog = await db.get_custom_catalog(catalog_id)
        if not catalog:
            raise RuntimeError("A selected catalogue no longer exists.")
        await db.add_item_to_custom_catalog(catalog_id, meta["tmdb_id"], location[1], meta["media_type"])
        if catalog.get("visibility") in ("owner", "tokens"):
            await db.set_media_visibility(meta["tmdb_id"], location[1], meta["media_type"],
                                          catalog["visibility"], catalog.get("allowed_tokens") or [])
        if catalog.get("exclusive"):
            await db.mark_item_exclusive(catalog_id, meta["tmdb_id"], location[1],
                                         meta["media_type"], catalog.get("searchable", False))
