"""Telegram verification and exact-size backfill shared by both link checkers."""
import asyncio

from pyrogram.errors import FloodWait
from pymongo import UpdateOne

from Backend.helper.encrypt import decode_string

ALIVE, DEAD, UNKNOWN = "alive", "dead", "unknown"


async def inspect_message(clients, chat_id, msg_id):
    try:
        chat_id, msg_id = int(chat_id), int(msg_id)
        if chat_id > 0:
            chat_id = int(f"-100{chat_id}")
    except (TypeError, ValueError):
        return UNKNOWN, None
    confirmed_dead = False
    for client in clients:
        while True:
            try:
                message = await client.get_messages(chat_id, msg_id)
                break
            except FloodWait as exc:
                await asyncio.sleep(getattr(exc, "value", 5) + 1)
            except Exception:
                message = None
                break
        # Transport/access failures are unknown, never evidence for deletion.
        if message is None:
            continue
        if message.empty:
            confirmed_dead = True
            continue
        media = next((getattr(message, name, None) for name in
                      ("document", "video", "audio", "animation", "voice", "video_note", "photo", "sticker")
                      if getattr(message, name, None)), None)
        if media is not None:
            size = getattr(media, "file_size", None)
            return ALIVE, size if isinstance(size, int) and size > 0 else None
        confirmed_dead = True
    return (DEAD if confirmed_dead else UNKNOWN), None


async def inspect_quality(clients, quality_id):
    try:
        decoded = await decode_string(quality_id)
        refs = decoded.get("parts") if "parts" in decoded else [decoded]
        if not refs:
            return UNKNOWN, []
        sizes, statuses = [], []
        for ref in refs:
            status, size = await inspect_message(clients, ref.get("chat_id"), ref.get("msg_id"))
            statuses.append(status)
            if size is not None:
                sizes.append({"chat_id": ref["chat_id"], "msg_id": ref["msg_id"], "size_bytes": size})
        status = DEAD if DEAD in statuses else UNKNOWN if UNKNOWN in statuses else ALIVE
        return status, sizes
    except Exception:
        return UNKNOWN, []


def size_updates(document_id, media_type, quality, sizes):
    """Target only the unchanged stream/part, never replace a stale media document."""
    root = "telegram" if media_type == "movie" else "seasons.episodes.telegram"
    path = "telegram.$[q]" if media_type == "movie" else "seasons.$[s].episodes.$[e].telegram.$[q]"
    operations = []
    for row in sizes:
        filters = [{"q.id": quality["id"]}]
        if media_type == "tv":
            filters.extend([{"s.episodes.telegram.id": quality["id"]},
                            {"e.telegram.id": quality["id"]}])
        if quality.get("parts"):
            part = next((p for p in quality["parts"] if str(p.get("chat_id")) == str(row["chat_id"])
                         and str(p.get("msg_id")) == str(row["msg_id"])), None)
            if part is None or part.get("size_bytes") == row["size_bytes"]:
                continue
            filters.append({"p.chat_id": part["chat_id"], "p.msg_id": part["msg_id"]})
            field = path + ".parts.$[p].size_bytes"
        else:
            if len(sizes) != 1 or quality.get("size_bytes") == row["size_bytes"]:
                continue
            field = path + ".size_bytes"
        operations.append(UpdateOne({"_id": document_id, root + ".id": quality["id"]},
                                    {"$set": {field: row["size_bytes"]}}, array_filters=filters))
    return operations
