"""Collect video/document sizes alongside the original link-checking logic."""
from pymongo import UpdateOne


def collect_file_size(message, chat_id, msg_id, sizes):
    if sizes is None:
        return
    media = getattr(message, "video", None) or getattr(message, "document", None)
    size = getattr(media, "file_size", None)
    if isinstance(size, int) and size > 0:
        sizes.append({"chat_id": str(chat_id).removeprefix("-100"),
                      "msg_id": msg_id, "size_bytes": size})


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
