"""Shared live counts and owner-friendly stats formatting."""


async def library_totals(db):
    """Count on MongoDB; do not transfer entire episode/stream arrays to Python."""
    movies = series = episodes = streams = size = 0
    size_available = True
    for i in range(1, db.current_db_index + 1):
        storage = db.dbs.get(f"storage_{i}")
        if storage is None:
            continue
        movies += await storage["movie"].count_documents({})
        series += await storage["tv"].count_documents({})
        movie_rows = (
            await storage["movie"]
            .aggregate(
                [
                    {"$group": {"_id": None, "streams": {"$sum": {"$size": {"$ifNull": ["$telegram", []]}}}}},
                ]
            )
            .to_list(length=1)
        )
        episode_rows = (
            await storage["tv"]
            .aggregate(
                [
                    {"$unwind": "$seasons"},
                    {"$unwind": "$seasons.episodes"},
                    {
                        "$group": {
                            "_id": None,
                            "episodes": {"$sum": 1},
                            "streams": {"$sum": {"$size": {"$ifNull": ["$seasons.episodes.telegram", []]}}},
                        }
                    },
                ]
            )
            .to_list(length=1)
        )
        streams += movie_rows[0]["streams"] if movie_rows else 0
        if episode_rows:
            episodes += episode_rows[0]["episodes"]
            streams += episode_rows[0]["streams"]
        try:
            size += (await storage.command("dbStats"))["dataSize"]
        except Exception:
            size_available = False
    return {
        "movies": movies,
        "series": series,
        "episodes": episodes,
        "streams": streams,
        "files": streams,
        "titles": movies + series,
        "size_bytes": size if size_available else None,
        "size": f"{size / 1024**2:.2f} MiB" if size_available else "Unavailable",
    }


def live_counts(entries, now, stale_seconds=180):
    active = []
    for entry in entries:
        idle = now - (entry.get("last_ts") or entry.get("start_ts") or 0)
        if (entry.get("status") or "active") != "active" or idle > stale_seconds:
            continue
        if not entry.get("total_bytes") and idle > 60:
            continue
        active.append(entry)
    users = {e.get("meta", {}).get("token") for e in active}
    users.discard(None)
    users.discard("")
    return {"live_streams": len(active), "active_users": len(users)}


def format_stats(s):
    ram = s.get("ram") or {}
    free, total = ram.get("free_bytes"), ram.get("total_bytes")
    memory = (
        f"{free / 1024**3:.2f} GiB free out of {total / 1024**3:.2f} GiB"
        if free is not None and total is not None
        else "Unavailable"
    )
    return (
        "📊 Caitlin — System Stats\n\n"
        f"🧠 RAM: {memory}\n"
        f'🏷️ Version: {s["version"]}\n'
        f'⏱️ Uptime: {s["uptime"]}\n\n'
        "📚 Media Library\n"
        f'🎬 Movies: {s["movies"]:,}\n'
        f'📺 TV shows: {s["tv_shows"]:,}\n'
        f'🎞️ Episodes: {s["episodes"]:,}\n'
        f'📂 Files: {s["files"]:,}\n'
        f'📚 Total titles: {s["titles"]:,}\n'
        f'🗄️ DB size: {s["db_size"]}\n\n'
        "🟢 Live Activity\n"
        f'▶️ Live streams: {s["live_streams"]:,}\n'
        f'👥 Active users: {s["active_users"]:,}\n\n'
        "📥 Live Ingestion\n"
        f'⏯️ State: {"Paused" if s.get("ingestion_paused") else "Running"}\n'
    )


# ----- Shared system snapshot for HTTP and Telegram consumers
async def collect_system_stats(db, client):
    from time import time
    from Backend import StartTime, __version__
    from Backend.helper.pyro import get_readable_time, get_readable_file_size
    from Backend.helper.settings_manager import SettingsManager
    from Backend.logger import LOGGER
    from Backend.helper.ingestion_status import ingestion_snapshot
    from Backend.helper.system_memory import memory_status
    from Backend.helper.custom_dl import ACTIVE_STREAMS, STALE_STREAM_IDLE

    try:
        totals = await library_totals(db)

        return {
            "status": "success",
            "data": {
                "version": __version__,
                "ram": memory_status(),
                **await ingestion_snapshot(client),
                **live_counts(list(ACTIVE_STREAMS.values()), time(), STALE_STREAM_IDLE),
                "movies": totals["movies"],
                "tv_shows": totals["series"],
                "episodes": totals["episodes"],
                "streams": totals["streams"],
                "files": totals["files"],
                "titles": totals["titles"],
                "uptime": get_readable_time(int(time() - StartTime)),
                "db_size": (
                    get_readable_file_size(totals["size_bytes"]) if totals["size_bytes"] is not None else "Unavailable"
                ),
                "storage_dbs": db.current_db_index,
                "auth_channels": len(SettingsManager.current().auth_channels),
            },
        }
    except Exception as e:
        LOGGER.error(f"[Stats] Error: {e}")
        return {"status": "error", "message": str(e)}
