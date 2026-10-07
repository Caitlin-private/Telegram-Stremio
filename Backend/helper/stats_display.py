"""Shared live counts and owner-friendly stats formatting."""
def live_counts(entries, now, stale_seconds=180):
    active = []
    for entry in entries:
        idle = now - (entry.get('last_ts') or entry.get('start_ts') or 0)
        if (entry.get('status') or 'active') != 'active' or idle > stale_seconds:
            continue
        if not entry.get('total_bytes') and idle > 60:
            continue
        active.append(entry)
    users = {e.get('meta', {}).get('token') for e in active}
    users.discard(None)
    users.discard('')
    return {'live_streams': len(active), 'active_users': len(users)}


def format_stats(s):
    ram = s.get('ram') or {}
    free, total = ram.get('free_bytes'), ram.get('total_bytes')
    memory = (f'{free / 1024**3:.2f} GiB free out of {total / 1024**3:.2f} GiB'
              if free is not None and total is not None else 'Unavailable')
    return (
        '📊 Caitlin — System Stats\n\n'
        f'🧠 RAM: {memory}\n'
        f'🏷️ Version: {s["version"]}\n'
        f'⏱️ Uptime: {s["uptime"]}\n\n'
        '📚 Media Library\n'
        f'🎬 Movies: {s["movies"]:,}\n'
        f'📺 TV shows: {s["tv_shows"]:,}\n'
        f'🎞️ Episodes: {s["episodes"]:,}\n'
        f'🔗 Streams: {s["streams"]:,}\n'
        f'🗄️ DB size: {s["db_size"]}\n\n'
        '🟢 Live Activity\n'
        f'▶️ Live streams: {s["live_streams"]:,}\n'
        f'👥 Active users: {s["active_users"]:,}\n\n'
        '📥 Live Ingestion\n'
        f'⏳ Pending media (known): {s.get("pending_media", 0):,}\n'
        f'⚙️ Processing / waiting for lock: {s.get("media_handlers", 0):,}\n'
        f'🗂️ Queued for DB: {s.get("queued_media", 0):,}\n'
        f'💾 DB worker active: {s.get("writing_media", 0):,}\n'
        f'📨 Telegram updates waiting (all types): {s.get("telegram_updates_waiting") if s.get("telegram_updates_waiting") is not None else "Unavailable"}\n'
        f'⚠️ Queue task errors since restart: {s.get("ingestion_write_errors", 0):,}'
    )
