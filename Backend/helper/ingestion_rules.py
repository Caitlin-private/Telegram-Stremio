"""Small Caitlin parsing extensions shared by ingestion entry points."""
import re
from asyncio import Lock
from functools import wraps

manual_ingestion_lock = Lock()


def serialize_manual_ingestion(function):
    @wraps(function)
    async def wrapped(*args, **kwargs):
        async with manual_ingestion_lock:
            return await function(*args, **kwargs)
    return wrapped

_PIXELS = re.compile(r'(?i)(?<![a-z0-9])(240|360|480|540|576|720|1080|1440|2160|4320)p(?![a-z0-9])')
_ALIASES = re.compile(r'(?i)(?<![a-z0-9])(nhd|qhd|fhd|hd)(?![a-z0-9])')
_RESOLUTIONS = {'nhd': '360p', 'qhd': '540p', 'hd': '720p', 'fhd': '1080p'}
_LEADING_EPISODE = re.compile(r'^\s*(\d{1,5})(?:\s*[.\-_:]\s*|\s+)(?=\S)')


def resolution_hint(name):
    """Explicit pixels win over an alias, including contradictory filenames."""
    explicit = _PIXELS.search(name or '')
    if explicit:
        return explicit[1] + 'p'
    alias = _ALIASES.search(name or '')
    return _RESOLUTIONS[alias[1].lower()] if alias else None


def normalize_resolution_aliases(name):
    if _PIXELS.search(name or ''):
        return name
    return re.sub(
        r'(?i)(?<![a-z0-9])(nhd|qhd|fhd|hd)(?![a-z0-9])(?:\s*\((?:quarter|full)\s+hd\))?',
        lambda m: _RESOLUTIONS[m[1].lower()], name or '',
    )


def leading_episode(*texts):
    """Prefer the message/caption, then the original filename; no guessed E1."""
    for text in texts:
        match = _LEADING_EPISODE.match(text or '')
        if match and int(match[1]) > 0:
            return int(match[1])
    raise ValueError('No episode number at the start of the message or filename. Use a name such as 12. The Apothecary Diaries.mkv.')


def episode_mode(value, default='fixed'):
    mode = str(value or default)
    if mode not in ('fixed', 'order', 'filename'):
        raise ValueError('Episode detection must be fixed, order or filename.')
    return mode


def next_episode(doc, season):
    def _same_season(value):
        try:
            return int(value) == int(season)
        except (TypeError, ValueError):
            return value == season

    numbers = []
    for current in (doc or {}).get('seasons', []) or []:
        if not _same_season(current.get('season_number')):
            continue
        for episode in current.get('episodes', []) or []:
            try:
                numbers.append(int(episode.get('episode_number') or 0))
            except (TypeError, ValueError):
                continue
    return max(numbers, default=0) + 1
