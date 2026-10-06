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

_PIXELS = re.compile(r'(?i)(?<![a-z0-9])(240|360|480|540|576|720|1080|1440|2160|4320)[pi](?![a-z0-9])')
_ALIASES = re.compile(r'(?i)(?<![a-z0-9])(?<!dts[ ._-])(?:nHD|qHD|FHD|Quarter[ ._-]+HD|Full[ ._-]+HD|HD)(?![a-z0-9])(?:\s*\((?:quarter|full)\s+hd\))?')
_RESOLUTIONS = {'nhd': '360p', 'qhd': '540p', 'hd': '720p', 'fhd': '1080p'}
_LEADING_EPISODE = re.compile(r'^\s*(\d{1,5})(?:\s*[.\-_:)]\s*|\s+)(?=\S)')


def _alias_quality(match):
    label = match[0].lower().split('(')[0].strip()
    if label.startswith('quarter'):
        return '540p'
    if label.startswith('full'):
        return '1080p'
    return _RESOLUTIONS[label]


def resolution_hint(*names):
    """Explicit pixels win over an alias, including contradictory filenames."""
    for name in names:
        explicit = _PIXELS.search(name or '')
        if explicit:
            return explicit[1] + 'p'
        if re.search(r'(?i)(?<![a-z0-9])(?:4k|uhd)(?![a-z0-9])', name or ''):
            return '2160p'
    for name in names:
        alias = _ALIASES.search(name or '')
        if alias:
            return _alias_quality(alias)
    return None


def normalize_resolution_aliases(name):
    quality = resolution_hint(name)
    # Remove contradictory labels from the parsed title without introducing
    # another resolution for PTN/GuessIt to select.
    has_pixels = _PIXELS.search(name or '') or re.search(r'(?i)\b(?:4k|uhd)\b', name or '')
    return _ALIASES.sub(lambda m: ' ' if has_pixels else quality, name or '')


def nonnegative_number(value):
    if isinstance(value, bool) or not re.fullmatch(r'\d+', str(value).strip()):
        return None
    return int(value)


def leading_episode(*texts):
    """Prefer the message/caption, then the original filename; no guessed E1."""
    for text in texts:
        text = (text or '').lstrip('\ufeff\u200b\u200e\u200f \n\r\t')
        match = _LEADING_EPISODE.match(text)
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


def ordered_episodes(doc, season, stream_ids):
    """Retried links keep their episode; only new messages advance the count."""
    previous = {}
    for current in (doc or {}).get('seasons', []) or []:
        if nonnegative_number(current.get('season_number')) != season:
            continue
        for episode in current.get('episodes', []) or []:
            number = nonnegative_number(episode.get('episode_number'))
            if number is not None:
                for quality in episode.get('telegram') or []:
                    if quality.get('id'):
                        previous[quality['id']] = number
    number = next_episode(doc, season)
    result = []
    for stream_id in stream_ids:
        if stream_id not in previous:
            previous[stream_id] = number
            number += 1
        result.append(previous[stream_id])
    return result
