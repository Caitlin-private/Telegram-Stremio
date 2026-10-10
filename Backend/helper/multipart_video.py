"""Independently playable video parts (never byte-concatenated archives)."""
import re

PART = re.compile(
    r'(?i)(?<![a-z0-9])(?:part|cd|disc|disk)[ ._-]*'
    r'(\d+)(?=$|[\s._\-\[\]()])'
)
VIDEO_EXT = re.compile(r'(?i)\.(?:mkv|mp4|avi|mov|webm|m4v|ts)$')


def video_part(filename):
    filename = str(filename or '').strip()
    extension = VIDEO_EXT.search(filename)
    if not extension:
        return None
    stem = filename[:extension.start()]
    # Three-digit part numbers may precede release tags. Short numbers must
    # be at the end, avoiding movie titles such as "Example Part 2 (2024)".
    match = next((m for m in PART.finditer(stem)
                  if len(m[1]) >= 3 or not stem[m.end():].strip(' ._-[]()')), None)
    if not match or int(match[1]) < 1:
        return None
    clean = filename[:match.start()] + filename[match.end():]
    group = re.sub(r'[\s._-]+', ' ', clean.lower()).strip()
    return {'number': int(match[1]), 'group': group, 'clean': clean}


def with_video_part(quality):
    """Recover missing part fields in old/manual records without a DB migration."""
    if quality.get('group_key') or quality.get('parts'):
        return quality  # Byte-split archives use a different playback path.
    part = video_part(quality.get('name'))
    if not part:
        return quality
    return {**quality, 'video_part': part['number'], 'video_group': part['group']}


def part_sort_key(item):
    item = with_video_part(item)
    return (bool(item.get('video_part')), item.get('video_group') or '', item.get('video_part') or 0)
