"""Independently playable video parts (never byte-concatenated archives)."""
import re

PART = re.compile(r'(?i)(?:[ ._-]+|^)(?:part|cd|disc|disk)[s ._-]*0*(\d+)(?=\.(?:mkv|mp4|avi|mov|webm|m4v|ts)$)')


def video_part(filename):
    match = PART.search(filename or '')
    if not match or int(match[1]) < 1:
        return None
    clean = filename[:match.start()] + filename[match.end():]
    group = re.sub(r'[\s._-]+', ' ', clean.lower()).strip()
    return {'number': int(match[1]), 'group': group, 'clean': clean}


def part_sort_key(item):
    return (bool(item.get('video_part')), item.get('video_group') or '', item.get('video_part') or 0)
