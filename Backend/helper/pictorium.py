"""Pictorium instance and exported poster-template URLs."""
import re
from urllib.parse import urlsplit

PLACEHOLDERS = {'type', 'tmdb_id', 'imdb_id', 'tmdb_id|imdb_id', 'shape'}


def validate_poster_url(value):
    value = str(value or '').strip()
    if not value:
        return ''
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username or parsed.password or parsed.fragment):
        raise ValueError('Enter an HTTP(S) Pictorium URL without credentials or a fragment.')
    parsed.port  # Validate malformed ports as well.
    fields = set(re.findall(r'\{([^{}]+)\}', value))
    if fields:
        if (fields - PLACEHOLDERS or '{type}' not in parsed.path
                or not any('{'+key+'}' in parsed.path for key in ('tmdb_id', 'imdb_id', 'tmdb_id|imdb_id'))
                or '/api/poster/' not in parsed.path):
            raise ValueError('Paste the Pictorium poster template containing {type} and a supported ID placeholder.')
    elif parsed.query or '/api/poster' in parsed.path or parsed.path.rstrip('/').endswith(('/configure', '/manifest.json')) or '/u/' in parsed.path:
        raise ValueError('Paste an instance base URL or the exported poster template, not a configure or manifest link.')
    return value if fields else value.rstrip('/')


def poster_url(template, tmdb_id, imdb_id, media_type):
    """Return None when the title lacks an ID required by the template."""
    if not template:
        return None
    try:
        tmdb = str(int(tmdb_id)) if int(tmdb_id) > 0 else ''
    except (TypeError, ValueError):
        tmdb = ''
    imdb = str(imdb_id or '')
    if not re.fullmatch(r'tt\d+', imdb):
        imdb = ''
    if '{' not in template:
        template = template.rstrip('/') + '/api/poster/{type}/{tmdb_id|imdb_id}'
    values = {'type': 'tv' if media_type in ('tv', 'series') else 'movie',
              'tmdb_id': tmdb, 'imdb_id': imdb, 'tmdb_id|imdb_id': tmdb or imdb, 'shape': 'poster'}
    for key, value in values.items():
        if '{'+key+'}' in template and not value:
            return None
        template = template.replace('{'+key+'}', value)
    return template
