"""Shared poster selection for addon metadata and Telegram announcements."""
from Backend.helper.metadata import resolve_cover_url
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.fanart import fanart_artwork
from Backend.logger import LOGGER

#----- Turn a stored image reference into an absolute URL for Stremio clients.
#----- Rebinds gradient covers and app-served /thumb paths to the current hosts.
def _abs_media_url(value: str) -> str:
    value = resolve_cover_url(value)
    idx = value.find("/thumb/")
    return f"{SettingsManager.current().base_url}{value[idx:]}" if idx != -1 else value


BETTERPOSTER_DEFAULT = "https://btttr.cc/poster/imdb/poster-default/{imdb_id}.jpg"
RPDB_FREE = "https://api.ratingposterdb.com/t0-free-rpdb/imdb/poster-default/{imdb_id}.jpg"


def _poster_url(imdb_id: str, fallback: str, tmdb_id=None, media_type='movie', shape='poster', provider=None) -> str:
    settings = SettingsManager.current()
    if provider == 'same':
        provider = None
    if provider == 'default':
        return _abs_media_url(fallback)
    if provider == 'fanart':
        return _abs_media_url(fallback)  # Fanart requires asynchronous artwork lookup.
    if (provider == 'pictorium' or (provider is None and settings.pictorium_poster_enabled)):
        from Backend.helper.pictorium import poster_url
        return poster_url(settings.pictorium_poster_url, tmdb_id, imdb_id, media_type, shape=shape) or _abs_media_url(fallback)
    if shape == 'landscape':
        # Existing BetterPoster/RPDB templates are portrait endpoints.
        # Fanart backgrounds are resolved by the existing artwork lookup.
        return _abs_media_url(fallback)
    if imdb_id:
        if (provider == "spatial" or (provider is None and settings.spatial_poster_enabled)) and settings.spatial_poster:
            spatial_type = "series" if media_type in ("tv", "series") else "movie"
            return settings.spatial_poster.replace("{type}", spatial_type).replace("{imdb_id}", str(imdb_id))
        if provider == 'better' or (provider is None and settings.better_poster_enabled):
            template = settings.better_poster or BETTERPOSTER_DEFAULT
            return template.replace("{imdb_id}", str(imdb_id))
        if provider == 'rpdb' or (provider is None and settings.rpdb_enabled):
            key = settings.rpdb_api_key
            template = (
                f"https://api.ratingposterdb.com/{key}/imdb/poster-default/{{imdb_id}}.jpg"
                if key else RPDB_FREE
            )
            return template.replace("{imdb_id}", str(imdb_id))
    return _abs_media_url(fallback)


async def _apply_fanart(meta: dict, item: dict) -> None:
    if not SettingsManager.current().fanart_enabled:
        return
    try:
        art = await fanart_artwork(item.get("imdb_id"), item.get("tmdb_id"), item.get("media_type"))
    except Exception as e:
        LOGGER.warning(f"[FANART] artwork lookup failed for {item.get('imdb_id')}: {e}")
        return
    if art.get("poster"):
        meta["poster"] = art["poster"]
    if art.get("logo"):
        meta["logo"] = art["logo"]
    if art.get("background"):
        meta["background"] = art["background"]



