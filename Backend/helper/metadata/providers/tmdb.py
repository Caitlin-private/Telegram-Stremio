"""TMDb metadata provider."""
from __future__ import annotations

from typing import Optional

from themoviedb import aioTMDb

from Backend.config import Telegram
from Backend.helper.metadata.common import (
    ensure_media_ids,
    logo_from_imdb,
    parse_year_range,
    ALT_TITLE_LOOKUPS,
    ALT_TITLES_CACHE,
    API_SEMAPHORE,
    MOVIE_YEAR_THRESHOLD,
    EPISODE_CACHE,
    STRONG_MATCH,
    TMDB_DETAILS_CACHE,
    TMDB_SEARCH_CACHE,
    TMDB_THRESHOLD,
    cached_call,
    format_runtime,
    format_tmdb_image,
    normalize_title,
    score_candidate,
    score_candidate_aliases,
    title_similarity,
)
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

_tmdb_client: aioTMDb | None = None
_tmdb_client_key: str | None = None


def tmdb_api_key() -> str:
    try:
        key = SettingsManager.current().tmdb_api
        if key:
            return key
    except Exception:
        pass
    return getattr(Telegram, "TMDB_API", "") or ""


def get_tmdb_client() -> aioTMDb:
    global _tmdb_client, _tmdb_client_key
    current_key = tmdb_api_key()
    if _tmdb_client is None or _tmdb_client_key != current_key:
        _tmdb_client = aioTMDb(key=current_key, language="en-US", region="US")
        _tmdb_client_key = current_key
    return _tmdb_client


def get_tmdb_logo(images) -> str:
    logos = getattr(images, "logos", None) if images else None
    if not logos:
        return ""
    for logo in logos:
        if getattr(logo, "iso_639_1", None) == "en" and getattr(logo, "file_path", None):
            return format_tmdb_image(logo.file_path, "w300")
    for logo in logos:
        if getattr(logo, "file_path", None):
            return format_tmdb_image(logo.file_path, "w300")
    return ""


def _extract_cast(details) -> list:
    credits = getattr(details, "credits", None) or {}
    cast = getattr(credits, "cast", []) or []
    return [getattr(c, "name", None) or getattr(c, "original_name", None) for c in cast]


def _tmdb_country_codes(details) -> list:
    codes: list = []
    for code in (getattr(details, "origin_country", None) or []):
        if code and code not in codes:
            codes.append(code)
    for country in (getattr(details, "production_countries", None) or []):
        code = getattr(country, "iso_3166_1", None) or (
            country.get("iso_3166_1") if isinstance(country, dict) else None
        )
        if code and code not in codes:
            codes.append(code)
    return codes


def tmdb_title_year(item, media_type: str) -> tuple:
    if media_type == "movie":
        date = getattr(item, "release_date", None)
        return getattr(item, "title", "") or "", getattr(date, "year", 0) if date else 0
    date = getattr(item, "first_air_date", None)
    return getattr(item, "name", "") or "", getattr(date, "year", 0) if date else 0


async def raw_search(title: str, media_type: str, year: Optional[int]):
    client = get_tmdb_client()
    async with API_SEMAPHORE:
        if media_type == "movie":
            results = await (
                client.search().movies(query=title, year=year)
                if year
                else client.search().movies(query=title)
            )
            if not results and year:
                results = await client.search().movies(query=title)
            return results
        return await client.search().tv(query=title)


async def _tmdb_alternative_titles(media_type: str, tmdb_id) -> list:
    if not tmdb_id:
        return []
    cache_key = (media_type, tmdb_id)

    async def _produce():
        titles: list = []
        try:
            client = get_tmdb_client()
            async with API_SEMAPHORE:
                target = client.movie(tmdb_id) if media_type == "movie" else client.tv(tmdb_id)
                alt = await target.alternative_titles()
            entries = list(getattr(alt, "titles", None) or []) + list(getattr(alt, "results", None) or [])
            titles = [t for t in (getattr(e, "title", "") for e in entries) if t]
        except Exception as e:
            LOGGER.warning(f"TMDb alternative-titles fetch failed for {media_type} id={tmdb_id}: {e}")
        return titles

    return await cached_call(ALT_TITLES_CACHE, cache_key, "alt_titles", _produce)


def _movie_adjust(score: float, query_title: str, query_year, titles: list, r_year) -> float:
    """Movie-only guard rails on top of the fuzzy score.

    * year gate: with a filename year, a candidate more than 2 years away can
      never pass, and exactly 2 years away only passes on a near-exact title;
    * a tiny bonus for an exact normalized-title match (used for tie-breaks).
    """
    qnorm = normalize_title(query_title)
    exact = any(t and normalize_title(t) == qnorm for t in titles)
    near_exact = exact or any(t and title_similarity(query_title, t) >= 0.95 for t in titles)
    try:
        qy, ry = int(query_year or 0), int(r_year or 0)
    except (TypeError, ValueError):
        qy = ry = 0
    if qy and ry:
        diff = abs(qy - ry)
        if diff > 2 or (diff == 2 and not near_exact):
            return min(score, TMDB_THRESHOLD - 0.01)
    if exact:
        score = min(1.0, score + 0.02)
    return score


def _top_candidate(scored: list):
    """Highest score; candidates within 0.03 of it are decided by popularity."""
    if not scored:
        return None, 0.0
    top = max(sc for sc, _, _ in scored)
    close = [(sc, it) for sc, it, _ in scored if sc >= top - 0.03]

    def _pop(entry):
        sc, it = entry
        return (
            float(getattr(it, "popularity", 0) or 0),
            float(getattr(it, "vote_count", 0) or 0),
            sc,
        )

    sc, it = max(close, key=_pop)
    return it, sc


async def pick_best(results, query_title: str, query_year: Optional[int], media_type: str, min_score: Optional[float] = None):
    if not results:
        return None
    is_movie = media_type == "movie"
    year_reliable = is_movie
    year_lower_bound = not year_reliable
    threshold = TMDB_THRESHOLD
    if is_movie and query_year:
        threshold = max(threshold, MOVIE_YEAR_THRESHOLD)
    if min_score is not None:
        threshold = max(threshold, min_score)
    scored = []
    best_item, best_score = None, 0.0
    for item in results:
        r_title, r_year = tmdb_title_year(item, media_type)
        # original_title / original_name also counts as an alias
        orig = getattr(item, "original_title", None) or getattr(item, "original_name", None) or ""
        score = score_candidate_aliases(
            query_title, query_year, r_title, r_year,
            aliases=[orig] if orig and orig != r_title else None,
            year_reliable=year_reliable, year_lower_bound=year_lower_bound,
        )
        if is_movie:
            score = _movie_adjust(score, query_title, query_year, [r_title, orig], r_year)
        scored.append((score, item, r_year))
    if is_movie:
        best_item, best_score = _top_candidate(scored)
    else:
        for score, item, _ in scored:
            if score > best_score:
                best_score, best_item = score, item

    if best_score >= max(STRONG_MATCH, threshold):
        return best_item

    # Fetch official alternative titles for top candidates
    scored.sort(key=lambda x: x[0], reverse=True)
    for _, item, r_year in scored[:ALT_TITLE_LOOKUPS]:
        r_title, _ = tmdb_title_year(item, media_type)
        orig = getattr(item, "original_title", None) or getattr(item, "original_name", None) or ""
        alt_titles = await _tmdb_alternative_titles(media_type, getattr(item, "id", None))
        aliases = list(alt_titles or [])
        if orig:
            aliases.append(orig)
        alt_score = score_candidate_aliases(
            query_title, query_year, r_title, r_year,
            aliases=aliases,
            year_reliable=year_reliable, year_lower_bound=year_lower_bound,
        )
        if is_movie:
            alt_score = _movie_adjust(alt_score, query_title, query_year, [r_title, *aliases], r_year)
        if alt_score > best_score:
            best_score, best_item = alt_score, item
            if best_score >= max(STRONG_MATCH, threshold):
                break

    return best_item if best_score >= threshold and best_item is not None else None


async def safe_search(title: str, type_: str, year: Optional[int] = None, min_score: Optional[float] = None):
    is_tv = type_ != "movie"
    search_year = None if is_tv else year
    cache_key = f"tmdb_search::{type_}::{title}::{year}::{min_score}"

    async def _produce():
        try:
            results = await raw_search(title, type_, search_year)
            best = await pick_best(results, title, year, type_, min_score=min_score)
            if best is None and results:
                top = results[0]
                top_title = getattr(top, "title" if type_ == "movie" else "name", "?")
                LOGGER.info(f"TMDb '{title}' (year={year}) top result '{top_title}' did not meet threshold")
            return best
        except Exception as e:
            LOGGER.error(f"TMDb search failed for '{title}' [{type_}]: {e}")
            return None

    return await cached_call(TMDB_SEARCH_CACHE, cache_key, "tmdb_search", _produce)


async def details(media_type: str, item_id):
    cache_key = (media_type, item_id)

    async def _produce():
        try:
            client = get_tmdb_client()
            async with API_SEMAPHORE:
                target = client.movie(item_id) if media_type == "movie" else client.tv(item_id)
                det = await target.details(append_to_response="external_ids,credits")
                det.images = await target.images()
            return det
        except Exception as e:
            LOGGER.warning(f"TMDb {media_type} details fetch failed for id={item_id}: {e}")
            return None

    return await cached_call(TMDB_DETAILS_CACHE, cache_key, "tmdb_details", _produce)


async def episode_details(tv_id, season, episode):
    key = (tv_id, season, episode)

    async def _produce():
        try:
            async with API_SEMAPHORE:
                return await get_tmdb_client().episode(tv_id, season, episode).details()
        except Exception:
            return None

    return await cached_call(EPISODE_CACHE, key, "tmdb_ep", _produce)


async def external_imdb_id(media_type: str, tmdb_id) -> str | None:
    try:
        det = await details(media_type, tmdb_id)
        ext = getattr(det, "external_ids", None) if det else None
        return getattr(ext, "imdb_id", None) if ext else None
    except Exception:
        return None


def build_movie_payload(movie, quality, encoded_string) -> dict:
    release = getattr(movie, "release_date", None)
    title = movie.title or getattr(movie, "original_title", "") or ""
    eng = title
    orig = getattr(movie, "original_title", None) or title
    payload = {
        "tmdb_id": movie.id,
        "imdb_id": getattr(getattr(movie, "external_ids", None), "imdb_id", None),
        "title": title,
        "title_english": eng,
        "original_title": orig if orig != title else "",
        "year": getattr(release, "year", 0) if release else 0,
        "rate": getattr(movie, "vote_average", 0) or 0,
        "description": movie.overview or "",
        "poster": format_tmdb_image(movie.poster_path),
        "backdrop": format_tmdb_image(movie.backdrop_path, "original"),
        "logo": get_tmdb_logo(getattr(movie, "images", None)) or logo_from_imdb(
            getattr(getattr(movie, "external_ids", None), "imdb_id", None)
        ),
        "cast": _extract_cast(movie),
        "runtime": str(format_runtime(getattr(movie, "runtime", None))),
        "media_type": "movie",
        "genres": [g.name for g in (movie.genres or [])],
        "original_language": getattr(movie, "original_language", None),
        "origin_country": _tmdb_country_codes(movie),
        "quality": quality,
        "encoded_string": encoded_string,
    }
    return ensure_media_ids(payload, seed=f"tmdb:movie:{movie.id}")


def build_tv_payload(tv, ep, season, episode, quality, encoded_string) -> dict:
    first_air = getattr(tv, "first_air_date", None)
    last_air = getattr(tv, "last_air_date", None)
    year, year_end = parse_year_range(
        getattr(first_air, "year", None) if first_air else None,
        getattr(last_air, "year", None) if last_air else None,
    )
    series_runtime = tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
    runtime = format_runtime((getattr(ep, "runtime", None) if ep else None) or series_runtime)
    fallback_ep_title = f"S{season:02d}E{episode:02d}"
    title = tv.name or getattr(tv, "original_name", "") or ""
    orig = getattr(tv, "original_name", None) or title
    payload = {
        "tmdb_id": tv.id,
        "imdb_id": getattr(getattr(tv, "external_ids", None), "imdb_id", None),
        "title": title,
        "title_english": title,
        "original_title": orig if orig != title else "",
        "year": year,
        "year_end": year_end,
        "rate": getattr(tv, "vote_average", 0) or 0,
        "description": tv.overview or "",
        "poster": format_tmdb_image(tv.poster_path),
        "backdrop": format_tmdb_image(tv.backdrop_path, "original"),
        "logo": get_tmdb_logo(getattr(tv, "images", None)) or logo_from_imdb(
            getattr(getattr(tv, "external_ids", None), "imdb_id", None)
        ),
        "genres": [g.name for g in (tv.genres or [])],
        "media_type": "tv",
        "cast": _extract_cast(tv),
        "runtime": str(runtime),
        "original_language": getattr(tv, "original_language", None),
        "origin_country": _tmdb_country_codes(tv),
        "season_number": season,
        "episode_number": episode,
        "episode_title": getattr(ep, "name", fallback_ep_title) if ep else fallback_ep_title,
        "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
        "episode_overview": getattr(ep, "overview", "") if ep else "",
        "episode_released": (
            ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z")
            if (ep and getattr(ep, "air_date", None))
            else ""
        ),
        "quality": quality,
        "encoded_string": encoded_string,
    }
    return ensure_media_ids(payload, seed=f"tmdb:tv:{tv.id}")
