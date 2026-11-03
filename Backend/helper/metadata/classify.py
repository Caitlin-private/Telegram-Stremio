"""Movie-vs-TV evidence helpers (pure Python, no provider/network imports).

The ingestion pipeline used to guess "episode" from any leftover number in a
filename, so "Apollo 13 (1995) 1080p" became episode 13 of a series called
"Apollo". These helpers decide, from the raw filename text only, whether a
file carries *textual evidence* of being a TV episode, and whether it looks
like a movie (a release year and no such evidence).
"""
from __future__ import annotations

import re
from datetime import date

# SxxExx / S01.E02
_SXXEXX_RE = re.compile(r"(?i)(?<![a-z0-9])s\d{1,2}[._\s-]*e\d{1,4}(?![a-z0-9])")
# 1x05 (never matches 1920x1080: the leading digits must sit on a word edge)
_NXNN_RE = re.compile(r"(?i)(?<![a-z0-9])\d{1,2}x\d{2,3}(?![a-z0-9])")
# "Season 2" / "Series 02"
_SEASON_WORD_RE = re.compile(r"(?i)(?<![a-z])(?:season|series)[\s._-]*0*\d{1,2}(?!\d)")
# bare season tag, e.g. "Show S02 Complete"
_SEASON_TAG_RE = re.compile(r"(?i)(?<![a-z0-9])s0*\d{1,2}(?![a-z0-9])")
# "Ep 12" / "Episode 12" / "Eps 01"
_EP_WORD_RE = re.compile(r"(?i)(?<![a-z])(?:eps?|episodes?)[\s._-]*0*\d{1,4}(?!\d)")
# "E1172" glued tag
_E_TAG_RE = re.compile(r"(?i)(?<![a-z0-9])e0*\d{1,4}(?![a-z0-9])")
# "[Group] Title - 1172" / "Title - 016 720p": a dash then a 2-4 digit number
_DASH_EP_RE = re.compile(r"(?i)\s[-\u2013\u2014]\s*0*\d{2,4}(?=\s|\.|\[|\(|$)")
# a plausible release year
_YEAR_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
# explicit "movie / film" wording (used for anime)
_MOVIE_WORD_RE = re.compile(r"(?i)(?<![a-z])(?:the[\s._-]+)?(?:movie|film)(?![a-z])")


def _max_year() -> int:
    return date.today().year + 1


def find_year(name: str, parsed_year=None) -> int | None:
    """Return a plausible release year found in the parsed fields or raw name."""
    limit = _max_year()
    try:
        if parsed_year is not None and 1900 <= int(parsed_year) <= limit:
            return int(parsed_year)
    except (TypeError, ValueError):
        pass
    for m in _YEAR_RE.finditer(name or ""):
        y = int(m.group(1))
        if 1900 <= y <= limit:
            return y
    return None


def year_tokens(name: str) -> list[int]:
    """All plausible year-like tokens in order ("Blade.Runner.2049.2017" -> [2049, 2017]).

    With two of them the *last* one is the release year; earlier ones belong to
    the title ("2049", "1984", "2012").
    """
    limit = _max_year()
    out = []
    for m in _YEAR_RE.finditer(name or ""):
        y = int(m.group(1))
        if 1900 <= y <= limit:
            out.append(y)
    return out


def has_season_marker(name: str) -> bool:
    """True when the name explicitly says which season it is (SNN / Season N / SxxExx / NxNN)."""
    n = name or ""
    return bool(
        _SXXEXX_RE.search(n) or _NXNN_RE.search(n)
        or _SEASON_WORD_RE.search(n) or _SEASON_TAG_RE.search(n)
    )


def has_explicit_tv_marker(name: str, *, anime: bool = False) -> bool:
    """True when the *text* of the name carries episode/season evidence.

    ``anime=True`` also accepts the fansub style ``Title - 1172``.
    A bare number ("Apollo 13", "Toy Story 3") is never evidence.
    """
    n = name or ""
    if has_season_marker(n):
        return True
    if _EP_WORD_RE.search(n) or _E_TAG_RE.search(n):
        return True
    try:  # E01-E04 ranges / "Combined" packs
        from Backend.helper.split_files import parse_combined_episodes
        if parse_combined_episodes(n):
            return True
    except Exception:
        pass
    if anime and has_dash_episode(n):
        return True
    return False


def has_movie_word(name: str) -> bool:
    return bool(_MOVIE_WORD_RE.search(name or ""))


def looks_like_movie(name: str, parsed_year=None, *, anime: bool = False) -> bool:
    """A release year (or explicit "Movie") and no textual TV evidence."""
    if has_explicit_tv_marker(name, anime=anime):
        return False
    return find_year(name, parsed_year) is not None or has_movie_word(name)


def has_dash_episode(name: str) -> bool:
    """``Title - 1172`` style number, ignoring 4-digit release years."""
    limit = _max_year()
    for m in _DASH_EP_RE.finditer(name or ""):
        digits = re.sub(r"\D", "", m.group(0))
        if len(digits.lstrip("0")) == 4 and 1900 <= int(digits) <= limit:
            continue
        return True
    return False
