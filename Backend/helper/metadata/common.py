"""Shared helpers for metadata providers and resolvers."""
from __future__ import annotations

import asyncio
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Optional
from urllib.parse import quote

from rapidfuzz import fuzz

from Backend.logger import LOGGER
from Backend.helper.async_cache import TTLCache, cached_call

# Match thresholds
CINEMETA_THRESHOLD = 0.60
TMDB_THRESHOLD = 0.55
TVDB_THRESHOLD = 0.55
KITSU_THRESHOLD = 0.55
STRONG_MATCH = 0.92
# Movie accept threshold once the filename carries a release year
MOVIE_YEAR_THRESHOLD = 0.75
ALT_TITLE_LOOKUPS = 5

# Combined-file constants (Specials season)
COMBINED_SEASON = 0
COMBINED_EPISODE_BASE = 1000

GRADIENT_COVER_BASE = "https://gradient-cover-api.vercel.app"

API_SEMAPHORE = asyncio.Semaphore(12)

# Shared caches (provider modules may also keep their own)
IMDB_CACHE = TTLCache(maxsize=300)
TMDB_SEARCH_CACHE = TTLCache(maxsize=300)
TMDB_DETAILS_CACHE = TTLCache(maxsize=300)
EPISODE_CACHE = TTLCache(maxsize=500)
ALT_TITLES_CACHE = TTLCache(maxsize=300)
TVDB_CACHE = TTLCache(maxsize=300)
KITSU_CACHE = TTLCache(maxsize=300)


_APOSTROPHE_RE = re.compile(r"['\u2018\u2019`\u00B4]")
_SYMBOL_STRIP_RE = re.compile(r"[&.\-:]+")
_HTML_RE = re.compile(r"<[^>]+>")



def strip_html(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", _HTML_RE.sub(" ", text)).strip()


def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = title.lower().strip()
    t = re.sub(r"^\b(the|a|an)\b\s+", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def fuzzy_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    try:
        set_ratio = fuzz.token_set_ratio(a, b) / 100.0
        sort_ratio = fuzz.token_sort_ratio(a, b) / 100.0
        a_tokens, b_tokens = a.split(), b.split()
        coverage = (
            min(len(a_tokens), len(b_tokens)) / max(len(a_tokens), len(b_tokens))
            if a_tokens and b_tokens
            else 0.0
        )
        return max(sort_ratio, set_ratio * coverage)
    except Exception:
        return SequenceMatcher(None, a, b).ratio()


def title_similarity(t1: str, t2: str) -> float:
    n1, n2 = normalize_title(t1), normalize_title(t2)
    return fuzzy_ratio(n1, n2) if n1 and n2 else 0.0


# ── Token-coverage guard ─────────────────────────────────────────────────────
# Fuzzy ratios alone accept look-alikes: "Cobra Kai" ~ "Cobra" (alias of
# "Space Adventure Cobra") = 0.71 and "Mysterious Nurse" ~ "Delicious Nursery"
# = 0.67, both above the 0.55 accept threshold.  A candidate is only trusted
# below STRONG_MATCH when it really contains the query's significant words.
_NOISE_TOKENS = frozenset({
    # articles / glue
    "the", "a", "an", "of", "and", "in", "on", "to", "at", "for", "vs",
    # brand prefixes that titles often drop ("Marvel's Agents of S.H.I.E.L.D.")
    "marvel", "marvels", "dc", "disney", "disneys", "netflix", "hbo", "bbc",
    "amazon", "apple", "pixar", "lucasfilm", "nickelodeon", "paramount",
    # release noise
    "original", "uncut", "extended", "remastered", "complete", "final",
    "edition", "dubbed", "dub", "subbed", "sub",
})
_ROMAN_TOKENS = {
    "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6",
    "vii": "7", "viii": "8", "ix": "9", "x": "10",
}


def _significant_tokens(normalized: str) -> list:
    out = []
    for tok in normalized.split():
        tok = _ROMAN_TOKENS.get(tok, tok)
        if tok in _NOISE_TOKENS or (len(tok) < 2 and not tok.isdigit()):
            continue
        out.append(tok)
    return out


def _tokens_close(a: str, b: str) -> bool:
    if a == b:
        return True
    if len(a) < 4 or len(b) < 4 or a.isdigit() or b.isdigit():
        return False
    try:
        # "nurse"/"nurses" pass (0.91); "nurse"/"nursery" (0.83) and
        # "mysterious"/"delicious" (0.53) do not.
        return fuzz.ratio(a, b) >= 85
    except Exception:
        return False


def title_tokens_covered(query: str, candidate: str) -> bool:
    """True when the candidate contains the query's significant words.

    Titles of one or two significant words must be fully covered; longer
    titles may miss a single word.
    """
    q = _significant_tokens(normalize_title(query))
    c = _significant_tokens(normalize_title(candidate))
    if not q or not c:
        return True  # nothing meaningful to judge
    cset = set(c)
    missing = [t for t in q if t not in cset and not any(_tokens_close(t, x) for x in cset)]
    return len(missing) <= (0 if len(q) <= 2 else 1)


def guarded_title_similarity(query: str, candidate: str) -> float:
    """title_similarity, capped below every accept threshold when the candidate
    lacks words from the query (unless it is already a near-exact match)."""
    score = title_similarity(query, candidate)
    if score < STRONG_MATCH and not title_tokens_covered(query, candidate):
        return min(score, 0.49)
    return score


def year_from_str(year_val) -> int:
    if not year_val:
        return 0
    m = re.search(r"(\d{4})", str(year_val))
    return int(m.group(1)) if m else 0


def strip_symbols(text: str) -> str:
    if not text:
        return ""
    text = _APOSTROPHE_RE.sub("", text)
    text = _SYMBOL_STRIP_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def score_candidate(
    query_title: str,
    query_year: Optional[int],
    result_title: str,
    result_year: int,
    year_reliable: bool = True,
    year_lower_bound: bool = False,
) -> float:
    score = guarded_title_similarity(query_title, result_title)
    if score < 0.5:
        return score

    if query_year and result_year:
        if year_lower_bound:
            if int(query_year) >= result_year and score >= 0.80:
                score += 0.15 / (1 + (int(query_year) - result_year) * 0.1)
            elif int(query_year) < result_year - 1:
                # The show premiered after the file's year: it cannot be this show
                score = max(0.0, score - 0.10 * (result_year - int(query_year) - 1))
            return score
        diff = abs(int(query_year) - result_year)
        if year_reliable:
            if diff > 2:
                score = max(0.0, score - 0.10 * (diff - 2))
            elif score >= 0.80:
                if diff == 0:
                    score = min(1.0, score + 0.20)
                elif diff == 1:
                    score = min(1.0, score + 0.07)
        elif diff == 0 and score >= 0.80:
            score = min(1.0, score + 0.05)
    elif query_year and year_reliable and not year_lower_bound:
        score = max(0.0, score - 0.20)
    return score



def collect_title_aliases(*groups) -> list:
    """Flatten title / alias fields from provider payloads into unique strings."""
    out: list = []
    seen: set = set()
    for group in groups:
        if group is None:
            continue
        if isinstance(group, str):
            items = [group]
        elif isinstance(group, dict):
            # translations / titles maps: use all values
            items = list(group.values())
        elif isinstance(group, (list, tuple, set)):
            items = []
            for x in group:
                if isinstance(x, dict):
                    # TVDB-style {"name": "..."} or {"title": "..."}
                    items.append(x.get("name") or x.get("title") or x.get("alias") or "")
                else:
                    items.append(x)
        else:
            items = [str(group)]
        for raw in items:
            t = str(raw or "").strip()
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
    return out


def score_candidate_aliases(
    query_title: str,
    query_year: Optional[int],
    primary_title: str,
    result_year: int,
    aliases=None,
    year_reliable: bool = True,
    year_lower_bound: bool = False,
) -> float:
    """Score a candidate using primary title + all aliases (best match wins).

    Used by TVDB / TMDB / Kitsu / Cinemeta so alternate names, translations,
    and abbreviations participate in matching — not only the canonical title.
    """
    titles = collect_title_aliases(primary_title, aliases)
    if not titles:
        return 0.0
    best = 0.0
    for t in titles:
        s = score_candidate(
            query_title, query_year, t, result_year,
            year_reliable=year_reliable,
            year_lower_bound=year_lower_bound,
        )
        if s > best:
            best = s
            if best >= STRONG_MATCH:
                break
    return best


def build_query_variants(title: str, year: Optional[int] = None) -> list:
    variants = [title]
    if year:
        variants.append(f"{title} {year}")
    stripped = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", title)).strip()
    if stripped and stripped.lower() != title.lower():
        variants.append(stripped)
        if year:
            variants.append(f"{stripped} {year}")
    no_article = re.sub(r"^\b(the|a|an)\b\s+", "", title, flags=re.IGNORECASE).strip()
    if no_article and no_article.lower() != title.lower():
        variants.append(no_article)
    seen: set = set()
    ordered = []
    for v in variants:
        key = v.lower()
        if v and key not in seen:
            seen.add(key)
            ordered.append(v)
    return ordered


def first(value):
    return value[0] if isinstance(value, list) else value


def format_runtime(minutes) -> str:
    return f"{minutes} min" if minutes else ""


def gradient_cover_path(title: str, portrait: bool = False) -> str:
    path = f"/api/image?text={quote((title or 'Media').strip() or 'Media')}&badge="
    return f"{path}&orientation=portrait" if portrait else path


def resolve_cover_url(value: str) -> str:
    value = str(value or "")
    idx = value.find("/api/image?")
    return f"{GRADIENT_COVER_BASE}{value[idx:]}" if idx != -1 else value


def format_tmdb_image(path: str, size="w500") -> str:
    return f"https://image.tmdb.org/t/p/{size}{path}" if path else ""


def format_imdb_images(imdb_id: str) -> dict:
    if not imdb_id:
        return {"poster": "", "backdrop": "", "logo": ""}
    return {
        "poster": f"https://images.metahub.space/poster/small/{imdb_id}/img",
        "backdrop": f"https://images.metahub.space/background/medium/{imdb_id}/img",
        "logo": f"https://images.metahub.space/logo/medium/{imdb_id}/img",
    }


ID_REFS = re.compile(r"(?:https?://)?(?:www\.|m\.)?(?:imdb\.com/title/tt\d+|themoviedb\.org/(?:movie|tv)/\d+)\S*|\btt\d{7,10}\b", re.I)


def has_raw_ref(text) -> bool:
    return bool(ID_REFS.search(text or ""))


#----- Caption plus the URLs hidden behind its text links (IMDB deeplinks)
def caption_with_links(message) -> str:
    urls = [e.url for e in (getattr(message, "caption_entities", None) or []) if getattr(e, "url", None)]
    return "\n".join([getattr(message, "caption", None) or "", *urls])


def extract_default_id(text: str) -> str | None:
    if not text:
        return None
    bare_imdb = re.search(r"\b(tt\d{7,10})\b", text)
    if bare_imdb:
        return bare_imdb.group(1)
    imdb_url = re.search(r"/title/(tt\d+)", text)
    if imdb_url:
        return imdb_url.group(1)
    tmdb_url = re.search(r"/(?:movie|tv)/(\d+)", text)
    if tmdb_url:
        return tmdb_url.group(1)
    return None


def split_default_id(default_id) -> tuple:
    """Returns (imdb_id, tmdb_id, explicit_imdb, use_tmdb)."""
    if not default_id:
        return None, None, False, False
    value = str(default_id).strip()
    if value.startswith("tt"):
        return value, None, True, False
    if value.isdigit():
        return None, int(value), False, True
    return None, None, False, False


def empty_payload_base() -> dict:
    return {
        "tmdb_id": None,
        "imdb_id": None,
        "title": "",
        "year": 0,
        "rate": 0,
        "description": "",
        "poster": "",
        "backdrop": "",
        "logo": "",
        "cast": [],
        "runtime": "",
        "genres": [],
        "original_language": None,
        "origin_country": [],
    }



def ensure_media_ids(payload: dict, *, seed: str = "") -> dict:
    """Guarantee usable integer tmdb_id and string imdb_id for DB / Stremio / admin UI.

    - Real provider IDs are kept when present.
    - Missing tmdb_id → negative synthetic id (same pattern as manual/custom titles).
    - Missing imdb_id → ``tg{abs(tmdb_id)}`` so Stremio idPrefixes (tt|tg) still work.
    """
    if not isinstance(payload, dict):
        return payload

    tmdb = payload.get("tmdb_id")
    try:
        if tmdb is not None and str(tmdb).strip().lower() not in ("", "null", "none"):
            tmdb = int(tmdb)
        else:
            tmdb = None
    except (TypeError, ValueError):
        tmdb = None

    imdb = payload.get("imdb_id")
    if imdb is not None:
        imdb = str(imdb).strip()
        if imdb.lower() in ("", "null", "none"):
            imdb = None
        elif imdb.isdigit():
            imdb = f"tt{imdb}"
    else:
        imdb = None

    if tmdb is None:
        # Stable-ish synthetic id from available seeds so re-index merges
        import hashlib
        base = (
            seed
            or imdb
            or str(payload.get("kitsu_id") or "")
            or str(payload.get("tvdb_id") or "")
            or str(payload.get("title") or "")
            or "unknown"
        )
        digest = int(hashlib.md5(base.encode("utf-8")).hexdigest()[:8], 16)
        tmdb = -(digest % 1_000_000_000 + 1)

    if not imdb:
        imdb = f"tg{abs(int(tmdb))}"

    payload["tmdb_id"] = int(tmdb)
    payload["imdb_id"] = imdb
    return payload


def coerce_int_id(value, default=None):
    """Parse query/path tmdb_id that may arrive as 'null' / '' / None."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s or s.lower() in ("null", "none", "undefined"):
        return default
    try:
        return int(float(s))  # tolerate "123.0"
    except (TypeError, ValueError):
        return default


def logo_from_imdb(imdb_id: str | None) -> str:
    """Metahub clearlogo built from an IMDb id (used when providers lack logos)."""
    if not imdb_id:
        return ""
    iid = str(imdb_id).strip()
    if not iid.startswith("tt"):
        iid = f"tt{iid}" if iid.isdigit() else iid
    return f"https://images.metahub.space/logo/medium/{iid}/img"


def normalize_rating(value) -> float:
    """Clamp provider scores into a 0–10 star-style rating.

    TVDB ``score`` is often a popularity rank (hundreds/thousands) — those
    must not surface as 953.4-style ratings.
    """
    try:
        v = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if v <= 0:
        return 0.0
    if v <= 10:
        return round(v, 1)
    if v <= 100:
        # percentage-style (e.g. 82.5 → 8.3)
        return round(v / 10.0, 1)
    # Popularity / rank scores — not a star rating
    return 0.0


def parse_year_range(start=None, end=None) -> tuple:
    """Return (start_year:int|0, end_year:int|None) from provider fields."""
    def _y(val):
        if val is None or val == "":
            return None
        if isinstance(val, int):
            return val if val > 0 else None
        m = re.search(r"(19|20)\d{2}", str(val))
        return int(m.group(0)) if m else None

    s = _y(start)
    e = _y(end)
    if s and e and e < s:
        s, e = e, s
    if s and e and e == s:
        e = None
    return s or 0, e
