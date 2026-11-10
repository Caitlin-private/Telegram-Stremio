"""Release-name understanding shared by parsing, provider matching and Verify Media.

Pure Python on purpose (no PTN / GuessIt / network): everything here is deterministic and
unit-testable.

What it knows about
-------------------
* ``.`` ``_`` ``-`` and spaces are all word separators.
* Leading junk is not title text: ``@Uploader_`` (cut at the next space / ``_`` / ``-``),
  ``[Tag]`` / ``(Tag)``, ``www.site.com -`` and promo lines such as ``MuST Join➥ - ARM KOREAN DRAMA``.
* The release year is the *last* plausible year before the first release tag (resolution, source,
  codec, audio, platform...), so ``Blade Runner 2049 (2017)`` and ``Wonder Woman 1984 2020`` work.
* A number that sits before the year is part of the title (sequel number): ``Awarapan 2 2026``.
* Telegram posts often carry a promo caption while the real release name is the file name;
  :func:`choose_media_title` picks the text that actually looks like a release.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

# ── text normalisation ────────────────────────────────────────────────────────

_PUNCT_MAP = str.maketrans({
    "\u2018": "'", "\u2019": "'", "\u201a": "'", "\u201b": "'", "\u00b4": "'", "`": "'",
    "\u201c": '"', "\u201d": '"',
    "\u2013": "-", "\u2014": "-", "\u2212": "-",
    "\u200b": "", "\u200e": "", "\u200f": "", "\ufeff": "",
})


def normalize_unicode_text(text: str) -> str:
    """NFKC (turns fancy 𝗕𝗼𝗹𝗱 / full-width letters back into ASCII) + typographic punctuation."""
    return unicodedata.normalize("NFKC", text or "").translate(_PUNCT_MAP)


_EXT_RE = re.compile(
    r"(?i)\.(?:mkv|mp4|avi|ts|m4v|mov|wmv|webm|flv|m2ts|mpg|mpeg|zip)(?:\.\d{2,3})?\s*$"
)

# Promo / call-to-action lines Telegram channels put in captions.
_PROMO_ARROW_PREFIX_RE = re.compile(r"^\s*[^\r\n]{1,80}?[➥➜➤→➔⇨⟶»]\s*[-:|]*\s*", re.UNICODE)
_PROMO_WORD_RE = re.compile(
    r"(?i)\b(?:join|joined|subscribe|follow|channel|telegram|t\.me|click\s+here|backup|"
    r"request(?:s|ed)?|group|official|updates?|share|support)\b"
)
_PROMO_ARROW_RE = re.compile(r"[➥➜➤➔⇨⟶]")

_AT_TOKEN_RE = re.compile(r"@[^\s_\-]+")
_LEADING_BRACKET_RE = re.compile(r"^\s*\[[^\]\r\n]{1,48}\]\s*")
_LEADING_SITE_RE = re.compile(
    r"(?i)^\s*(?:www\.[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)*\.[a-z]{2,10}|"
    r"[a-z0-9][a-z0-9-]*\.(?:com|net|org|in|biz|cc|xyz|me|to|tv|vip|lol|ws|pw|co|"
    r"info|live|club|site|app|fun|top|cam|mobi|one|ink|run|art|work|lat|icu|mx|wiki|pro|click|link|"
    r"buzz|cyou|rest|life|world|blog|bz|mov|movie|movies|film|ooo|sx|gs|nu|vc|ag|pm)\b)"
    r"(?:\s*[-–—|:]+\s*|\s+(?=[A-Za-z0-9]))"
)
_LEADING_JUNK_RE = re.compile(r"^[\s._\-~|:]+")


def _has_title_text(value: str) -> bool:
    """True when ``value`` still holds at least two letters besides a file extension."""
    return len(re.findall(r"[A-Za-z]", _EXT_RE.sub("", value or ""))) >= 2


def _strip_leading_at_token(value: str) -> str:
    match = re.match(r"^\s*(@[^\s_\-]+)", value)
    if not match:
        return value
    token = match.group(1)
    rest = value[match.end():]
    rest_stripped = re.sub(r"^[\s_\-]+", "", rest)
    if "." in token:
        # "@Channel.Title.2020.mkv": dots may be the separators. Cut only at the first dot when
        # cutting the whole token would leave nothing to search with.
        head, _, tail = token.partition(".")
        if not _has_title_text(rest_stripped) and len(tail) >= 4:
            return re.sub(r"^[\s_\-]+", "", tail + rest)
    return rest_stripped


def strip_uploader_prefix(name: str) -> str:
    """Remove leading uploader / promo / site / bracket tags (repeatedly) and return the release name."""
    value = normalize_unicode_text(name or "").strip()
    value = _LEADING_JUNK_RE.sub("", value)
    # Common Telegram filename form: "Join_@Uploader_Title..." (without an
    # arrow). Remove only the call-to-action when it directly introduces a
    # channel handle; never strip a legitimate title beginning with "Join".
    value = re.sub(
        r"(?i)^\s*(?:join|subscribe|follow)\s*[_\s-]+(?=@)",
        "",
        value,
        count=1,
    )
    value = _PROMO_ARROW_PREFIX_RE.sub("", value, count=1).strip()
    for _ in range(6):
        before = value
        value = _strip_leading_at_token(value)
        value = _LEADING_BRACKET_RE.sub("", value, count=1)
        value = _LEADING_SITE_RE.sub("", value, count=1)
        value = _LEADING_JUNK_RE.sub("", value)
        if value == before:
            break
    return value.strip()


def strip_channel_tags(text: str) -> str:
    """``@Uploader`` tags anywhere in a name, each cut at the next space / ``_`` / ``-`` (never the whole name)."""
    raw = _LEADING_JUNK_RE.sub("", normalize_unicode_text(text or "").strip())
    had_leading_handle = bool(re.match(r"(?i)^(?:(?:join|subscribe|follow)[_\s-]+)?@", raw))
    out = strip_uploader_prefix(raw)

    def _inline(match: re.Match) -> str:
        token = match.group(0)
        if "." in token:
            head, _, tail = token.partition(".")
            if len(tail) >= 4:
                return " " + tail
        return " "

    out = re.sub(r"(?<![\w.])@[^\s_\-\[\]()]+", _inline, out)
    # Some uploaders concatenate their brand after the handle with an
    # underscore: "@Akatsuki_MediaWonder_Woman". Once the handle is removed,
    # discard that attached "Media" marker only in this handle-prefixed case.
    if had_leading_handle:
        out = re.sub(r"(?i)^\s*Media(?=[A-Z])", "", out, count=1)
    return re.sub(r"\s+", " ", out).strip()


# ── tag vocabulary ────────────────────────────────────────────────────────────

_STRONG_TAGS = {
    "webrip", "webdl", "bluray", "brrip", "bdrip", "bdremux", "hdrip", "hdtv", "dvdrip", "dvdscr", "hdcam",
    "camrip", "hdts", "hdtc", "tsrip", "predvd", "pdvd", "dvd", "remux",
    "x264", "x265", "h264", "h265", "hevc", "avc", "av1", "xvid", "divx", "10bit", "8bit", "10bits",
    "aac", "aac2", "aac5", "ac3", "eac3", "ddp", "ddp2", "ddp5", "ddp7", "dd2", "dd5", "dd7", "dts", "atmos",
    "truehd", "flac", "mp3", "opus",
    "esub", "esubs", "msub", "msubs", "hsub", "hardsub", "hardsubs", "softsub", "softsubs", "subbed",
    "amzn", "nf", "dsnp", "zee5", "hotstar", "jhs", "sonyliv", "sony", "atvp", "hmax", "hulu", "jc", "aha",
    "hdr", "hdr10", "dolby", "vision", "sdr", "imax",
    "proper", "repack", "webhd", "hq", "hqclean", "clean", "predvdrip",
}

_LANGUAGE_WORDS = {
    "hindi", "hin", "tamil", "tam", "telugu", "tel", "malayalam", "mal", "kannada", "kan", "bengali", "bangla",
    "ben", "punjabi", "pun", "marathi", "mar", "gujarati", "guj", "urdu", "english", "eng", "chinese", "chi",
    "korean", "kor", "japanese", "jap", "russian", "spanish", "french", "german", "thai", "turkish",
    "dubbed", "dub", "org", "original", "multi", "dual", "audio", "sub", "subs", "esub", "line",
}

EDITION_WORDS = {
    "uncut", "uncensored", "extended", "unrated", "directors", "director", "director's", "theatrical",
    "remastered", "remaster", "anniversary", "special", "ultimate", "restored", "imax", "edition", "cut",
    "version", "redux", "final", "collectors", "collector's", "complete", "rerelease", "re-release",
}

_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")
_RES_RE = re.compile(r"^(?:\d{3,4}[pi]|[248]k)$")


def _is_strong_tag(low: List[str], i: int) -> bool:
    token = low[i]
    nxt = low[i + 1] if i + 1 < len(low) else ""
    if token in _STRONG_TAGS or _RES_RE.match(token):
        return True
    if token == "web" and nxt in {"dl", "rip", "hd", "dlrip"}:
        return True
    if token == "blu" and nxt == "ray":
        return True
    if token in {"dual", "multi"} and nxt in {"audio", "sub", "subs"}:
        return True
    if token == "h" and nxt in {"264", "265"}:
        return True
    if token in {"dd", "ddp", "dd+", "ddp+"} and re.match(r"^\d", nxt):
        return True
    return False


# ── tokenising ────────────────────────────────────────────────────────────────

def _tokens(text: str) -> List[str]:
    text = re.sub(r"\[[^\]]*\]", " ", text)            # [AMZN Hindi ...], [Judas]
    text = text.replace("&", " & ")
    text = re.sub(r"[._:;,~|/\\!?()\"*+=<>{}]+", " ", text)
    text = re.sub(r"-+", " ", text)
    return [t for t in text.split() if t]


@dataclass
class Identity:
    title: str = ""
    year: Optional[int] = None
    has_tags: bool = False          # a release tag (resolution / source / codec ...) was seen
    editions: List[str] = field(default_factory=list)
    promo: bool = False

    @property
    def usable(self) -> bool:
        return bool(self.title) and bool(re.search(r"[A-Za-z0-9]", self.title))


def _max_year(now_year: Optional[int]) -> int:
    return (now_year or datetime.now().year) + 1


def _clean_title_tokens(tokens: List[str]) -> List[str]:
    tokens = list(tokens)
    # trailing language / subtitle words that precede the year ("Movie Hindi Dubbed 2024")
    while len(tokens) > 1 and tokens[-1].lower() in _LANGUAGE_WORDS:
        tokens.pop()
    while tokens and tokens[0] == "&":
        tokens.pop(0)
    while tokens and tokens[-1] == "&":
        tokens.pop()
    return tokens


def extract_identity(name: str, now_year: Optional[int] = None) -> Identity:
    """Title + release year of a release name / caption, independent of PTN and GuessIt."""
    raw = normalize_unicode_text(name or "")
    ident = Identity(promo=bool(_PROMO_ARROW_RE.search(raw)))
    text = _EXT_RE.sub("", strip_channel_tags(raw))
    if not text:
        return ident
    max_year = _max_year(now_year)

    # 1) "(2007)" / "[2007]" is the unambiguous year marker.
    head, year = None, None
    for match in re.finditer(r"[\(\[]\s*((?:19|20)\d{2})\s*[\)\]]", text):
        if 1900 <= int(match.group(1)) <= max_year and re.search(r"[A-Za-z0-9]", text[:match.start()]):
            head, year = text[:match.start()], int(match.group(1))
            break

    toks_all = _tokens(head if head is not None else text)
    low = [t.lower() for t in toks_all]
    strong_at = next((i for i in range(1, len(low)) if _is_strong_tag(low, i)), None)
    ident.has_tags = strong_at is not None or any(_is_strong_tag(low, i) for i in range(len(low)))

    if head is not None:
        cut = strong_at if strong_at is not None else len(toks_all)
        title_tokens = toks_all[:cut]
    else:
        year_idx = [
            i for i in range(1, len(low))
            if _YEAR_RE.match(low[i]) and 1900 <= int(low[i]) <= max_year and (strong_at is None or i < strong_at)
        ]
        if year_idx:
            cut = year_idx[-1]
            year = int(low[cut])
            title_tokens = toks_all[:cut]
        else:
            title_tokens = toks_all[:strong_at] if strong_at is not None else toks_all
    title_tokens = _clean_title_tokens(title_tokens)
    ident.title = " ".join(title_tokens).strip()
    ident.year = year
    ident.editions = [t.lower() for t in title_tokens if t.lower() in EDITION_WORDS]
    return ident


# ── caption vs file name ──────────────────────────────────────────────────────

def looks_promotional(text: str) -> bool:
    raw = normalize_unicode_text(text or "")
    return bool(_PROMO_ARROW_RE.search(raw)) or bool(_PROMO_WORD_RE.search(strip_uploader_prefix(raw)))


def _evidence(text: Optional[str]) -> tuple:
    """(has_title, year, has_release_tags, promotional) for one candidate text."""
    if not text or not text.strip():
        return False, None, False, False
    ident = extract_identity(text)
    return ident.usable, ident.year, ident.has_tags, looks_promotional(text)


def _release_like(text: Optional[str]) -> bool:
    has_title, year, tags, promo = _evidence(text)
    # Promo captions may contain a real-looking year and resolution after
    # "Join @channel"; that does not make the promo text a release filename.
    return has_title and bool(year or tags) and not promo


def choose_media_title(caption: Optional[str], file_name: Optional[str]) -> Optional[str]:
    """Pick the text the matcher should read.

    The caption wins whenever it looks like a release name (that is the existing behaviour). When it
    is a promo line ("MuST Join➥ - ARM KOREAN DRAMA") or a bare word with no year / quality, and the
    file name is the real release name, the file name is used instead.
    """
    if not (caption and caption.strip()):
        return file_name
    if not (file_name and file_name.strip()):
        return caption
    if _release_like(caption):
        return caption
    if _release_like(file_name):
        return file_name
    return file_name if looks_promotional(caption) else caption


def alternate_media_title(caption: Optional[str], file_name: Optional[str], used: Optional[str]) -> Optional[str]:
    """The other candidate text, when it is a distinct, release-like name worth a second attempt."""
    for candidate in (caption, file_name):
        if candidate and candidate.strip() and candidate != used and _release_like(candidate):
            if extract_identity(candidate).title.lower() != extract_identity(used or "").title.lower():
                return candidate
            return candidate if (extract_identity(candidate).year and not extract_identity(used or "").year) else None
    return None
