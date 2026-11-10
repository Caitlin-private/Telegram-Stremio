"""Tag-aware duplicate detection for "Advanced File filtering" (beta).

Two files at the same resolution are only worth keeping side by side when they
really differ: a different cut (IMAX, uncut, ...), a different source (WEB-DL
vs BluRay), codec (x264 / x265 / AV1), bit depth, dynamic range (SDR / HDR /
Dolby Vision) or a premium audio track. Name and size alone cannot tell
"Movie.1080p.BluRay.x264" from a re-upload of itself, tags can.

How two files are compared
--------------------------
* **Edition** (imax, uncut, remastered, ...) must match exactly. A theatrical
  file and an uncut file are different content, so an untagged file is never a
  copy of an "uncut" one (and "theatrical" counts as untagged).
* **Hard-coded subtitles** count as part of the edition: a hardsubbed file and a
  clean one are never copies of each other.
* **Technical tags** (source, codec, bit depth, HDR, premium audio formats,
  audio languages, subtitles) are compared as sets: the incoming file is a copy
  only when every tag it names is already on the existing file. A tag the
  incoming file does not mention adds nothing new; a tag the existing file lacks
  (a second language, 10-bit, an ESub track...) is new information, so the file
  is kept.

So with ``Movie 1080p uncut x264`` stored: ``uncut x265`` is kept (new codec),
another ``uncut x264`` is a copy, ``x264`` alone is kept (different cut) and
plain ``uncut`` is a copy. With ``Movie 1080p Dual Audio Hindi English`` stored,
a plain ``Hindi`` file is a copy but ``Hindi English Tamil`` is kept.
To teach it a new tag, add a line to ``_TAGS``.
"""
from __future__ import annotations

import re
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

Signature = Dict[str, FrozenSet[str]]

EDITION = "edition"
# Order is only for readability; every category except EDITION is a technical one.
TECHNICAL = ("source", "codec", "bitdepth", "hdr", "audio", "language", "subs")
CATEGORIES = (EDITION,) + TECHNICAL

_SEP = r"[ ._-]?"
# tag -> alternation of the words that mean it
_LANGUAGES = {
    "hindi": "hindi", "tamil": "tamil", "telugu": "telugu", "malayalam": "malayalam",
    "kannada": "kannada", "bengali": "bengali|bangla", "punjabi": "punjabi", "marathi": "marathi",
    "gujarati": "gujarati", "urdu": "urdu", "english": "english", "korean": "korean",
    "japanese": "japanese", "chinese": "chinese|mandarin|cantonese", "spanish": "spanish|latino|castellano",
    "french": "french|vff|vfq", "german": "german|deutsch", "italian": "italian",
    "russian": "russian", "portuguese": "portuguese", "turkish": "turkish", "thai": "thai",
    "arabic": "arabic", "indonesian": "indonesian", "vietnamese": "vietnamese",
}
# (category, tag, regex).  Regexes run on the lower-cased name with "_" read as a
# space; lookarounds keep "x264" out of "x2640" and "dv" out of "advance".
_TAGS: List[Tuple[str, str, str]] = [
    # ── edition: genuinely different cuts / masters ─────────────────────────
    (EDITION, "imax", rf"imax(?:{_SEP}enhanced)?"),
    (EDITION, "uncut", r"uncut"),
    (EDITION, "unrated", r"unrated"),
    (EDITION, "extended", rf"extended(?:{_SEP}(?:cut|edition|version))?"),
    (EDITION, "directors", rf"director'?s{_SEP}(?:cut|edition|version)"),
    (EDITION, "remastered", rf"(?:4k{_SEP})?(?:remaster(?:ed)?|restored)"),
    (EDITION, "special", rf"(?:special|ultimate|collector'?s|anniversary){_SEP}edition"),
    (EDITION, "criterion", r"criterion"),
    (EDITION, "final_cut", rf"final{_SEP}cut"),
    (EDITION, "open_matte", rf"open{_SEP}matte"),
    (EDITION, "3d", rf"3d|(?:h|half){_SEP}sbs|sbs"),
    (EDITION, "colorized", r"colou?rized"),
    (EDITION, "hardsub", rf"hard{_SEP}sub(?:s|bed|titles?)?|hardcoded|hard{_SEP}coded"),
    # ── source ──────────────────────────────────────────────────────────────
    ("source", "remux", rf"(?:(?:bd|uhd|blu{_SEP}ray){_SEP})?remux"),
    ("source", "bluray", rf"blu{_SEP}ray|bd{_SEP}rip|br{_SEP}rip|bd"),
    ("source", "webdl", rf"web{_SEP}dl|web"),
    ("source", "webrip", rf"web{_SEP}rip"),
    ("source", "hdrip", rf"hd{_SEP}rip"),
    ("source", "hdtv", r"hdtv|pdtv"),
    ("source", "dvd", rf"dvd{_SEP}(?:rip|scr)|dvd"),
    ("source", "cam", rf"hd{_SEP}cam|cam{_SEP}rip|cam|hdts|ts|tc"),
    # ── codec ───────────────────────────────────────────────────────────────
    ("codec", "hevc", rf"x{_SEP}265|h{_SEP}265|hevc"),
    ("codec", "avc", rf"x{_SEP}264|h{_SEP}264|avc"),
    ("codec", "av1", r"av1"),
    ("codec", "vp9", r"vp9"),
    ("codec", "xvid", r"xvid|divx"),
    # ── bit depth ───────────────────────────────────────────────────────────
    ("bitdepth", "10bit", rf"10{_SEP}bits?|hi10p?"),
    ("bitdepth", "8bit", rf"8{_SEP}bits?"),
    ("bitdepth", "12bit", rf"12{_SEP}bits?"),
    # ── dynamic range ───────────────────────────────────────────────────────
    ("hdr", "dv", rf"dolby{_SEP}vision|dovi|dv"),
    ("hdr", "hdr10plus", rf"hdr{_SEP}10{_SEP}(?:\+|plus)"),
    ("hdr", "hdr", rf"hdr(?:{_SEP}10)?"),
    ("hdr", "hlg", r"hlg"),
    ("hdr", "sdr", r"sdr"),
    # ── premium audio (ordinary AAC / AC3 / DD / DDP are deliberately ignored) ─
    ("audio", "atmos", r"atmos"),
    ("audio", "truehd", rf"true{_SEP}hd"),
    ("audio", "dtshd", rf"dts{_SEP}hd(?:{_SEP}ma)?"),
    ("audio", "dtsx", rf"dts{_SEP}x"),
    # ── audio languages ("English Subtitles" is a subtitle tag, not an audio language) ──
    ("language", "dual", rf"dual(?:{_SEP}audio)?"),
    ("language", "multi", rf"multi(?:{_SEP}(?:audio|lang(?:uage)?s?))?(?!{_SEP}sub)"),
    ("language", "dubbed", r"dubbed|dub"),
] + [
    ("language", tag, rf"(?:{words})(?!{_SEP}sub)")
    for tag, words in _LANGUAGES.items()
] + [
    # ── subtitles (the generic "sub" is covered by any specific subtitle tag) ──
    ("subs", "esub", rf"e{_SEP}subs?|eng(?:lish)?{_SEP}sub(?:s|titles?)?"),
    ("subs", "msub", rf"m{_SEP}subs?|multi{_SEP}sub(?:s|titles?)?"),
    ("subs", "softsub", rf"soft{_SEP}sub(?:s|titles?)?"),
    ("subs", "sub", r"subs?|subbed|subtitles?|vostfr|vost"),
]

_COMPILED: List[Tuple[str, str, "re.Pattern[str]"]] = [
    (cat, tag, re.compile(rf"(?<![a-z0-9])(?:{pattern})(?![a-z0-9])"))
    for cat, tag, pattern in _TAGS
]

# Tags that mean nothing alone or conflict with a stronger tag in the same category.
_SUPERSEDED = {
    "source": [("remux", {"bluray"}), ("webrip", {"webdl"})],
    "hdr": [("hdr10plus", {"hdr"})],
    "subs": [("esub", {"sub"}), ("msub", {"sub"}), ("softsub", {"sub"})],
}
# "ts" / "tc" / "bd" are only trusted as source tags in a name that also looks like a release.
_RELEASE_HINT = re.compile(r"(?<![a-z0-9])(?:\d{3,4}p|[xh]\.?26[45]|hevc|av1|web|blu|dvd|hdtv|rip)(?![a-z0-9])")


def _normalise(name: str) -> str:
    return re.sub(r"_+", " ", (name or "").lower())


def signature(name: str) -> Signature:
    """Tags found in a file name, grouped by category."""
    text = _normalise(name)
    found: Dict[str, set] = {c: set() for c in CATEGORIES}
    for cat, tag, rx in _COMPILED:
        match = rx.search(text)
        if not match:
            continue
        if tag == "cam" and match.group(0) in ("ts", "tc") and not _RELEASE_HINT.search(text):
            continue
        found[cat].add(tag)
    for cat, rules in _SUPERSEDED.items():
        for strong, weak in rules:
            if strong in found[cat]:
                found[cat] -= weak
    return {c: frozenset(v) for c, v in found.items()}


def is_redundant(incoming: Signature, existing: Signature) -> bool:
    """True when ``incoming`` brings nothing new compared with ``existing``."""
    if incoming[EDITION] != existing[EDITION]:
        return False
    for c in TECHNICAL:
        tags = incoming[c]
        if c == "subs" and tags == {"sub"} and existing[c]:
            continue  # a bare "sub" is covered by any specific subtitle tag
        if not tags <= existing[c]:
            return False
    return True


def find_redundant(incoming_name: str, existing_names: Iterable[str]) -> Optional[str]:
    """Name of the first existing file the incoming one merely duplicates, else None."""
    sig = signature(incoming_name)
    for existing in existing_names:
        if is_redundant(sig, signature(existing)):
            return existing
    return None


def describe(sig: Signature) -> str:
    parts = [t for c in CATEGORIES for t in sorted(sig[c])]
    return " ".join(parts) or "untagged"


# ── "forwarded through the main bot" bypass ─────────────────────────────────
_BYPASS: ContextVar[bool] = ContextVar("advanced_file_filter_bypass", default=False)


@contextmanager
def bypass_advanced_filter():
    """Files sent straight to the main bot are deliberate: never filter them."""
    token = _BYPASS.set(True)
    try:
        yield
    finally:
        _BYPASS.reset(token)


def bypassed() -> bool:
    return _BYPASS.get()
