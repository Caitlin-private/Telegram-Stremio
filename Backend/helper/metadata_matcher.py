import re
from dataclasses import dataclass, replace
from difflib import SequenceMatcher
from typing import Any

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover - optional runtime acceleration
    fuzz = None


NOISE_WORDS = {
    "amzn", "amazon", "nf", "netflix", "zee5", "hotstar", "jio", "sony", "liv",
    "web", "webdl", "web-dl", "webrip", "dl", "hdrip", "brrip", "bluray", "blu-ray",
    "hdtv", "dvdrip", "remux", "proper", "repack", "extended", "uncut",
    "h264", "h265", "x264", "x265", "hevc", "av1", "10bit", "8bit",
    "ddp", "dd", "aac", "atmos", "dts", "truehd", "5.1", "7.1",
    "1080p", "720p", "2160p", "480p", "4k", "uhd", "hdr", "dv",
    "tamil", "telugu", "malayalam", "hindi", "english", "kannada", "bengali",
    "mkv", "mp4", "avi", "webm", "mov", "flv", "wmv", "m4v",
}

RELEASE_SITE_WORDS = {
    "www", "com", "org", "net", "in", "cc", "cards", "card", "site",
    "1tamilmv", "tamilmv", "tamilmvbiz", "1tamilblasters", "tamilblasters",
    "moviesda", "isaimini", "tamilrockers", "movierulz", "telegram",
}

GENERIC_TITLES = {
    "patriot", "war", "master", "hero", "leo", "animal", "jawan", "king",
    "queen", "love", "life", "ghost", "beast", "vikram", "don", "boss",
}

ROMAN_BY_NUMBER = {
    1: "i", 2: "ii", 3: "iii", 4: "iv", 5: "v",
    6: "vi", 7: "vii", 8: "viii", 9: "ix", 10: "x",
    11: "xi", 12: "xii", 13: "xiii", 14: "xiv", 15: "xv",
    16: "xvi", 17: "xvii", 18: "xviii", 19: "xix", 20: "xx",
}
NUMBER_BY_ROMAN = {roman: number for number, roman in ROMAN_BY_NUMBER.items()}
PART_MARKERS = {"part", "pt", "chapter", "chap", "vol", "volume", "season"}
AUDIO_WORDS = {"ddp", "dd", "aac", "atmos", "dts", "truehd"}


@dataclass
class MatchIntent:
    raw_title: str
    clean_title: str
    year: int | None
    media_type: str
    title_variants: list[str] | None = None
    season: int | None = None
    episode: int | None = None
    season_pack: bool = False
    quality: str | None = None


@dataclass
class MatchCandidate:
    source: str
    title: str
    year: int | None
    media_type: str
    imdb_id: str | None = None
    tmdb_id: int | str | None = None
    popularity: float = 0.0
    raw: Any = None


@dataclass
class MatchDecision:
    accepted: bool
    candidate: MatchCandidate | None
    confidence: float
    reason: str
    candidates: list[dict]
    extra: dict | None = None


# Two provider spellings of the same title ("Machante Maalakha" vs
# "Machante Malakha") must still share external ids: a one-letter difference
# previously left the stored doc without an imdb id, so /meta and /stream
# lookups by the IMDb id returned nothing and the title looked missing
# (2026-10-17 Machante incident).
_FUZZY_IDENTITY_MIN_SIMILARITY = 95.0


def _titles_equivalent(left: str, right: str) -> bool:
    """Near-identical normalized titles (provider spelling variants)."""
    if not left or not right:
        return False
    if abs(len(left) - len(right)) > 3:
        return False
    if len(left.split()) != len(right.split()):
        return False
    return title_similarity(left, right) >= _FUZZY_IDENTITY_MIN_SIMILARITY


def enrich_candidate_identity(
    selected: MatchCandidate | None,
    candidates: list[MatchCandidate],
) -> MatchCandidate | None:
    """Fill missing external IDs from equivalent provider candidates."""
    if selected is None or (selected.imdb_id and selected.tmdb_id):
        return selected

    selected_title = normalize_title(selected.title)
    equivalents: list[MatchCandidate] = []

    for candidate in candidates:
        if candidate is selected or candidate.media_type != selected.media_type:
            continue
        candidate_title = normalize_title(candidate.title)
        exact_match = candidate_title == selected_title
        if not exact_match:
            # Fuzzy merge is delimited hard: both sides must carry a year and
            # neither title may be generic — a missing-year near-duplicate
            # must never donate its ids (collision-prone titles).
            if not (selected.year and candidate.year):
                continue
            if is_generic_title(selected_title) or is_generic_title(candidate_title):
                continue
            if not _titles_equivalent(selected_title, candidate_title):
                continue
        if selected.year and candidate.year and abs(int(selected.year) - int(candidate.year)) > 1:
            continue
        if selected.year and not candidate.year and selected_title in GENERIC_TITLES:
            continue
        equivalents.append(candidate)

    imdb_ids = {candidate.imdb_id for candidate in equivalents if candidate.imdb_id}
    tmdb_ids = {candidate.tmdb_id for candidate in equivalents if candidate.tmdb_id}
    imdb_id = selected.imdb_id or (next(iter(imdb_ids)) if len(imdb_ids) == 1 else None)
    tmdb_id = selected.tmdb_id or (next(iter(tmdb_ids)) if len(tmdb_ids) == 1 else None)

    if imdb_id == selected.imdb_id and tmdb_id == selected.tmdb_id:
        return selected
    return replace(selected, imdb_id=imdb_id, tmdb_id=tmdb_id)


def normalize_title(value: str | None) -> str:
    tokens = _tokenize_title(value)
    words = []
    for index, word in enumerate(tokens):
        if word in NOISE_WORDS:
            continue
        if _is_year_token(word) or _is_episode_token(word):
            continue
        if re.fullmatch(r"\d{3,4}p?", word):
            continue
        if word.isdigit():
            number = int(word)
            if not (1 <= number <= 20) or not _keep_title_number(tokens, index):
                continue
        elif len(word) == 1 and not _roman_to_int(word):
            continue
        words.append(word)
    return " ".join(words).strip()


def _tokenize_title(value: str | None) -> list[str]:
    text = (value or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[\._\-+\[\]\(\)\{\}:;,/\\|]+", " ", text)
    return re.findall(r"[a-z0-9]+(?:'[a-z0-9]+)?", text)


def _is_year_token(token: str) -> bool:
    return bool(re.fullmatch(r"(?:19|20)\d{2}", token or ""))


def _is_episode_token(token: str) -> bool:
    return bool(re.fullmatch(r"s\d{1,2}(?:e\d{1,2})?", token or ""))


def _roman_to_int(token: str | None) -> int | None:
    return NUMBER_BY_ROMAN.get((token or "").lower())


def _title_number(token: str | None) -> int | None:
    token = (token or "").lower()
    if token.isdigit() and 1 <= int(token) <= 20:
        return int(token)
    return _roman_to_int(token)


def _has_content_before(tokens: list[str], index: int) -> bool:
    for token in tokens[:index]:
        if (
            token not in NOISE_WORDS
            and token not in RELEASE_SITE_WORDS
            and token not in PART_MARKERS
            and not _is_year_token(token)
            and not _is_episode_token(token)
        ):
            return True
    return False


def _keep_title_number(tokens: list[str], index: int) -> bool:
    token = tokens[index]
    previous = tokens[index - 1] if index > 0 else ""
    next_token = tokens[index + 1] if index + 1 < len(tokens) else ""
    if previous in AUDIO_WORDS or next_token in AUDIO_WORDS:
        return False
    if previous in PART_MARKERS:
        return _has_content_before(tokens, index - 1)
    if next_token and next_token.isdigit() and previous in AUDIO_WORDS:
        return False
    if _is_year_token(next_token):
        return _has_content_before(tokens, index)
    if index == len(tokens) - 1:
        return _has_content_before(tokens, index)
    return False


def _drop_release_site_prefix(tokens: list[str]) -> list[str]:
    out = list(tokens)
    while out and (out[0] in RELEASE_SITE_WORDS or re.fullmatch(r"\d*tamilmv\d*", out[0] or "")):
        out.pop(0)
    return out


def _append_unique(items: list[str], value: str | None) -> None:
    value = (value or "").strip()
    if value and value not in items:
        items.append(value)


def _add_number_equivalent_variants(variants: list[str], normalized: str | None) -> None:
    tokens = (normalized or "").split()
    for index, token in enumerate(tokens):
        number = _title_number(token)
        if not number:
            continue
        roman = ROMAN_BY_NUMBER[number]
        forms = [str(number), roman]
        previous = tokens[index - 1] if index > 0 else ""
        for form in forms:
            replaced = list(tokens)
            replaced[index] = form
            _append_unique(variants, " ".join(replaced))
            if previous in PART_MARKERS and index >= 2:
                without_marker = list(tokens)
                without_marker[index] = form
                del without_marker[index - 1]
                _append_unique(variants, " ".join(without_marker))
            elif previous not in PART_MARKERS and index > 0:
                with_part = list(tokens)
                with_part[index:index] = ["part"]
                with_part[index + 1] = form
                _append_unique(variants, " ".join(with_part))


def _canonical_numbered_title(value: str | None) -> str:
    tokens = normalize_title(value).split()
    canonical = []
    for token in tokens:
        if token in PART_MARKERS:
            continue
        number = _title_number(token)
        canonical.append(str(number) if number else token)
    return " ".join(canonical)


def _number_signature(value: str | None) -> tuple[int, ...]:
    signature = []
    for token in normalize_title(value).split():
        number = _title_number(token)
        if number:
            signature.append(number)
    return tuple(signature)


def _variant_from_tokens(tokens: list[str]) -> str:
    return normalize_title(" ".join(_drop_release_site_prefix(tokens)))


def build_title_variants(raw_title: str | None, parsed_title: str | None = None, year: int | None = None, site: str | None = None) -> list[str]:
    variants: list[str] = []

    def add(value: str | None) -> None:
        normalized = normalize_title(value)
        _append_unique(variants, normalized)
        _add_number_equivalent_variants(variants, normalized)

    def add_tokens(tokens: list[str]) -> None:
        normalized = _variant_from_tokens(tokens)
        _append_unique(variants, normalized)
        _add_number_equivalent_variants(variants, normalized)

    if parsed_title:
        add_tokens(_tokenize_title(parsed_title))
        add(parsed_title)

    raw_tokens = _tokenize_title(raw_title)
    if raw_tokens:
        year_index = None
        for idx, token in enumerate(raw_tokens):
            if token == str(year) or re.fullmatch(r"(?:19|20)\d{2}", token):
                year_index = idx
                break
        if year_index is not None:
            before_year = raw_tokens[:year_index]
            add_tokens(before_year)
            for start in range(len(before_year)):
                add_tokens(before_year[start:])

        add_tokens(raw_tokens)
        if site:
            site_tokens = set(_tokenize_title(site))
            add_tokens([token for token in raw_tokens if token not in site_tokens])

    return variants


def title_similarity(left: str | None, right: str | None) -> float:
    left_norm = normalize_title(left)
    right_norm = normalize_title(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 100.0
    if _canonical_numbered_title(left_norm) == _canonical_numbered_title(right_norm):
        return 100.0
    if fuzz:
        if len(left_norm.split()) <= 1 or len(right_norm.split()) <= 1:
            return float(max(
                fuzz.ratio(left_norm, right_norm),
                fuzz.token_sort_ratio(left_norm, right_norm),
            ))
        return float(max(
            fuzz.ratio(left_norm, right_norm),
            fuzz.token_sort_ratio(left_norm, right_norm),
            fuzz.token_set_ratio(left_norm, right_norm),
        ))
    return SequenceMatcher(None, left_norm, right_norm).ratio() * 100.0


def is_generic_title(title: str | None) -> bool:
    normalized = normalize_title(title)
    return normalized in GENERIC_TITLES or len(normalized.split()) <= 1


def _year_score(intent_year: int | None, candidate_year: int | None) -> tuple[float, str | None]:
    if not intent_year:
        return 0.0, None
    if not candidate_year:
        return -12.0, "metadata_year_missing"
    delta = abs(int(intent_year) - int(candidate_year))
    if delta == 0:
        return 18.0, None
    if delta == 1:
        return 8.0, None
    return -80.0, "metadata_year_mismatch"


def _candidate_public(candidate: MatchCandidate, score: float, title_score: float, reason: str | None) -> dict:
    return {
        "source": candidate.source,
        "title": candidate.title,
        "year": candidate.year,
        "media_type": candidate.media_type,
        "imdb_id": candidate.imdb_id,
        "tmdb_id": candidate.tmdb_id,
        "score": round(score, 2),
        "title_score": round(title_score, 2),
        "reason": reason,
    }


def _same_identity(left: MatchCandidate, right: MatchCandidate) -> bool:
    if left.imdb_id and right.imdb_id and left.imdb_id == right.imdb_id:
        return True
    if left.tmdb_id and right.tmdb_id and str(left.tmdb_id) == str(right.tmdb_id):
        return True
    return (
        left.media_type == right.media_type
        and normalize_title(left.title) == normalize_title(right.title)
        and left.year == right.year
    )


def choose_best_candidate(intent: MatchIntent, candidates: list[MatchCandidate]) -> MatchDecision:
    scored: list[tuple[float, float, str | None, MatchCandidate]] = []
    rejected_reason = "metadata_no_candidates"

    for candidate in candidates:
        reason = None
        if candidate.media_type != intent.media_type:
            reason = "metadata_media_type_mismatch"
        variants = intent.title_variants or [intent.clean_title or intent.raw_title]
        best_variant = max(variants, key=lambda variant: title_similarity(variant, candidate.title))
        title_score = title_similarity(best_variant, candidate.title)
        intent_numbers = _number_signature(best_variant)
        candidate_numbers = _number_signature(candidate.title)
        year_bonus, year_reason = _year_score(intent.year, candidate.year)
        if year_reason:
            reason = year_reason
        if intent_numbers and candidate_numbers and intent_numbers != candidate_numbers:
            reason = "metadata_sequel_mismatch"

        score = title_score + year_bonus + min(float(candidate.popularity or 0.0), 20.0) / 20.0
        if intent_numbers and candidate_numbers and intent_numbers == candidate_numbers:
            score += 8.0
        elif intent_numbers and not candidate_numbers:
            score -= 8.0
        if reason:
            score -= 100.0
        scored.append((score, title_score, reason, candidate))

    scored.sort(key=lambda item: item[0], reverse=True)
    public = [_candidate_public(candidate, score, title_score, reason) for score, title_score, reason, candidate in scored[:8]]

    if not scored:
        return MatchDecision(False, None, 0.0, rejected_reason, public)

    top_score, top_title_score, top_reason, top = scored[0]
    second_score = -999.0
    for score, _, _, candidate in scored[1:]:
        if not _same_identity(top, candidate):
            second_score = score
            break
    margin = top_score - second_score
    generic = is_generic_title(intent.clean_title)

    if top.media_type != intent.media_type:
        return MatchDecision(False, top, top_score, "metadata_media_type_mismatch", public)

    min_title = 92.0 if generic else 86.0
    min_margin = 8.0 if intent.year else 12.0
    if generic:
        min_margin = 12.0

    weak_reason = top_reason
    if not weak_reason and intent.year and top.year and abs(int(intent.year) - int(top.year)) > 1:
        weak_reason = "metadata_year_mismatch"
    if not weak_reason and intent.year and not top.year:
        weak_reason = "metadata_year_missing"
    if not weak_reason and generic and not intent.year:
        weak_reason = "metadata_generic_title_needs_year"
    if not weak_reason and top_title_score < min_title:
        weak_reason = "metadata_title_mismatch"
    if not weak_reason and second_score > -999.0 and margin < min_margin:
        weak_reason = "metadata_ambiguous_match"

    if weak_reason:
        return MatchDecision(True, top, top_score, f"accepted_low_confidence:{weak_reason}", public)

    return MatchDecision(True, top, top_score, "accepted", public)


def decision_metadata(decision: MatchDecision, intent: MatchIntent) -> dict:
    data = {
        "parsed_title": intent.clean_title,
        "parsed_year": intent.year,
        "parsed_media_type": intent.media_type,
        "search_variants": intent.title_variants or [intent.clean_title],
        "match_confidence": round(decision.confidence, 2),
        "match_rejection_reason": None if decision.accepted else decision.reason,
        "match_reason": decision.reason,
        "match_candidates": decision.candidates,
        "auto_matched": bool(decision.candidate),
    }
    if decision.extra:
        data.update(decision.extra)
    return data
