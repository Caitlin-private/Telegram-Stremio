"""Verify indexed media against the *current* metadata matcher.

Every indexed file is re-read from Telegram (spread across all worker bots),
run through the same ``metadata()`` pipeline a normal scan uses, and compared
with the title it is currently filed under:

* same title            -> untouched
* different title       -> re-indexed under the correct title, old entry removed
* no confident match   -> copied to Skip; source deletion follows the Skip
                           Channel setting unless a batch-wide provider outage is suspected
* missing / unreadable  -> reported as dead (never modified here) or as an
                           error; inconclusive results never change anything

Database changes are applied one at a time under the shared live-ingestion
lock; the slow Telegram + metadata work runs concurrently in batches.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, AsyncIterator, Dict, List, Optional

from wzgram.errors import FloodWait

from Backend.config import _int_env
from Backend.helper.checker_requests import CHECK_TIMEOUT, CheckUnavailable, check_reference, get_check_message
from Backend.helper.encrypt import decode_string, encode_string
from Backend.helper.metadata import caption_with_links, extract_default_id, metadata
from Backend.helper.metadata_matcher import _number_signature
from Backend.helper.metadata.common import collect_title_aliases, score_candidate
from Backend.helper.metadata.name_identity import alternate_media_title, choose_media_title, extract_identity
from Backend.helper.pyro import (
    clean_filename,
    finalize_media_name,
    get_readable_file_size,
    is_media,
    message_has_thumb,
)
from Backend.helper.resolution_policy import rejection_reason, source_resolution
from Backend.helper.skip_channel import is_skip_channel, route_to_skip_channel
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

# Same shape as quick scan: 200 messages per Telegram call, one lane per worker bot,
# a bounded metadata budget (QUICK_SCAN_CONCURRENCY, default 16) shared by all lanes,
# 20 ms fetch pacing that backs off on FloodWait, and a pause while bot streams play.
VERIFY_BATCH_SIZE = 200
VERIFY_BUFFER = 6000          # units held while grouping them by channel
VERIFY_PAGE_SIZE = 100
VERIFY_MIN_DELAY = 0.02
VERIFY_MAX_DELAY = 2.0
VERIFY_FLOOD_RETRIES = 5
VERIFY_METADATA_TIMEOUT = 60   # a hung provider call must never pin a metadata slot
SPEED_WINDOW = 30.0
VERIFY_APPLY_CONCURRENCY = 8
_ACCESS_ERRORS = {"ChannelPrivate", "PeerIdInvalid", "ChannelInvalid", "ChatAdminRequired", "UserNotParticipant"}
# A batch in which (almost) nothing matches looks like a metadata-provider
# outage, not 20+ genuinely unidentifiable files. Never delete on that signal.
OUTAGE_MIN_BATCH = 10
OUTAGE_RATIO = 0.9
OUTAGE_ABORT_BATCHES = 3
MAX_REPORTED_CHANGES = 300

_EMPTY = (None, "", 0)

# Verify-only confidence threshold. The upstream metadata matcher remains
# untouched; this helper is used solely to decide whether an existing library
# filing is safe to retain/reassign during Verify Media.
_VERIFY_MOVIE_MATCH_THRESHOLD = 0.65


def _verify_score_candidate_aliases(
    query_title: str,
    query_year,
    primary_title: str,
    result_year,
    aliases=None,
    year_reliable: bool = True,
    year_lower_bound: bool = False,
) -> float:
    query_numbers = _number_signature(query_title)
    primary_numbers = _number_signature(primary_title)
    # Reuse the canonical metadata matcher's installment parser. An explicit
    # Part I/II (or 1/2) conflict must not be rescued by fuzzy alias scoring.
    if query_numbers and primary_numbers and query_numbers != primary_numbers:
        return 0.0

    best = 0.0
    for candidate in collect_title_aliases(primary_title, aliases):
        candidate_numbers = _number_signature(candidate)
        if query_numbers and candidate_numbers and query_numbers != candidate_numbers:
            continue
        score = score_candidate(
            query_title, query_year, candidate, result_year or 0,
            year_reliable=year_reliable,
            year_lower_bound=year_lower_bound,
        )
        best = max(best, score)
    return best


def blank_verify_state() -> Dict[str, int]:
    return {
        "verified": 0,        # files compared with the matcher
        "matched": 0,         # already under the right title
        "mismatched": 0,      # filed under the wrong title/episode
        "reassigned": 0,      # ...and moved to the correct one
        "unmatched": 0,       # no confident match; routed to the configured Skip channel
        "duplicates": 0,      # correct title already had this quality; wrong entry dropped
        "skipped": 0,         # not verifiable (skip-channel file, non-video, ...)
        "held": 0,            # suspected provider outage; deliberately left untouched
        "moved_to_skip": 0,   # low-confidence files copied to Skip and removed from source
        "kept": 0,            # provider found nothing, but the current filing agrees with the file name
    }


def _is_set(value) -> bool:
    return value not in _EMPTY


def same_title(doc: dict, media_type: str, info: dict) -> bool:
    """Mirror the identity rules used when a file is first filed (imdb / tmdb / kitsu / title+year)."""
    if collection_for(media_type) != collection_for(info.get("media_type")):
        return False
    for key in ("imdb_id", "tmdb_id", "kitsu_id"):
        a, b = doc.get(key), info.get(key)
        if _is_set(a) and _is_set(b) and str(a) == str(b):
            return True
    title, year = doc.get("title"), doc.get("release_year")
    if title and year and not str(doc.get("imdb_id") or "").startswith("tgauto"):
        try:
            return title == info.get("title") and int(year) == int(str(info.get("year"))[:4])
        except (TypeError, ValueError):
            return False
    return False


def collection_for(media_type) -> str:
    return "tv" if str(media_type).lower() in ("tv", "series") else "movie"


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def same_slot(unit: dict, info: dict) -> bool:
    """TV only: the file must also sit under the season/episode the matcher now derives."""
    if unit["media_type"] != "tv":
        return True
    return (_int_or_none(info.get("season_number")) == unit["season"]
            and _int_or_none(info.get("episode_number")) == unit["episode"])


# ── Walking the library ──────────────────────────────────────────────────────

def _doc_units(db_index: int, media_type: str, doc: dict) -> List[dict]:
    ident = {k: doc.get(k) for k in ("tmdb_id", "imdb_id", "kitsu_id", "title", "release_year")}
    base = {"db_index": db_index, "media_type": media_type, "doc_id": doc["_id"], "doc": ident}
    units = []
    if media_type == "movie":
        for q in doc.get("telegram") or []:
            if q.get("id"):
                units.append({**base, "season": None, "episode": None, "quality": q})
    else:
        for season in doc.get("seasons") or []:
            for episode in season.get("episodes") or []:
                for q in episode.get("telegram") or []:
                    if q.get("id"):
                        units.append({**base, "season": _int_or_none(season.get("season_number")),
                                      "episode": _int_or_none(episode.get("episode_number")), "quality": q})
    return units


async def iter_units(db) -> AsyncIterator[dict]:
    for i in range(1, db.current_db_index + 1):
        storage = db.dbs.get(f"storage_{i}")
        if storage is None:
            continue
        for media_type in ("movie", "tv"):
            last_id = None
            while True:
                query = {"_id": {"$gt": last_id}} if last_id is not None else {}
                docs = await storage[media_type].find(query).sort("_id", 1) \
                    .limit(VERIFY_PAGE_SIZE).to_list(length=VERIFY_PAGE_SIZE)
                if not docs:
                    break
                for doc in docs:
                    last_id = doc["_id"]
                    # Hand-entered titles use curated metadata and manual
                    # tagging; provider verification must leave them untouched.
                    if doc.get("manual_added") or str(doc.get("imdb_id") or "").startswith("tg"):
                        continue
                    for unit in _doc_units(i, media_type, doc):
                        yield unit


async def unit_sources(quality: dict) -> List[tuple]:
    """(chat_id, msg_id) of every Telegram message backing one stored quality, part order."""
    parts = quality.get("parts")
    if not parts:
        decoded = await decode_string(quality["id"])
        if not isinstance(decoded, dict):
            raise ValueError("invalid decoded stream")
        parts = decoded.get("parts") or [decoded]
    ordered = sorted(parts, key=lambda p: p.get("part_number", 0) or 0)
    return [(p.get("chat_id"), p.get("msg_id")) for p in ordered]


def _label(doc: dict, season=None, episode=None) -> str:
    title = doc.get("title") or "Unknown"
    if season is not None and episode is not None:
        return f"{title} S{season:02d}E{episode:02d}"
    return title


class MediaVerifier:
    def __init__(self, manager, client, dry_run: bool = False, check_episodes: bool = True,
                 pause_for_playback: bool = True) -> None:
        self.m = manager
        self.db = manager._db
        self.client = client
        self.dry_run = dry_run
        self.check_episodes = check_episodes
        self.pause_for_playback = pause_for_playback
        self._meta_sem = asyncio.Semaphore(max(1, min(64, _int_env("QUICK_SCAN_CONCURRENCY", 16))))
        self._apply_sem = asyncio.Semaphore(VERIFY_APPLY_CONCURRENCY)
        self._routers: Dict[int, List[Any]] = {}
        self._router_lock = asyncio.Lock()
        self._router_turn = 0
        self._delays: Dict[str, float] = {}
        self._cooldowns: Dict[str, float] = {}
        self._no_access: set = set()
        self._catalog: Dict[str, dict] = {}
        self._suspect_streak = 0
        self._samples: deque = deque([(time.monotonic(), 0)])
        self._abort: Optional[Exception] = None
        self._stopped = asyncio.Event()
        self._priority = None

    # ── helpers ─────────────────────────────────────────────────────────────
    @property
    def state(self) -> dict:
        return self.m.state

    @property
    def counters(self) -> Dict[str, int]:
        return self.m.state["verify"]

    def _note(self, action: str, unit: dict, to: str = "", name: str = "", api: str = "Unknown") -> None:
        if not self.dry_run:
            from Backend.helper import log_channel
            log_channel.log_verify_change(
                action, name or unit["quality"].get("name") or "",
                _label(unit["doc"], unit["season"], unit["episode"]), to, api=api)
        changes = self.state["changes"]
        if len(changes) < MAX_REPORTED_CHANGES:
            changes.append({
                "action": action,
                "name": name or unit["quality"].get("name") or "",
                "from": _label(unit["doc"], unit["season"], unit["episode"]),
                "to": to,
            })

    def _workers(self) -> List[Any]:
        """Every connected worker bot; the main bot stays free for live ingestion when others exist."""
        from Backend.pyrofork.bot import multi_clients
        found: List[Any] = []
        for key, c in sorted(multi_clients.items()):
            if key < 0 or c is None or getattr(c, "is_connected", True) is False:
                continue
            if not any(c is f for f in found):
                found.append(c)
        others = [c for c in found if c is not self.client]
        return others or found or [self.client]

    @staticmethod
    def _key(client) -> str:
        return str(getattr(getattr(client, "me", None), "id", None) or getattr(client, "name", None) or id(client))

    async def _wait_playback(self) -> None:
        if self._priority is not None:
            await self._priority.wait()

    async def _plain_call(self, operation, *args, scan_client=None, **kwargs):
        for attempt in range(4):
            await self._wait_playback()
            try:
                return await operation(*args, **kwargs)
            except FloodWait as exc:
                if attempt == 3:
                    raise
                await asyncio.sleep(min(int(exc.value) + 1, 300))

    async def _eligible_routers(self, chat_id) -> List[Any]:
        """Bots that may delete in the source channel and post to the skip channel."""
        key = int(str(chat_id).replace("-100", ""))
        async with self._router_lock:
            if key not in self._routers:
                from Backend.helper.scan_resources import select_scan_clients
                try:
                    self._routers[key] = await select_scan_clients(
                        self.client, int(f"-100{key}"), 0, False, self._plain_call)
                except Exception as exc:
                    LOGGER.warning(f"[Verify] No eligible bot for channel {key}: {exc}")
                    self._routers[key] = []
            return self._routers[key]

    # ── Telegram reads (batched, paced, FloodWait-aware per bot) ────────────
    async def _get_batch(self, client, chat_id: int, ids: List[int]) -> Optional[list]:
        key, chat = self._key(client), int(f"-100{chat_id}")
        for _ in range(VERIFY_FLOOD_RETRIES + 1):
            await self._wait_playback()
            wait = self._cooldowns.get(key, 0) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            await asyncio.sleep(self._delays.get(key, VERIFY_MIN_DELAY))
            try:
                async with asyncio.timeout(CHECK_TIMEOUT):
                    res = await client.get_messages(chat, ids)
            except FloodWait as exc:
                self._cooldowns[key] = time.monotonic() + max(1, int(exc.value)) + 1
                self._delays[key] = min(VERIFY_MAX_DELAY, max(0.1, self._delays.get(key, VERIFY_MIN_DELAY) * 2))
                LOGGER.warning(f"[Verify] FloodWait {exc.value}s on bot {key}; retrying this batch")
                continue
            self._delays[key] = max(VERIFY_MIN_DELAY, self._delays.get(key, VERIFY_MIN_DELAY) * 0.9)
            return res if isinstance(res, list) else [res]
        return None

    async def _read_batch(self, worker, chat_id: int, ids: List[int]) -> Optional[Dict[int, Any]]:
        """{msg_id: message | None}; falls over to other bots, then the main bot. None = inconclusive."""
        pool = self._workers()
        start = pool.index(worker) if worker in pool else 0
        order = [worker] + [pool[(start + n) % len(pool)] for n in range(1, min(3, len(pool)))] + [self.client]
        tried = []
        for client in order:
            if any(client is t for t in tried) or (id(client), chat_id) in self._no_access:
                continue
            tried.append(client)
            try:
                res = await self._get_batch(client, chat_id, ids)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if type(exc).__name__ in _ACCESS_ERRORS:
                    self._no_access.add((id(client), chat_id))
                LOGGER.warning(f"[Verify] Batch read failed on bot {self._key(client)}: {type(exc).__name__}: {exc}")
                continue
            if res is not None:
                return {m.id: m for m in res if m is not None and getattr(m, "id", None) is not None}
        return None

    @staticmethod
    def _stored_resolution(value) -> Optional[str]:
        value = str(value or "").strip()
        return value if value and value.lower() not in ("unknown", "n/a", "none") else None

    @staticmethod
    def _alive(msg) -> Optional[bool]:
        if msg is None or getattr(msg, "empty", False):
            return False
        if getattr(msg, "restrictions", None) or getattr(msg, "restriction_reason", None):
            return None  # restricted: inconclusive, never evidence of a dead file
        return bool(is_media(msg))

    async def _peek(self, client, chat_id, msg_id):
        """(message, alive) for a single message; alive is None when inconclusive."""
        try:
            msg = await get_check_message(client, chat_id, msg_id, source="Verify")
        except CheckUnavailable:
            return None, None
        return msg, bool(msg is not None and not msg.empty and is_media(msg))

    async def _fetch(self, worker, chat_id, msg_id):
        async with asyncio.timeout(CHECK_TIMEOUT):
            msg, alive = await self._peek(worker, chat_id, msg_id)
            if alive or worker is self.client:
                return msg, alive
            return await self._peek(self.client, chat_id, msg_id)

    async def _evaluate_message(self, msg, stored_quality=None) -> tuple:
        """Run the scan's metadata pipeline for one message -> (info | None, title).

        The caption is only trusted when it reads like a release name; promo captions
        ("MuST Join➥ - ARM KOREAN DRAMA") fall back to the file name. A clean miss on the first
        text gets one more attempt with the other text when that is a different release-like name.
        """
        file = msg.video or msg.document
        file_name = getattr(file, "file_name", None)
        title = choose_media_title(msg.caption, file_name)
        if not title:
            return None, ""
        channel_int = int(str(msg.chat.id).replace("-100", ""))
        kwargs = dict(
            override_id=extract_default_id(caption_with_links(msg)),
            # "Inception (2010).mkv" has no resolution in its name: the filing already has one.
            quality_hint=source_resolution(msg.caption, file_name) or self._stored_resolution(stored_quality),
            raise_errors=True,
        )
        info = await metadata(clean_filename(title), channel_int, msg.id, **kwargs)
        if info is None:
            alt = alternate_media_title(msg.caption, file_name, title)
            if alt:
                info = await metadata(clean_filename(alt), channel_int, msg.id, **kwargs)
                if info is not None:
                    title = alt
        return info, title

    # A provider miss must not undo a filing that still agrees with the file name.
    @staticmethod
    def _name_agrees(unit: dict, title: str) -> bool:
        doc = unit["doc"]
        if not doc.get("title"):
            return False
        try:
            ident = extract_identity(clean_filename(title))
            if not ident.usable:
                return False
            if unit["media_type"] == "movie":
                doc_year = _int_or_none(doc.get("release_year")) or 0
                if ident.year and doc_year:
                    return _verify_score_candidate_aliases(
                        ident.title, ident.year, doc["title"], doc_year,
                        year_reliable=True, year_lower_bound=False) >= _VERIFY_MOVIE_MATCH_THRESHOLD
                # No year on one side ("Rocky Aur Rani Kii Prem Kahaani.mkv"): the title alone must be near-exact.
                return _verify_score_candidate_aliases(
                    ident.title, None, doc["title"], 0,
                    year_reliable=False, year_lower_bound=False) >= 0.85
            from Backend.helper.metadata.parse import parse_media_name
            parsed = parse_media_name(clean_filename(title))
            if _int_or_none(parsed.get("season")) != unit["season"] or _int_or_none(parsed.get("episode")) != unit["episode"]:
                return False
            return _verify_score_candidate_aliases(
                parsed.get("title") or ident.title, None, doc["title"], 0,
                year_reliable=False, year_lower_bound=True) >= _VERIFY_MOVIE_MATCH_THRESHOLD
        except Exception:
            return False

    # A new identity is only applied when it also agrees with the file name (movies).
    @staticmethod
    def _confident(info: dict, title: str, explicit_id: bool) -> bool:
        if explicit_id or info.get("media_type") == "tv":
            return True
        ident = extract_identity(clean_filename(title))
        if not ident.usable:
            return False
        names = [info.get("title"), info.get("title_english"), info.get("original_title")]
        score = _verify_score_candidate_aliases(
            ident.title, ident.year, info.get("title") or "", _int_or_none(info.get("year")) or 0,
            aliases=[n for n in names[1:] if n], year_reliable=True, year_lower_bound=False)
        return score >= _VERIFY_MOVIE_MATCH_THRESHOLD

    # ── phase A: read-only evaluation (bounded by the shared metadata budget) ─
    async def _evaluate(self, unit: dict, msg, alive, worker) -> dict:
        try:
            if alive is None:
                return {"unit": unit, "status": "error"}
            if alive is False:
                return {"unit": unit, "status": "dead"}
            if is_skip_channel(msg) or not (msg.video or msg.document):
                return {"unit": unit, "status": "skipped"}
            file = msg.video or msg.document
            stored_quality = unit["quality"].get("quality")
            # Admission follows the settings (resolution filters, "allow unknown resolution"):
            # only what the name/caption itself says counts, never the stored quality.
            if rejection_reason(source_resolution(msg.caption, file.file_name)):
                # Indexed earlier, excluded by today's resolution policy: not a title problem,
                # so never forward/delete it from here.
                return {"unit": unit, "status": "skipped"}
            async with self._meta_sem:
                info, title = await asyncio.wait_for(self._evaluate_message(msg, stored_quality), VERIFY_METADATA_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("[Verify] Could not evaluate stream=%s: %s: %s",
                           check_reference(unit["quality"].get("id")), type(exc).__name__, exc)
            return {"unit": unit, "status": "error"}

        out = {"unit": unit, "msg": msg, "info": info, "title": title, "worker": worker, "sources": unit["sources"]}
        if info is None and (unit["quality"].get("video_part") or unit["quality"].get("video_group")):
            # Playable multipart videos are rejected by metadata() whenever that setting is off.
            out["status"] = "skipped"
        elif info is None:
            if self._name_agrees(unit, title):
                out["status"], out["kept"] = "matched", True
            else:
                out["status"] = "unmatched"
        elif same_title(unit["doc"], unit["media_type"], info) and (not self.check_episodes or same_slot(unit, info)):
            out["status"] = "matched"
        elif not self._confident(info, title, bool(extract_default_id(caption_with_links(msg)))):
            # Weak new match: never re-assign it. If the current filing still agrees with the
            # file name keep it; only a filing that does not agree either goes to Skip.
            if self._name_agrees(unit, title):
                out["status"], out["kept"] = "matched", True
            else:
                out["status"] = "unmatched"
        else:
            out["status"] = "mismatch"
        return out

    # ── phase B: changes (the shared live-ingestion lock serializes DB writes) ─
    async def _remove_old(self, unit: dict) -> bool:
        return await self.db.remove_quality_entry(
            unit["db_index"], unit["media_type"], unit["doc_id"], unit["quality"]["id"],
            season=unit["season"], episode=unit["episode"])

    async def _prepare_part(self, chat_id, msg_id, first: Optional[dict], worker, stored_quality=None) -> Optional[dict]:
        """Metadata + insert arguments for one backing message (the first reuses phase A's work)."""
        if first is not None:
            msg, info, title = first["msg"], first["info"], first["title"]
        else:
            msg, alive = await self._fetch(worker, chat_id, msg_id)
            if not alive:
                return None
            async with self._meta_sem:
                info, title = await asyncio.wait_for(self._evaluate_message(msg, stored_quality), VERIFY_METADATA_TIMEOUT)
            if info is None:
                return {"unmatched": True, "chat_id": chat_id, "msg_id": msg_id}
        file = msg.video or msg.document
        channel_int = int(str(msg.chat.id).replace("-100", ""))
        encoded = info.get("encoded_string") or await encode_string({"chat_id": channel_int, "msg_id": msg.id})
        info = {**info, "encoded_string": encoded}
        # Never upload thumbnails to the image-host channel here: it posts a message per new
        # title (FloodWait stalls) and holds the whole lane. The lazy /thumb/ route serves it.
        key = "episode_backdrop" if info.get("media_type") == "tv" else "backdrop"
        if not info.get(key) and message_has_thumb(msg):
            info[key] = f"/thumb/{encoded}"
        return {
            "info": info, "channel": channel_int, "msg_id": msg.id,
            "size": get_readable_file_size(file.file_size), "raw_size": file.file_size,
            "name": finalize_media_name(title, bool(info.get("group_key"))),
        }

    async def _apply_mismatch(self, out: dict) -> None:
        unit, info, c = out["unit"], out["info"], self.counters
        tv = info.get("media_type") == "tv"
        target = _label({"title": info.get("title")},
                        _int_or_none(info.get("season_number")) if tv else None,
                        _int_or_none(info.get("episode_number")) if tv else None)
        c["mismatched"] += 1
        if self.dry_run:
            self._note("reassign", unit, to=target, name=out["title"], api=info.get("meta_source") or info.get("source") or "Unknown")
            return
        try:
            prepared = []
            for n, (chat_id, msg_id) in enumerate(out["sources"]):
                part = await self._prepare_part(chat_id, msg_id, out if n == 0 else None, out["worker"],
                                                stored_quality=out["unit"]["quality"].get("quality"))
                if part is not None:
                    prepared.append(part)
            if not prepared or all(p.get("unmatched") for p in prepared):
                raise RuntimeError("no indexable source for reassignment")

            from Backend.pyrofork.plugins.receiver import db_lock
            await self._wait_playback()
            duplicate = False
            announced = False
            async with db_lock:
                for part in prepared:
                    if part.get("unmatched"):
                        continue
                    status: dict = {}
                    result = await self.db.insert_media(
                        part["info"], channel=part["channel"], msg_id=part["msg_id"],
                        size=part["size"], name=part["name"], raw_size=part["raw_size"], status=status)
                    if not result:
                        raise RuntimeError("insert refused by database")
                    duplicate = duplicate or bool(status.get("duplicate_skipped"))
                    if not status.get("duplicate_skipped") and part["info"].get("tmdb_id"):
                        item = {"tmdb_id": part["info"]["tmdb_id"], "media_type": part["info"].get("media_type")}
                        self._catalog[f'{item["media_type"]}:{item["tmdb_id"]}'] = item
                        if not announced and SettingsManager.current().announce_scan_titles:
                            # Same poster post as live ingestion; queued, never awaited here.
                            from Backend.helper.announcer import announce_new_media
                            announce_new_media(part["info"])
                            announced = True
                await self._remove_old(unit)
            c["duplicates" if duplicate else "reassigned"] += 1
            self._note("duplicate" if duplicate else "reassign", unit, to=target, name=out["title"], api=info.get("meta_source") or info.get("source") or "Unknown")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The old entry was never removed: the file stays where it was.
            LOGGER.error(f"[Verify] Reassignment failed: {type(exc).__name__}: {exc}")
            self.state["errors"] += 1
            c["mismatched"] -= 1

    async def _skip_unmatched(self, out: dict) -> None:
        """Move an unidentifiable source to Skip only after copy and reason succeed."""
        unit = out["unit"]
        if self.dry_run:
            self._note("unmatched", unit, to="Skip channel", name=out.get("title") or "", api="No confident match")
            return
        from Backend.pyrofork.plugins.receiver import db_lock
        reason = "No sufficiently confident movie/series identity could be established by the metadata matcher."
        moved = 0
        try:
            sources = out.get("sources") or []
            for index, (chat_id, msg_id) in enumerate(sources):
                routers = await self._eligible_routers(chat_id)
                if not routers:
                    raise RuntimeError(f"No bot has the required permissions for source channel {chat_id}")
                # Select a bot that can access BOTH the source and configured Skip
                # destination. Multi-bot deployments often have uneven memberships.
                router = None
                skip_value = SettingsManager.current().skip_channel
                skip_chat = int(skip_value) if str(skip_value).lstrip("-").replace("-100", "").isdigit() else skip_value
                for candidate in routers:
                    try:
                        await self._plain_call(candidate.get_chat, skip_chat)
                        router = candidate
                        break
                    except Exception as access_error:
                        LOGGER.debug("[Verify] Bot %s cannot access Skip channel %r: %s",
                                     self._key(candidate), skip_value, access_error)
                if router is None:
                    raise RuntimeError(
                        f"No connected bot can access Skip channel {skip_value!r}. "
                        "Verify the channel ID and add at least one ingestion bot as a member with posting permission."
                    )
                msg = out.get("msg") if index == 0 and int(getattr(out.get("msg"), "id", 0) or 0) == int(msg_id) else None
                if msg is None:
                    msg = await get_check_message(router, chat_id, msg_id, source="Verify skip")
                if msg is None or getattr(msg, "empty", False) or not is_media(msg):
                    raise RuntimeError(f"Could not re-read media source {chat_id}/{msg_id}")
                await route_to_skip_channel(router, msg, reason=reason, retry_call=self._plain_call)
                moved += 1
            if moved != len(sources):
                raise RuntimeError("Not every backing Telegram message was moved to Skip")
            await self._wait_playback()
            async with db_lock:
                await self._remove_old(unit)
            self.counters["moved_to_skip"] += 1
            self._note("unmatched", unit, to="Skip channel", name=out.get("title") or "", api="No confident match")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.error("[Verify] Could not move unmatched media to Skip: %s: %s", type(exc).__name__, exc)
            self.state["errors"] += 1

    async def _apply(self, out: dict) -> None:
        async with self._apply_sem:
            if out["status"] == "unmatched":
                await self._skip_unmatched(out)
                return
            await self._apply_mismatch(out)

    # ── one batch = up to 200 files of one channel, read in a single call ───
    async def _process(self, worker, chat_id: int, units: List[dict]) -> None:
        s, c = self.state, self.counters
        await self._wait_playback()
        by_msg: Dict[int, List[dict]] = {}
        for unit in units:
            by_msg.setdefault(unit["msg"], []).append(unit)
        msgs = await self._read_batch(worker, chat_id, sorted(by_msg))

        verdict: Dict[int, tuple] = {}
        if msgs is not None:
            doubtful = []
            for mid in by_msg:
                alive = self._alive(msgs.get(mid))
                verdict[mid] = (msgs.get(mid), alive)
                if alive is False:
                    doubtful.append(mid)
            if doubtful:
                # A worker's "gone" is only believed once the main bot agrees.
                confirm = None
                try:
                    res = await self._get_batch(self.client, chat_id, doubtful)
                    confirm = {m.id: m for m in res or [] if m is not None and getattr(m, "id", None) is not None}
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.warning(f"[Verify] Dead-file confirmation failed: {type(exc).__name__}: {exc}")
                for mid in doubtful:
                    verdict[mid] = ((confirm.get(mid), self._alive(confirm.get(mid))) if confirm is not None
                                    else (None, None))

        results = await asyncio.gather(*[
            self._evaluate(u, *(verdict.get(u["msg"], (None, None))), worker) for u in units
        ], return_exceptions=True)
        outs = [r for r in results if isinstance(r, dict)]

        unmatched = sum(1 for o in outs if o["status"] == "unmatched")
        suspect = len(outs) >= OUTAGE_MIN_BATCH and unmatched / len(outs) >= OUTAGE_RATIO
        if suspect:
            self._suspect_streak += 1
            LOGGER.warning("[Verify] %s of %s files in channel %s matched nothing; treating as a metadata "
                           "outage and leaving the batch untouched.", unmatched, len(outs), chat_id)
        elif len(outs) >= OUTAGE_MIN_BATCH:
            self._suspect_streak = 0

        to_apply = []
        for out in outs:
            status, unit = out["status"], out["unit"]
            s["checked"] += 1
            if status == "dead":
                s["dead"] += 1
                s["dead_entries"].append({"id": unit["quality"]["id"],
                                          "title": _label(unit["doc"], unit["season"], unit["episode"])})
                continue
            if status == "error":
                s["errors"] += 1
                continue
            s["alive"] += 1
            if status == "skipped":
                c["skipped"] += 1
                continue
            c["verified"] += 1
            if status == "matched":
                c["matched"] += 1
                if out.get("kept"):
                    c["kept"] += 1
            elif status == "unmatched":
                c["unmatched"] += 1
                if suspect:
                    # Provider outages must never mass-delete otherwise healthy source files.
                    c["held"] += 1
                    LOGGER.warning("[Verify] Suspected metadata outage: holding unmatched file %s.",
                                   _label(unit["doc"], unit["season"], unit["episode"]))
                else:
                    to_apply.append(out)
            else:
                to_apply.append(out)
        failed = len(results) - len(outs)
        s["errors"] += failed
        s["checked"] += failed
        now = time.monotonic()
        self._samples.append((now, s["checked"]))
        while len(self._samples) > 2 and now - self._samples[0][0] > SPEED_WINDOW:
            self._samples.popleft()
        t0, c0 = self._samples[0]
        s["speed"] = round((s["checked"] - c0) / max(1.0, now - t0), 1)   # recent, not lifetime average

        if to_apply:
            await asyncio.gather(*[self._apply(o) for o in to_apply])

        if self._suspect_streak >= OUTAGE_ABORT_BATCHES and self._abort is None:
            self._abort = RuntimeError(
                "Stopped: almost nothing is matching, which usually means the metadata providers "
                "(TMDB/TVDB/Cinemeta) are unreachable. Files in those batches were not changed.")

    # ── pipeline: library walk -> per-channel batches -> one lane per bot ───
    async def _produce(self, queue: asyncio.Queue) -> None:
        s = self.state
        buckets: Dict[int, List[dict]] = {}
        held = 0

        async def flush(chat):
            nonlocal held
            batch = buckets.pop(chat)
            held -= len(batch)
            await queue.put((chat, batch))

        async for unit in iter_units(self.db):
            if self.m._cancel or self._abort:
                return
            try:
                sources = await unit_sources(unit["quality"])
                chat_id, msg_id = sources[0]
                unit["chat"], unit["msg"], unit["sources"] = int(str(chat_id).replace("-100", "")), int(msg_id), sources
            except Exception:
                s["checked"] += 1
                s["errors"] += 1
                continue
            buckets.setdefault(unit["chat"], []).append(unit)
            held += 1
            if len(buckets[unit["chat"]]) >= VERIFY_BATCH_SIZE:
                await flush(unit["chat"])
            elif held >= VERIFY_BUFFER:
                await flush(max(buckets, key=lambda k: len(buckets[k])))
        for chat in list(buckets):
            if self.m._cancel or self._abort:
                return
            await flush(chat)

    async def run(self) -> None:
        workers = self._workers()
        if self.pause_for_playback:
            from Backend.helper.scan_resources import PlaybackPriority
            self._priority = PlaybackPriority(self._stopped, True, grace=10)
        LOGGER.info(f"[Verify] Starting with {len(workers)} bot(s), metadata budget "
                    f"{self._meta_sem._value}, batches of {VERIFY_BATCH_SIZE}"
                    f"{' (preview only)' if self.dry_run else ''}")
        queue: asyncio.Queue = asyncio.Queue(maxsize=len(workers) * 2)

        async def lane(worker):
            while True:
                item = await queue.get()
                if item is None:
                    return
                if self.m._cancel or self._abort:
                    continue  # drain so the producer can finish
                await self._process(worker, item[0], item[1])

        async def producer():
            try:
                await self._produce(queue)
            finally:
                for _ in workers:
                    await queue.put(None)

        tasks = [asyncio.create_task(producer())] + [asyncio.create_task(lane(w)) for w in workers]
        try:
            await asyncio.gather(*tasks)
        finally:
            self._stopped.set()
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._abort:
            raise self._abort
        await self._sync_catalogs()

    async def _sync_catalogs(self) -> None:
        if not self._catalog or self.dry_run:
            return
        from Backend.helper.auto_catalog import sync_single_media
        for key, item in self._catalog.items():
            try:
                await sync_single_media(self.db, **item)
            except Exception as exc:
                LOGGER.warning(f"[Verify] Catalog update failed for {key}: {exc}")
