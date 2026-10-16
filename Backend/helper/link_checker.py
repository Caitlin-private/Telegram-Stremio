import asyncio
import time

from pyrogram.errors import FloodWait

from Backend.helper.encrypt import decode_string
from Backend.helper.pyro import is_media
from Backend.helper.file_sizes import collect_file_size, size_updates
from Backend.helper.checker_workers import ordered_check_clients
from Backend.logger import LOGGER
from Backend.pyrofork.bot import multi_clients

ALIVE, DEAD, UNKNOWN = "alive", "dead", "unknown"


#----- Background task that periodically flags dead Telegram links
class DeadLinkChecker:
    def __init__(self, db, app, check_interval_hours: int = 24):
        self.db = db
        self.app = app
        self.check_interval_seconds = check_interval_hours * 3600
        self.is_running = False
        self._next_check_at = time.monotonic() + 120
        self._manual_running = False
        self._manual_generation = 0

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        LOGGER.info(f"Started Dead Link Checker background task (Interval: {self.check_interval_seconds}s)")
        asyncio.create_task(self._run_loop())

    def manual_started(self):
        """Suspend automatic checks immediately while a manual check is active."""
        self._manual_running = True
        self._manual_generation += 1

    def manual_finished(self, successful: bool):
        """A completed manual check replaces the last automatic check."""
        self._manual_running = False
        if successful:
            self._next_check_at = time.monotonic() + self.check_interval_seconds

    async def _run_loop(self):
        while self.is_running:
            if self._manual_running or time.monotonic() < self._next_check_at:
                await asyncio.sleep(1)
                continue
            generation = self._manual_generation
            try:
                LOGGER.info("Starting Dead Link Checker scan...")
                completed = await self._scan_all_media(generation)
                if completed and generation == self._manual_generation:
                    LOGGER.info("Dead Link Checker scan complete.")
                    self._next_check_at = time.monotonic() + self.check_interval_seconds
            except Exception as e:
                LOGGER.error(f"Error in Dead Link Checker loop: {e}")
                self._next_check_at = time.monotonic() + self.check_interval_seconds
            await asyncio.sleep(1)

    async def _wait_for_manual(self, generation):
        return self._manual_running or generation != self._manual_generation

    async def _scan_all_media(self, generation=None):
        if not multi_clients:
            LOGGER.warning("No bot clients available for Dead Link Checker.")
            return False

        for i in range(1, self.db.current_db_index + 1):
            if await self._wait_for_manual(generation):
                return False
            active_db = self.db.dbs[f"storage_{i}"]

            #----- Movies (snapshot ids first so no cursor stays open during the slow Telegram checks)
            try:
                movie_ids = [d["_id"] for d in await active_db["movie"].find(
                    {"telegram": {"$exists": True, "$not": {"$size": 0}}, "telegram.is_dead": {"$ne": True}},
                    {"_id": 1},
                ).to_list(None)]
                for movie_id in movie_ids:
                    if await self._wait_for_manual(generation):
                        return False
                    movie = await active_db["movie"].find_one({"_id": movie_id})
                    if not movie:
                        continue
                    tmdb_id = movie.get("tmdb_id")
                    for quality in movie.get("telegram", []):
                        if await self._wait_for_manual(generation):
                            return False
                        if quality.get("is_dead"):
                            continue
                        sizes = []
                        status = await self._check_file_alive(self._next_clients(), quality.get("id"), sizes)
                        await self._save_sizes(active_db, "movie", movie_id, quality, sizes)
                        if status == DEAD:
                            LOGGER.warning(f"Found dead link for Movie {tmdb_id} (Quality: {quality.get('quality')})")
                            await self.db.flag_dead_link("movie", tmdb_id, i, quality.get("id"))
                        await asyncio.sleep(0.5)
            except Exception as e:
                LOGGER.error(f"Error scanning movies in DB {i}: {e}")

            #----- TV Shows (snapshot ids first, then re-read each doc with a short query)
            try:
                tv_ids = [d["_id"] for d in await active_db["tv"].find(
                    {"seasons.episodes.telegram": {"$exists": True, "$not": {"$size": 0}}, "seasons.episodes.telegram.is_dead": {"$ne": True}},
                    {"_id": 1},
                ).to_list(None)]
                for tv_id in tv_ids:
                    if await self._wait_for_manual(generation):
                        return False
                    tv = await active_db["tv"].find_one({"_id": tv_id})
                    if not tv:
                        continue
                    tmdb_id = tv.get("tmdb_id")
                    for season in tv.get("seasons", []):
                        for ep in season.get("episodes", []):
                            for quality in ep.get("telegram", []):
                                if await self._wait_for_manual(generation):
                                    return False
                                if quality.get("is_dead"):
                                    continue
                                sizes = []
                                status = await self._check_file_alive(self._next_clients(), quality.get("id"), sizes)
                                await self._save_sizes(active_db, "tv", tv_id, quality, sizes)
                                if status == DEAD:
                                    LOGGER.warning(f"Found dead link for TV {tmdb_id} S{season.get('season_number')}E{ep.get('episode_number')} (Quality: {quality.get('quality')})")
                                    await self.db.flag_dead_link("tv", tmdb_id, i, quality.get("id"))
                                await asyncio.sleep(0.5)
            except Exception as e:
                LOGGER.error(f"Error scanning TV shows in DB {i}: {e}")

        return True

    def _next_clients(self):
        offset = getattr(self, "_worker_cursor", 0)
        self._worker_cursor = offset + 1
        return ordered_check_clients(multi_clients, offset)

    async def _save_sizes(self, storage, kind, document_id, quality, sizes):
        try:
            operations = size_updates(document_id, kind, quality, sizes)
            if operations:
                await storage[kind].bulk_write(operations, ordered=False)
        except Exception:
            LOGGER.warning("[LinkChecker] Size update failed; continuing the link check", exc_info=True)

    async def _check_file_alive(self, clients, quality_id: str, sizes=None) -> str:
        try:
            decoded = await decode_string(quality_id)
        except Exception as e:
            LOGGER.error(f"Link checker failed to decode {quality_id}: {e}")
            return UNKNOWN
        if not decoded:
            return UNKNOWN

        #----- Split file: every part must be alive; one confirmed-dead part kills the link
        if "parts" in decoded:
            parts = decoded.get("parts") or []
            if not parts:
                return UNKNOWN
            saw_unknown = False
            for part in parts:
                status = await self._check_message_status(clients, part.get("chat_id"), part.get("msg_id"), sizes)
                if status == DEAD:
                    return DEAD
                if status == UNKNOWN:
                    saw_unknown = True
            return UNKNOWN if saw_unknown else ALIVE

        #----- Single file
        if "chat_id" not in decoded or "msg_id" not in decoded:
            return UNKNOWN
        return await self._check_message_status(clients, decoded["chat_id"], decoded["msg_id"], sizes)

    async def _check_message_status(self, clients, chat_id, msg_id, sizes=None) -> str:
        if chat_id is None or msg_id is None:
            return UNKNOWN

        chat_id = int(f"-100{chat_id}")
        msg_id = int(msg_id)
        confirmed_dead = False
        for client in clients:
            try:
                message = await client.get_messages(chat_id, msg_id)
            except FloodWait as e:
                await asyncio.sleep(getattr(e, "value", 5) + 1)
                continue
            except Exception:
                continue

            if message is None or message.empty:
                confirmed_dead = True
                continue
            if is_media(message):
                collect_file_size(message, chat_id, msg_id, sizes)
                return ALIVE
            confirmed_dead = True

        return DEAD if confirmed_dead else UNKNOWN
