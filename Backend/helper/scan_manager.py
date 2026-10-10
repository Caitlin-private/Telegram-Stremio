from __future__ import annotations

import asyncio
import time
from contextvars import ContextVar
from typing import Any, Dict, List, Optional

from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired, MessageDeleteForbidden

from Backend.logger import LOGGER
from Backend.helper.encrypt import encode_string, decode_string
from Backend.helper.metadata import metadata, extract_default_id
from Backend.helper.pyro import apply_video_thumb_to_metadata, clean_filename, finalize_media_name, get_readable_file_size
from Backend.helper.skip_channel import is_skip_channel, route_to_skip_channel
from Backend.helper.split_files import parse_split_info
from Backend.helper.subtitles import ingest_subtitle, is_subtitle_file

SCAN_BATCH_SIZE = 200          
SCAN_MAX_EMPTY_BATCHES = 10    
SCAN_MAX_ID_CAP = 1_000_000    
SCAN_BATCH_DELAY = 0.5         
SCAN_PERSIST_EVERY = 1         
SCAN_PROBE_TEXT = "🔄"         
SCAN_PROCESS_CONCURRENCY = 8   

DBCHECK_CONCURRENCY = 5        
DBCHECK_BATCH_DELAY = 0.3      
DBCHECK_PAGE_SIZE = 100        

_STATE_COLLECTION = "scan_state"
_SCAN_DOC_ID = "scan"


class _ScanStopped(Exception):
    """Cooperative stop while awaiting Telegram's retry window."""


def _now() -> float:
    return time.time()


def _fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


class ScanManager:
    def __init__(self) -> None:
        self._db = None
        self._task: Optional[asyncio.Task] = None
        self._cancel = False
        self._lock = asyncio.Lock()
        self._db_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._notice_task = None
        self._worker_context = ContextVar('scan_worker', default=None)
        self._worker_waits = {}
        self._priority = None
        self._metrics = {'telegram_seconds': 0.0, 'metadata_seconds': 0.0, 'db_seconds': 0.0, 'db_lock_seconds': 0.0}
        self.state: Dict[str, Any] = self._blank_state()

    #----- ── State helpers ────────────────────────────────────────────────────────
    @staticmethod
    def _blank_state() -> Dict[str, Any]:
        return {
            "status": "idle",            
            "mode": "scan",              
            "flood_wait_until": 0.0,
            "worker_limit": 2,
            "start_id": None,
            "end_id": None,
            "pause_for_playback": True,
            "active_workers": 0,
            "worker_waits": {},
            "catalog_pending": {},
            "purge_pending": [],
            "selected_channels": [],     
            "pending": [],               
            "current_channel": None,
            "current_channel_name": "",
            "current_id": 0,             
            "current_target_id": 0,      
            "cursors": {},               
            "counters": ScanManager._blank_counters(),
            "started_at": 0.0,
            "updated_at": 0.0,
            "finished_at": 0.0,
            "error": None,
        }

    @staticmethod
    def _blank_counters() -> Dict[str, int]:
        return {
            "skipped_delete_forbidden": 0,
            "total_found": 0,
            "processed": 0,
            "indexed": 0,
            "skipped_dup": 0,
            "skipped_meta": 0,
            "skipped_nonvid": 0,
            "subtitles_added": 0,
            "subtitles_skipped": 0,
            "errors": 0,
        }

    def bind_db(self, db) -> None:
        self._db = db

    async def load(self, db) -> None:
        self._db = db
        try:
            doc = await db.dbs["tracking"][_STATE_COLLECTION].find_one({"_id": _SCAN_DOC_ID})
        except Exception as e:
            LOGGER.error(f"[ScanManager] load failed: {e}")
            doc = None

        if doc:
            doc.pop("_id", None)
            if doc.get('discard_requested'):
                self.state = doc
                await self._discard_job()
                return
            merged = self._blank_state()
            merged.update(doc)
            merged["cursors"] = {str(k): int(v) for k, v in (merged.get("cursors") or {}).items()}
            if merged["status"] == "running":
                merged["status"] = "paused"
                report = merged.get('import_report') or {}
                if report.get('run_started'):
                    report['elapsed'] = report.get('elapsed', 0) + max(
                        0, merged.get('updated_at', report['run_started']) - report['run_started'])
                    report['run_started'] = 0
            self.state = merged
            if self.state["status"] == "paused":
                LOGGER.info("[ScanManager] Found an interrupted scan — marked as paused (resumable).")
            await self._persist()
        else:
            self.state = self._blank_state()

    async def _persist(self) -> None:
        if self._db is None:
            return
        self.state["updated_at"] = _now()
        try:
            doc = dict(self.state)
            doc["_id"] = _SCAN_DOC_ID
            await self._db.dbs["tracking"][_STATE_COLLECTION].update_one(
                {"_id": _SCAN_DOC_ID}, {"$set": doc}, upsert=True
            )
        except Exception as e:
            LOGGER.error(f"[ScanManager] persist failed: {e}")

    def get_status(self) -> Dict[str, Any]:
        s = self.state
        elapsed = 0.0
        if s["started_at"]:
            end = s["finished_at"] or _now()
            elapsed = max(0.0, end - s["started_at"])

        target = int(s.get("current_target_id", 0) or 0)
        cur = int(s.get("current_id", 0) or 0)
        progress = max(0, min(100, round(cur / target * 100))) if target > 0 else 0

        return {
            "status": s["status"],
            "mode": s["mode"],
            "cancelling": bool(s.get('discard_requested')),
            "worker_limit": s.get('worker_limit', 2),
            "start_id": s.get('start_id'),
            "end_id": s.get('end_id'),
            "pause_for_playback": s.get('pause_for_playback', True),
            "playback_paused": bool(self._priority and self._priority.paused),
            "active_workers": s.get('active_workers', 0),
            "timings": {k: round(v, 2) for k, v in self._metrics.items()},
            "flood_wait_seconds": max(0, int(s.get('flood_wait_until', 0) - _now() + 0.999)),
            "is_running": s["status"] == "running",
            "resumable": s["status"] in ("paused", "cancelled", "error") and bool(s["pending"]),
            "selected_channels": list(s["selected_channels"]),
            "pending": list(s["pending"]),
            "current_channel": s["current_channel"],
            "current_channel_name": s["current_channel_name"],
            "current_id": cur,
            "current_target_id": target,
            "progress": progress,
            "has_progress": target > 0,
            "counters": dict(s["counters"]),
            "elapsed": _fmt_elapsed(elapsed),
            "elapsed_seconds": int(elapsed),
            "error": s["error"],
        }

    async def _stream_id_exists(self, channel: int, msg_id: int) -> bool:
        db = self._db
        try:
            stream_hash = await encode_string({"chat_id": channel, "msg_id": msg_id})
        except Exception:
            stream_hash = None
        part_match = {"$elemMatch": {"chat_id": channel, "msg_id": msg_id}}
        for i in range(1, db.current_db_index + 1):
            storage = db.dbs.get(f"storage_{i}")
            if storage is None:
                continue
            for collection, prefix in (('movie', 'telegram'), ('tv', 'seasons.episodes.telegram')):
                checks = [{f'{prefix}.parts': part_match}]
                if stream_hash:
                    checks.append({f'{prefix}.id': stream_hash})
                if await storage[collection].find_one({'$or': checks}, {'_id': 1}):
                    return True
        return False

    async def start(self, client, channels: List[str], mode: str = "scan", worker_limit=None, pause_for_playback=None, start_id=None, end_id=None) -> Dict[str, Any]:
        async with self._lock:
            if self.state["status"] == "running" or (self._task and not self._task.done()):
                return {"ok": False, "message": "A scan is already running."}

            channels = [str(c).strip() for c in (channels or []) if str(c).strip()]

            if mode in ("scan", "quick") and not channels and self.state["pending"]:
                channels = list(self.state["pending"])

            if not channels:
                return {"ok": False, "message": "No channels selected."}

            resuming_job = mode != 'rescan' and self.state['status'] in ('paused', 'cancelled', 'error') and bool(self.state['pending'])
            if resuming_job:
                if ((start_id is not None and start_id != self.state.get('start_id')) or
                        (end_id is not None and end_id != self.state.get('end_id'))):
                    return {'ok': False, 'message': 'Cancel the current scan before changing its message range.'}
            else:
                self.state['start_id'], self.state['end_id'] = start_id, end_id
                for ch in channels:
                    self.state['cursors'].pop(ch, None)

            if mode == 'rescan' or self.state['status'] not in ('paused', 'cancelled', 'error'):
                self.state.pop('import_report', None)

            if mode == "rescan":
                for ch in channels:
                    self.state["cursors"].pop(str(ch), None)
                self.state['purge_pending'] = list(channels)
                self.state["selected_channels"] = list(channels)
                self.state["pending"] = list(channels)
                self.state["counters"] = self._blank_counters()
            else:
                resuming = self.state["status"] in ("paused", "cancelled", "error") and self.state["pending"]
                if resuming:
                    merged_pending = list(self.state["pending"])
                    for ch in channels:
                        if ch not in merged_pending:
                            merged_pending.append(ch)
                    self.state["pending"] = merged_pending
                    self.state["selected_channels"] = list(
                        dict.fromkeys(self.state["selected_channels"] + channels)
                    )
                else:
                    self.state["selected_channels"] = list(channels)
                    self.state["pending"] = list(channels)
                    self.state["counters"] = self._blank_counters()

            self.state["mode"] = mode
            self.state["status"] = "running"
            self.state["error"] = None
            self.state["finished_at"] = 0.0
            self.state["started_at"] = _now()
            self._cancel = False
            self._stop_event.clear()
            from Backend.helper.scan_resources import PlaybackPriority
            self.state['worker_limit'] = worker_limit if worker_limit is not None else self.state.get('worker_limit', 2)
            self.state['pause_for_playback'] = pause_for_playback if pause_for_playback is not None else self.state.get('pause_for_playback', True)
            self._priority = PlaybackPriority(self._stop_event, self.state['pause_for_playback'])
            self._worker_waits = {k: v for k, v in self.state.get('worker_waits', {}).items() if v > _now()}
            self.state['worker_waits'] = self._worker_waits
            self._metrics = dict.fromkeys(self._metrics, 0.0)
            await self._persist()

            self._task = asyncio.create_task(self._run(client))
            return {"ok": True, "message": f"{'Quick scan' if mode == 'quick' else 'Rescan' if mode == 'rescan' else 'Scan'} started.",
                    "status": self.get_status()}

    async def cancel(self, discard=False) -> Dict[str, Any]:
        if discard:
            async with self._lock:
                self.state['discard_requested'] = True
                self._cancel = True
                self._stop_event.set()
                await self._persist()
                if not self._task or self._task.done():
                    await self._discard_job()
            return {'ok': True, 'message': 'Scan cancelled. Any in-flight work will finish safely; you can then start any scan mode.'}
        if self.state["status"] != "running":
            return {"ok": False, "message": "No scan is currently running."}
        self._cancel = True
        self._stop_event.set()
        return {"ok": True, "message": "Stop requested — the scan will pause after the current batch."}

    async def _discard_job(self):
        keep = {key: self.state[key] for key in ('worker_limit', 'pause_for_playback', 'worker_waits') if key in self.state}
        self.state = self._blank_state()
        self.state.update(keep)
        self._priority = None
        await self._persist()

    async def _wait_for_playback(self):
        if self._priority:
            await self._priority.wait()
        if self._cancel:
            raise _ScanStopped()

    async def _telegram_call(self, operation, *args, scan_client=None, **kwargs):
        client = scan_client or self._worker_context.get() or getattr(operation, '__self__', None)
        key = str(getattr(getattr(client, 'me', None), 'id', None) or getattr(client, 'name', None) or id(client))
        while True:
            await self._wait_for_playback()
            if self._cancel:
                raise _ScanStopped()
            delay = self._worker_waits.get(key, 0) - _now()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                started = time.monotonic()
                try:
                    return await operation(*args, **kwargs)
                finally:
                    if getattr(operation, '__name__', '') != '_process_message':
                        self._metrics['telegram_seconds'] += time.monotonic() - started
            except FloodWait as exc:
                self._worker_waits[key] = _now() + max(1, exc.value) + 1
                self.state['flood_wait_until'] = max(self._worker_waits.values())
                LOGGER.warning(f'[ScanManager] Telegram FloodWait: {exc.value}s; retaining current message/batch for retry.')
                await self._persist()

    async def _run(self, client) -> None:
        try:
            while self.state["pending"] and not self._cancel:
                ch = self.state["pending"][0]
                try:
                    ch_id = int(ch)
                except ValueError:
                    LOGGER.warning(f"[ScanManager] invalid channel id: {ch}")
                    self.state["pending"].pop(0)
                    await self._persist()
                    continue

                await self._wait_for_playback()
                if ch in self.state.get('purge_pending', []):
                    await self._purge_channel_entries(int(str(ch).removeprefix('-100')))
                    self.state['purge_pending'].remove(ch)
                    await self._persist()
                await self._flush_scan_catalogs()
                report = self.state.get('import_report') or {}
                if report.get('channel') != ch or report.get('finished'):
                    report = {'baseline': dict(self.state['counters']), 'elapsed': 0}
                report.update(channel=ch, name=ch, mode=report.get('mode', self.state['mode']),
                              cursor=self.state['cursors'].get(ch, 1), target=0, run_started=_now())
                self.state['import_report'] = report
                completed = await self._scan_channel(client, ch_id, ch)
                if self._cancel:
                    break
                if completed:
                    if self.state["pending"] and self.state["pending"][0] == ch:
                        self.state["pending"].pop(0)
                    await self._persist()

            if self._cancel:
                self.state["status"] = "cancelled"
                LOGGER.info("[ScanManager] Scan cancelled by user (resumable).")
            else:
                self.state["status"] = "completed"
                self.state["current_channel"] = None
                self.state["current_channel_name"] = ""
                LOGGER.info("[ScanManager] Scan completed.")
            self.state["finished_at"] = _now()
            await self._persist()

        except _ScanStopped:
            self.state['status'] = 'cancelled'
            self.state['finished_at'] = _now()
            await self._persist()
        except (ChannelPrivate, ChatAdminRequired) as e:
            self.state["status"] = "error"
            self.state["error"] = f"Access denied to channel — make sure the bot is an admin. ({e})"
            self.state["finished_at"] = _now()
            LOGGER.error(f"[ScanManager] {self.state['error']}")
            await self._persist()
        except asyncio.CancelledError:
            await self._persist()
            raise
        except Exception as e:
            self.state["status"] = "error"
            self.state["error"] = str(e)
            self.state["finished_at"] = _now()
            LOGGER.error(f"[ScanManager] Unexpected error: {e}")
            await self._persist()

        finally:
            event = 'cancel' if self.state.get('discard_requested') else 'stop' if self.state['status'] == 'cancelled' else 'error'
            if self.state['status'] in ('cancelled', 'error'):
                await self._announce_import(client, event)
            if self.state.get('discard_requested'):
                await self._discard_job()

    async def _announce_import(self, client, event):
        from Backend.helper.scan_announcements import format_notice, send_notice
        from Backend.helper.stats_display import library_totals
        from Backend.helper.settings_manager import SettingsManager
        report = self.state.get('import_report')
        if not report or (event != 'start' and not report.get('run_started')):
            return
        now = _now()
        elapsed = report.get('elapsed', 0) + max(0, now - report.get('run_started', now))
        if event != 'start':
            report['elapsed'] = elapsed
            report['run_started'] = 0
            await self._persist()
        destination = SettingsManager.current().announcement_channel
        if not destination:
            return
        totals = None
        if event == 'finish':
            try:
                totals = await library_totals(self._db)
            except Exception as exc:
                LOGGER.warning(f'[Scan announcement] Cannot count library: {exc}')
        counts = {k: max(0, v - report['baseline'].get(k, 0))
                  for k, v in self.state['counters'].items()}
        try:
            destination = int(destination)
        except ValueError:
            pass
        text = format_notice(event, report, counts, _fmt_elapsed(elapsed), totals)
        self._notice_task = asyncio.create_task(send_notice(client, destination, text, self._notice_task))

    async def _flush_scan_catalogs(self):
        from Backend.helper.auto_catalog import sync_single_media
        pending = self.state.setdefault('catalog_pending', {})
        for key, item in list(pending.items()):
            await self._wait_for_playback()
            try:
                await sync_single_media(self._db, **item)
            except Exception as exc:
                LOGGER.warning(f'[Scan] Catalog update failed for {key}: {exc}')
            pending.pop(key, None)
        await self._persist()

    async def _scan_channel(self, client, chat_id: int, ch_key: str) -> bool:
        s = self.state

        try:
            chat = await self._telegram_call(client.get_chat, chat_id)
            s["current_channel_name"] = getattr(chat, "title", str(chat_id))
        except (ChannelPrivate, ChatAdminRequired, _ScanStopped):
            raise
        except Exception as e:
            s["current_channel_name"] = str(chat_id)
            LOGGER.warning(f"[ScanManager] Could not resolve channel name for {chat_id}: {e}")

        s["current_channel"] = ch_key

        last_id = await self._probe_last_message_id(client, chat_id, scan_retry=True)
        last_id = max(0, last_id - 1) if last_id is not None else None
        use_probe = last_id is not None
        s["current_target_id"] = last_id if use_probe else 0

        first_id = s.get('start_id') or 1
        if s.get('end_id') is not None:
            last_id = min(last_id, s['end_id']) if last_id is not None else s['end_id']
            use_probe = True
        s['current_target_id'] = last_id if use_probe else 0
        current = max(first_id, int(s['cursors'].get(str(ch_key), first_id) or first_id))
        scan_limit = last_id + 1 if use_probe and (s.get('start_id') or s.get('end_id')) else SCAN_MAX_ID_CAP
        report = s.get('import_report') or {}
        if report.get('channel') != ch_key or report.get('finished'):
            report = {'baseline': dict(s['counters']), 'elapsed': 0}
        report.setdefault('first_id', current)
        report.update(channel=ch_key, name=s['current_channel_name'], mode=report.get('mode', s['mode']),
                      target=max(0, min(last_id, scan_limit - 1) - report['first_id'] + 1) if use_probe else None,
                      cursor=current, run_started=_now())
        s['import_report'] = report
        await self._persist()
        await self._announce_import(client, 'start')
        from Backend.helper.scan_resources import select_scan_clients
        workers = await select_scan_clients(client, chat_id, s.get('worker_limit', 2),
                                            s.get('mode') == 'quick', self._telegram_call)
        s['active_workers'] = len(workers)
        worker_slots = {id(c): asyncio.Semaphore(max(1, SCAN_PROCESS_CONCURRENCY // len(workers))) for c in workers}
        LOGGER.info(
            f"[ScanManager] Scanning {s['current_channel_name']} ({chat_id}) from id {current}"
            + (f" up to {last_id} (probe)" if use_probe else " (heuristic mode — probe unavailable)")
            + f" using {len(workers)} bot(s), configured limit {s.get('worker_limit', 2) or 'Auto'}"
        )

        empty_streak = 0
        batch_count = 0

        while not self._cancel and current < scan_limit:
            #----- ── Stop condition ───────────────────────────────────────────────
            if use_probe:
                if current > last_id:
                    break
            elif empty_streak >= SCAN_MAX_EMPTY_BATCHES:
                break

            upper = min(current + SCAN_BATCH_SIZE, scan_limit)
            if use_probe:
                upper = min(upper, last_id + 1)
            batch_ids = list(range(current, upper))
            if not batch_ids:
                break

            async def fetch_lane(worker, ids):
                if not ids:
                    return []
                result = await self._telegram_call(worker.get_messages, chat_id, ids, scan_client=worker)
                return [(worker, m) for m in (result if isinstance(result, list) else [result])
                        if m is not None and not m.empty]
            lanes = await asyncio.gather(*(fetch_lane(worker, batch_ids[n::len(workers)])
                                          for n, worker in enumerate(workers)), return_exceptions=True)
            for lane in lanes:
                if isinstance(lane, BaseException):
                    raise lane  # Never advance past an unread lane.
            assigned = [pair for lane in lanes for pair in lane]
            to_process = [m for _, m in assigned]
            batch_had_content = bool(to_process)

            if to_process:
                # A resumed partial batch may be fetched again. Count each
                # detected source message once within this channel import.
                seen_through = report.get('detected_through', 0)
                detected = sum(1 for m in to_process if m.id > seen_through and (m.video or m.document))
                s['counters']['media_detected'] = s['counters'].get('media_detected', 0) + detected
                report['detected_through'] = max(seen_through, max(m.id for m in to_process))
                s["counters"]["total_found"] += len(to_process)
                sem = asyncio.Semaphore(SCAN_PROCESS_CONCURRENCY)

                async def _worker(worker, msg):
                    async with worker_slots[id(worker)], sem:
                        if self._cancel:
                            return
                        token = self._worker_context.set(worker)
                        try:
                            await self._telegram_call(self._process_message, worker, msg, chat_id)
                        except MessageDeleteForbidden:
                            s['counters']['skipped_delete_forbidden'] = s['counters'].get('skipped_delete_forbidden', 0) + 1
                            LOGGER.warning(f'[Scan] Cannot delete {chat_id}/{msg.id}; leaving the original and skipping this file.')
                        finally:
                            self._worker_context.reset(token)
                        s["counters"]["processed"] += 1
                        if msg.video or msg.document:
                            s['counters']['media_processed'] = s['counters'].get('media_processed', 0) + 1

                results = await asyncio.gather(*(_worker(worker, m) for worker, m in assigned), return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

            if self._cancel:
                s["cursors"][str(ch_key)] = current
                s["current_id"] = current
                await self._persist()
                return False

            empty_streak = 0 if batch_had_content else empty_streak + 1
            current = upper
            s["cursors"][str(ch_key)] = current
            s["current_id"] = current

            batch_count += 1
            if batch_count % SCAN_PERSIST_EVERY == 0:
                await self._persist()

            await self._flush_scan_catalogs()

            await asyncio.sleep(SCAN_BATCH_DELAY)

        await self._persist()
        LOGGER.info(f"[ScanManager] Finished {s['current_channel_name']} at id {current}")
        if not self._cancel:
            await self._announce_import(client, 'finish')
            report['finished'] = True
            await self._persist()
        return True

    async def _probe_last_message_id(self, client, chat_id: int, scan_retry=False):
        if scan_retry:
            probe = await self._telegram_call(client.send_message, chat_id, SCAN_PROBE_TEXT)
            try:
                await self._telegram_call(client.delete_messages, chat_id, probe.id)
            except MessageDeleteForbidden:
                LOGGER.warning(f'[Scan] Cannot remove probe {chat_id}/{probe.id}; continuing with its message boundary.')
            return probe.id
        probe = None
        try:
            probe = await client.send_message(chat_id, SCAN_PROBE_TEXT)
        except FloodWait as e:
            LOGGER.info(f"[ScanManager] FloodWait {e.value}s during probe — sleeping…")
            await asyncio.sleep(e.value)
            try:
                probe = await client.send_message(chat_id, SCAN_PROBE_TEXT)
            except Exception as ex:
                LOGGER.warning(f"[ScanManager] Probe send failed for {chat_id}: {ex}")
                return None
        except Exception as e:
            LOGGER.warning(f"[ScanManager] Could not send probe to {chat_id}: {e}")
            return None

        last_id = getattr(probe, "id", None)
        try:
            await client.delete_messages(chat_id, probe.id)
        except Exception as e:
            LOGGER.warning(
                f"[ScanManager] Could not delete probe message "
                f"{getattr(probe, 'id', None)} in {chat_id}: {e}"
            )
        return last_id

    async def _process_message(self, client, message, chat_id: int) -> None:
        s = self.state
        db = self._db
        quick = s.get('mode') == 'quick'

        if is_skip_channel(message):
            s["counters"]["skipped_meta"] += 1
            return

        #----- Subtitle files: match to a title and store, don't treat as media
        sub_name = message.document.file_name if message.document else ""
        if sub_name and is_subtitle_file(sub_name):
            channel_int = int(str(chat_id).replace("-100", ""))
            if await ingest_subtitle(sub_name, channel_int, message.id):
                s["counters"]["subtitles_added"] += 1
            else:
                s["counters"]["subtitles_skipped"] += 1
            return

        is_video = bool(message.video)
        is_supported = is_video
        if message.document and not is_video:
            mime = getattr(message.document, "mime_type", "") or ""
            if mime.startswith("video/"):
                is_supported = True
            else:
                candidate = message.caption or message.document.file_name or ""
                if parse_split_info(candidate):
                    is_supported = True

        if not is_supported:
            s["counters"]["skipped_nonvid"] += 1
            return

        file = message.video or message.document
        channel_int = int(str(chat_id).replace('-100', ''))
        if await self._stream_id_exists(channel_int, message.id):
            s['counters']['skipped_dup'] += 1
            return
        from Backend.helper.resolution_policy import source_resolution, rejection_reason
        reason = rejection_reason(source_resolution(message.caption, file.file_name))
        if reason:
            LOGGER.info(f'[Scan] Skipped {chat_id}/{message.id}: {reason}')
            if quick:
                if await client.delete_messages(chat_id, message.id) is False:
                    raise RuntimeError('Quick Scan could not delete the rejected message.')
            else:
                await route_to_skip_channel(client, message, reason=reason, force_delete=True, retry_call=self._telegram_call)
            s['counters']['skipped_resolution'] = s['counters'].get('skipped_resolution', 0) + 1
            return
        title = message.caption or file.file_name
        msg_id = message.id
        raw_size = file.file_size
        size = get_readable_file_size(file.file_size)
        channel_int = int(str(chat_id).replace("-100", ""))

        try:
            metadata_started = time.monotonic()
            metadata_info = await metadata(
                clean_filename(title), channel_int, msg_id,
                override_id=extract_default_id(message.caption or ""),
                quality_hint=source_resolution(message.caption, file.file_name),
                raise_errors=True,
            )
        except (FloodWait, MessageDeleteForbidden):
            raise
        except Exception as e:
            LOGGER.warning(f"[ScanManager] Metadata exception for msg {msg_id}: {e}")
            if quick:
                raise
            metadata_info = None
        finally:
            self._metrics['metadata_seconds'] += time.monotonic() - metadata_started

        if metadata_info is None:
            if quick:
                if await client.delete_messages(chat_id, message.id) is False:
                    raise RuntimeError('Quick Scan could not delete the rejected message.')
                s["counters"]["skipped_meta"] += 1
                return
            try:
                await route_to_skip_channel(client, message, retry_call=self._telegram_call)
            except FloodWait:
                raise
            except Exception as e:
                LOGGER.warning(f"[ScanManager] Skip-channel route failed for msg {msg_id}: {e}")
                raise
            s["counters"]["skipped_meta"] += 1
            return

        title_clean = finalize_media_name(title, bool(metadata_info.get('group_key')))
        encoded = metadata_info.get("encoded_string") or await encode_string({"chat_id": channel_int, "msg_id": msg_id})
        if not quick:
            await apply_video_thumb_to_metadata(metadata_info, message, encoded, client)

        insert_status: dict = {}
        try:
            from Backend.pyrofork.plugins.receiver import db_lock
            await self._wait_for_playback()
            lock_started = time.monotonic()
            async with db_lock, self._db_lock:
                self._metrics['db_lock_seconds'] += time.monotonic() - lock_started
                if self._cancel:
                    raise _ScanStopped()
                db_started = time.monotonic()
                if await self._stream_id_exists(channel_int, msg_id):
                    s['counters']['skipped_dup'] += 1
                    return
                updated_id = await db.insert_media(
                    metadata_info,
                    channel=channel_int,
                    msg_id=msg_id,
                    size=size,
                    name=title_clean,
                    raw_size=raw_size,
                    status=insert_status,
                )
                self._metrics['db_seconds'] += time.monotonic() - db_started
            if updated_id:
                if insert_status.get("duplicate_skipped"):
                    s["counters"]["skipped_dup"] += 1
                else:
                    s["counters"]["indexed"] += 1
                    from Backend.helper.announcer import announce_new_media
                    # One shared catalog update per title per batch, awaited
                    # through the playback gate instead of unbounded tasks.
                    item = {'tmdb_id': metadata_info.get('tmdb_id'), 'media_type': metadata_info.get('media_type')}
                    self.state.setdefault('catalog_pending', {})[f'{item["media_type"]}:{item["tmdb_id"]}'] = item
                    if not quick:
                        announce_new_media(metadata_info)
            else:
                s["counters"]["skipped_meta"] += 1
        except (FloodWait, _ScanStopped, MessageDeleteForbidden):
            raise
        except Exception as e:
            LOGGER.error(f"[ScanManager] DB insert error msg {msg_id}: {e}")
            s["counters"]["errors"] += 1

    #----- ── Purge (rescan helper) ────────────────────────────────────────────────
    async def _purge_channel_entries(self, channel_int: int) -> int:
        db = self._db
        purged = 0
        low, high = self.state.get('start_id') or 1, self.state.get('end_id') or 2147483647
        def targeted(part):
            try:
                return int(part['chat_id']) == channel_int and low <= int(part['msg_id']) <= high
            except (KeyError, TypeError, ValueError):
                return False

        async def retained(quality):
            nonlocal purged
            parts = quality.get('parts') or []
            if parts:
                remaining = [p for p in parts if not targeted(p)]
                if len(remaining) == len(parts):
                    return quality, False
                purged += len(parts) - len(remaining)
                if not remaining:
                    return None, True
                updated = dict(quality)
                archive = 'zip' if str(quality.get('group_key', '')).endswith('.zip') else None
                updated['id'], updated['size'] = await db._build_part_id_and_size(remaining, archive)
                updated['parts'] = remaining
                return updated, True
            try:
                decoded = await decode_string(quality['id'])
            except Exception:
                return quality, False
            if targeted(decoded):
                purged += 1
                return None, True
            return quality, False
        try:
            await db.dbs["tracking"]["subtitles"].delete_many({"chat_id": channel_int, 'msg_id': {'$gte': low, '$lte': high}})
        except Exception as e:
            LOGGER.warning(f"[ScanManager] subtitle purge failed for {channel_int}: {e}")
        for i in range(1, db.current_db_index + 1):
            storage = db.dbs.get(f"storage_{i}")
            if storage is None:
                continue

            async for movie in storage["movie"].find({}):
                remaining = []
                changed = False
                for q in movie.get("telegram", []):
                    kept, removed = await retained(q)
                    changed = changed or removed
                    if kept is not None:
                        remaining.append(kept)
                if changed:
                    if remaining:
                        movie["telegram"] = remaining
                        await storage["movie"].replace_one({"_id": movie["_id"]}, movie)
                    else:
                        await storage["movie"].delete_one({"_id": movie["_id"]})

            async for tv in storage["tv"].find({}):
                tv_changed = False
                for season in tv.get("seasons", []):
                    for episode in season.get("episodes", []):
                        remaining = []
                        for q in episode.get("telegram", []):
                            kept, removed = await retained(q)
                            tv_changed = tv_changed or removed
                            if kept is not None:
                                remaining.append(kept)
                        episode["telegram"] = remaining
                    season["episodes"] = [ep for ep in season["episodes"] if ep.get("telegram")]
                tv["seasons"] = [se for se in tv["seasons"] if se.get("episodes")]
                if tv_changed:
                    if tv["seasons"]:
                        await storage["tv"].replace_one({"_id": tv["_id"]}, tv)
                    else:
                        await storage["tv"].delete_one({"_id": tv["_id"]})
        return purged


class DbCheckManager:
    def __init__(self) -> None:
        self._db = None
        self._task: Optional[asyncio.Task] = None
        self._cancel = False
        self._lock = asyncio.Lock()
        self.state: Dict[str, Any] = self._blank_state()

    @staticmethod
    def _blank_state() -> Dict[str, Any]:
        return {
            "status": "idle",   
            "checked": 0,
            "alive": 0,
            "dead": 0,
            "errors": 0,
            "purged": 0,
            "speed": 0,
            "dead_entries": [],   
            "started_at": 0.0,
            "finished_at": 0.0,
            "error": None,
        }

    def bind_db(self, db) -> None:
        self._db = db

    def get_status(self) -> Dict[str, Any]:
        s = self.state
        elapsed = 0.0
        if s["started_at"]:
            end = s["finished_at"] or _now()
            elapsed = max(0.0, end - s["started_at"])
        return {
            "status": s["status"],
            "is_running": s["status"] == "running",
            "checked": s["checked"],
            "alive": s["alive"],
            "dead": s["dead"],
            "errors": s["errors"],
            "purged": s["purged"],
            "speed": s["speed"],
            "dead_count": len(s["dead_entries"]),
            "dead_entries": list(s["dead_entries"]),
            "elapsed": _fmt_elapsed(elapsed),
            "elapsed_seconds": int(elapsed),
            "error": s["error"],
        }

    #----- ── Control ───────────────────────────────────────────────────────────────
    async def start(self, client) -> Dict[str, Any]:
        async with self._lock:
            if self.state["status"] == "running":
                return {"ok": False, "message": "A DB check is already running."}
            self.state = self._blank_state()
            self.state["status"] = "running"
            self.state["started_at"] = _now()
            self._cancel = False
            self._task = asyncio.create_task(self._run(client))
            return {"ok": True, "message": "DB check started.", "status": self.get_status()}

    async def cancel(self) -> Dict[str, Any]:
        if self.state["status"] != "running":
            return {"ok": False, "message": "No DB check is currently running."}
        self._cancel = True
        return {"ok": True, "message": "Stop requested — finishing the current batch."}

    #----- ── Single-message check ───────────────────────────────────────────────────
    async def _check_message(self, client, stream_hash: str):
        try:
            decoded = await decode_string(stream_hash)
            if isinstance(decoded, dict) and "parts" in decoded:
                parts = decoded.get("parts") or []
                if not parts:
                    return False
                for part in parts:
                    alive = await self._check_one(client, part.get("chat_id"), part.get("msg_id"))
                    if alive is None:
                        return None
                    if not alive:
                        return False
                return True
            return await self._check_one(client, decoded.get("chat_id"), decoded.get("msg_id"))
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self._check_message(client, stream_hash)
        except Exception:
            return None

    async def _check_one(self, client, chat_id, msg_id):
        if chat_id is None or msg_id is None:
            return False
        try:
            chat_id = int(f"-100{chat_id}")
            msg_id = int(msg_id)
            msg = await client.get_messages(chat_id, msg_id)
            if msg is None or msg.empty:
                return False
            return True
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self._check_one(client, str(chat_id).replace("-100", ""), msg_id)
        except Exception:
            return None

    async def _process_batch(self, client, batch: List[str]):
        tasks = [self._check_message(client, h) for h in batch]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _record_results(self, batch: List[str], results) -> None:
        s = self.state
        for stream_hash, result in zip(batch, results):
            s["checked"] += 1
            if result is True:
                s["alive"] += 1
            elif result is False:
                s["dead"] += 1
                title = None
                try:
                    title = await self._db.get_title_by_stream_id(stream_hash)
                except Exception:
                    pass
                s["dead_entries"].append({"id": stream_hash, "title": title or "Unknown"})
            else:
                s["errors"] += 1
        elapsed = max(1, int(_now() - s["started_at"]))
        s["speed"] = s["checked"] // elapsed

    #----- ── Worker ──────────────────────────────────────────────────────────────────
    async def _run(self, client) -> None:
        db = self._db
        s = self.state
        try:
            for i in range(1, db.current_db_index + 1):
                storage = db.dbs.get(f"storage_{i}")
                if storage is None:
                    continue

                #----- Movies
                last_id = None
                while not self._cancel:
                    query = {"_id": {"$gt": last_id}} if last_id else {}
                    docs = await storage["movie"].find(query).sort("_id", 1) \
                        .limit(DBCHECK_PAGE_SIZE).to_list(length=DBCHECK_PAGE_SIZE)
                    if not docs:
                        break
                    for movie in docs:
                        last_id = movie["_id"]
                        stream_ids = [q.get("id") for q in movie.get("telegram", []) if q.get("id")]
                        for x in range(0, len(stream_ids), DBCHECK_CONCURRENCY):
                            if self._cancel:
                                break
                            batch = stream_ids[x:x + DBCHECK_CONCURRENCY]
                            results = await self._process_batch(client, batch)
                            await self._record_results(batch, results)
                            await asyncio.sleep(DBCHECK_BATCH_DELAY)

                #----- TV
                last_id = None
                while not self._cancel:
                    query = {"_id": {"$gt": last_id}} if last_id else {}
                    docs = await storage["tv"].find(query).sort("_id", 1) \
                        .limit(DBCHECK_PAGE_SIZE).to_list(length=DBCHECK_PAGE_SIZE)
                    if not docs:
                        break
                    for show in docs:
                        last_id = show["_id"]
                        stream_ids = []
                        for season in show.get("seasons", []):
                            for episode in season.get("episodes", []):
                                for q in episode.get("telegram", []):
                                    if q.get("id"):
                                        stream_ids.append(q["id"])
                        for x in range(0, len(stream_ids), DBCHECK_CONCURRENCY):
                            if self._cancel:
                                break
                            batch = stream_ids[x:x + DBCHECK_CONCURRENCY]
                            results = await self._process_batch(client, batch)
                            await self._record_results(batch, results)
                            await asyncio.sleep(DBCHECK_BATCH_DELAY)

            s["status"] = "cancelled" if self._cancel else "completed"
            s["finished_at"] = _now()
            LOGGER.info(f"[DbCheck] {s['status']} — checked {s['checked']}, dead {s['dead']}")
        except asyncio.CancelledError:
            s["status"] = "cancelled"
            s["finished_at"] = _now()
            raise
        except Exception as e:
            s["status"] = "error"
            s["error"] = str(e)
            s["finished_at"] = _now()
            LOGGER.error(f"[DbCheck] Error: {e}")

    #----- ── Purge ────────────────────────────────────────────────────────────────────
    async def purge(self, stream_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        #----- Delete the given dead stream entries (defaults to the last check's); returns count purged
        db = self._db
        if stream_ids is None:
            stream_ids = [d["id"] for d in self.state.get("dead_entries", [])]
        stream_ids = [h for h in stream_ids if h]
        if not stream_ids:
            return {"ok": False, "message": "No dead links to purge.", "purged": 0}

        purged = 0
        for x in range(0, len(stream_ids), DBCHECK_CONCURRENCY):
            batch = stream_ids[x:x + DBCHECK_CONCURRENCY]
            results = await asyncio.gather(
                *[db.delete_media_by_stream_id(h) for h in batch],
                return_exceptions=True,
            )
            purged += sum(1 for r in results if r is True)

        #----- Drop purged ids from the in-memory dead list
        purged_set = set(stream_ids)
        self.state["dead_entries"] = [
            d for d in self.state.get("dead_entries", []) if d["id"] not in purged_set
        ]
        self.state["purged"] = self.state.get("purged", 0) + purged
        return {"ok": True, "message": f"Purged {purged} dead entr{'y' if purged == 1 else 'ies'}.",
                "purged": purged}


class DuplicateManager:
    def __init__(self) -> None:
        self._db = None
        self._task: Optional[asyncio.Task] = None
        self._purge_task: Optional[asyncio.Task] = None
        self._cancel = False
        self._lock = asyncio.Lock()
        self.state: Dict[str, Any] = self._blank_state()

    @staticmethod
    def _blank_state() -> Dict[str, Any]:
        return {
            "status": "idle",
            "scanned": 0,
            "groups": [],
            "duplicate_count": 0,
            "purged": 0,
            "started_at": 0.0,
            "finished_at": 0.0,
            "error": None,
            "purge_status": "idle",
            "purge_total": 0,
            "purge_done": 0,
            "purge_started_at": 0.0,
            "purge_finished_at": 0.0,
        }

    def bind_db(self, db) -> None:
        self._db = db

    def get_status(self) -> Dict[str, Any]:
        s = self.state
        elapsed = 0.0
        if s["started_at"]:
            end = s["finished_at"] or _now()
            elapsed = max(0.0, end - s["started_at"])

        #----- Cleanup (purge) progress + ETA
        p_total = int(s.get("purge_total", 0) or 0)
        p_done = int(s.get("purge_done", 0) or 0)
        p_status = s.get("purge_status", "idle")
        p_elapsed = 0.0
        if s.get("purge_started_at"):
            p_end = s.get("purge_finished_at") or _now()
            p_elapsed = max(0.0, p_end - s["purge_started_at"])
        p_progress = round(p_done / p_total * 100) if p_total else 0
        p_eta = 0
        if p_status == "running" and p_done and p_elapsed > 0:
            rate = p_done / p_elapsed
            if rate > 0:
                p_eta = int(max(0, (p_total - p_done)) / rate)

        return {
            "status": s["status"],
            "is_running": s["status"] == "running",
            "scanned": s["scanned"],
            "group_count": len(s["groups"]),
            "duplicate_count": s["duplicate_count"],
            "purged": s["purged"],
            "groups": list(s["groups"]),
            "elapsed": _fmt_elapsed(elapsed),
            "elapsed_seconds": int(elapsed),
            "error": s["error"],
            "purge_status": p_status,
            "purge_running": p_status == "running",
            "purge_total": p_total,
            "purge_done": p_done,
            "purge_progress": p_progress,
            "purge_elapsed": _fmt_elapsed(p_elapsed),
            "purge_eta": _fmt_elapsed(p_eta) if p_eta else "—",
        }

    async def start(self) -> Dict[str, Any]:
        async with self._lock:
            if self.state["status"] == "running":
                return {"ok": False, "message": "A duplicate scan is already running."}
            if self.state.get("purge_status") == "running":
                return {"ok": False, "message": "A cleanup is currently running."}
            self.state = self._blank_state()
            self.state["status"] = "running"
            self.state["started_at"] = _now()
            self._cancel = False
            self._task = asyncio.create_task(self._run())
            return {"ok": True, "message": "Duplicate scan started.", "status": self.get_status()}

    async def cancel(self) -> Dict[str, Any]:
        if self.state["status"] != "running":
            return {"ok": False, "message": "No duplicate scan is currently running."}
        self._cancel = True
        return {"ok": True, "message": "Stop requested."}

    #----- Group a telegram list by (quality, name, size); record groups with 2+ entries
    def _collect(self, qualities: List[dict], label: str, media_type: str, gid: int) -> int:
        buckets: Dict[tuple, List[dict]] = {}
        for q in qualities:
            if not q.get("id"):
                continue
            buckets.setdefault(self._db._dup_key(q), []).append(q)
        for items in buckets.values():
            if len(items) < 2:
                continue
            gid += 1
            self.state["groups"].append({
                "group_id": gid,
                "title": label,
                "quality": items[0].get("quality"),
                "media_type": media_type,
                "entries": [
                    {"id": it["id"], "name": it.get("name"), "size": it.get("size")}
                    for it in items
                ],
            })
            self.state["duplicate_count"] += len(items) - 1
        return gid

    async def _run(self) -> None:
        db = self._db
        s = self.state
        try:
            gid = 0
            for i in range(1, db.current_db_index + 1):
                storage = db.dbs.get(f"storage_{i}")
                if storage is None:
                    continue

                async for movie in storage["movie"].find({}):
                    if self._cancel:
                        break
                    s["scanned"] += 1
                    year = movie.get("release_year")
                    label = f"{movie.get('title') or 'Unknown'}{f' ({year})' if year else ''}"
                    gid = self._collect(movie.get("telegram", []), label, "movie", gid)

                async for show in storage["tv"].find({}):
                    if self._cancel:
                        break
                    s["scanned"] += 1
                    title = show.get("title") or "Unknown"
                    for season in show.get("seasons", []):
                        for ep in season.get("episodes", []):
                            label = f"{title} S{season.get('season_number', 0):02d}E{ep.get('episode_number', 0):02d}"
                            gid = self._collect(ep.get("telegram", []), label, "tv", gid)

            s["status"] = "cancelled" if self._cancel else "completed"
            s["finished_at"] = _now()
            LOGGER.info(f"[Duplicates] {s['status']} — {len(s['groups'])} group(s), {s['duplicate_count']} redundant")
        except asyncio.CancelledError:
            s["status"] = "cancelled"
            s["finished_at"] = _now()
            raise
        except Exception as e:
            s["status"] = "error"
            s["error"] = str(e)
            s["finished_at"] = _now()
            LOGGER.error(f"[Duplicates] Error: {e}")

    #----- Delete duplicates: explicit ids, or (delete_all) keep the newest per group.
    #----- Runs in the background so the UI can poll deletion progress.
    async def purge(self, stream_ids: Optional[List[str]] = None, delete_all: bool = False) -> Dict[str, Any]:
        async with self._lock:
            if self.state.get("purge_status") == "running":
                return {"ok": False, "message": "A cleanup is already running."}

            ids: List[str] = []
            if delete_all:
                for g in self.state.get("groups", []):
                    ids.extend(e["id"] for e in g.get("entries", [])[:-1])
            elif stream_ids:
                ids = list(stream_ids)
            ids = [h for h in ids if h]
            if not ids:
                return {"ok": False, "message": "No duplicates selected to remove.", "purged": 0}

            self.state["purge_status"] = "running"
            self.state["purge_total"] = len(ids)
            self.state["purge_done"] = 0
            self.state["purge_started_at"] = _now()
            self.state["purge_finished_at"] = 0.0
            self._purge_task = asyncio.create_task(self._run_purge(ids))
            return {"ok": True, "message": f"Removing {len(ids)} duplicate(s)…",
                    "total": len(ids), "status": self.get_status()}

    async def _run_purge(self, ids: List[str]) -> None:
        db = self._db
        s = self.state
        purged = 0
        try:
            for h in ids:
                try:
                    if await db.delete_media_by_stream_id(h, delete_file=True):
                        purged += 1
                except Exception as e:
                    LOGGER.error(f"[Duplicates] purge failed for {h}: {e}")
                s["purge_done"] += 1

            purged_set = set(ids)
            new_groups = []
            for g in s.get("groups", []):
                remaining = [e for e in g.get("entries", []) if e["id"] not in purged_set]
                if len(remaining) >= 2:
                    g["entries"] = remaining
                    new_groups.append(g)
            s["groups"] = new_groups
            s["duplicate_count"] = sum(len(g["entries"]) - 1 for g in new_groups)
            s["purged"] = s.get("purged", 0) + purged
            s["purge_status"] = "completed"
            LOGGER.info(f"[Duplicates] cleanup completed — removed {purged}")
        except Exception as e:
            s["purge_status"] = "error"
            s["error"] = str(e)
            LOGGER.error(f"[Duplicates] cleanup error: {e}")
        finally:
            s["purge_finished_at"] = _now()


#----- ── Singletons ──────────────────────────────────────────────────────────────
scan_manager = ScanManager()
dbcheck_manager = DbCheckManager()
duplicate_manager = DuplicateManager()
