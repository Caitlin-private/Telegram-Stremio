"""Review theatrical movie copies only when a separate digital copy is indexed."""
import asyncio
import re
import uuid
from datetime import datetime, timezone

THEATRICAL = re.compile(r"(?<![a-z0-9])(?:CAMRip|TELESYNC|HDTC|HDTS)(?![a-z0-9])", re.I)
DIGITAL = re.compile(r"(?<![a-z0-9])(?:WEBRip|AMZN|BluRay|WEB|HDRip)(?![a-z0-9])", re.I)


def release_flags(quality, title=""):
    name = str(quality.get("name") or "")
    # Don't mistake the movie's own title (e.g. 'Web') for a release tag.
    tokens = re.findall(r"\w+", str(title))
    if tokens:
        prefix = r"^[\W_]*" + r"[\W_]+".join(re.escape(t) for t in tokens) + r"(?=$|[\W_])"
        name = re.sub(prefix, "", name, count=1, flags=re.I)
    theatrical = bool(THEATRICAL.search(name))
    return theatrical, bool(DIGITAL.search(name)) and not theatrical


def eligible_files(movie):
    files = movie.get("telegram") or []
    title = movie.get("title") or ""
    digital_ids = {q.get("id") for q in files if q.get("id") and release_flags(q, title)[1]}
    if not digital_ids:
        return [], []
    candidates = [q for q in files if q.get("id") and q["id"] not in digital_ids and release_flags(q, title)[0]]
    replacements = [q for q in files if q.get("id") in digital_ids and release_flags(q, title)[1]]
    return candidates, replacements


class DigitalReleaseCleaner:
    def __init__(self, db):
        self.db = db
        self.lock = asyncio.Lock()
        self.task = None
        self.cancel_requested = False
        self.entries = {}
        self.state = self.blank_state()

    @staticmethod
    def blank_state():
        return dict(status="idle", scanned=0, removed=0, skipped=0, failed=0,
                    processed=0, total=0, error=None, scan_id="")

    def status(self, offset=0):
        entries = list(self.entries.values())
        return {**self.state, "count": len(entries), "offset": offset,
                "entries": [{k: v for k, v in e.items() if k not in ("document_id", "storage")}
                            for e in entries[offset:offset + 100]]}

    async def start(self):
        async with self.lock:
            if self.state["status"] in ("running", "deleting"):
                raise ValueError("A cleaner operation is already running.")
            self.entries = {}
            self.state = self.blank_state()
            self.state.update(status="running", scan_id=uuid.uuid4().hex)
            self.cancel_requested = False
            self.task = asyncio.create_task(self._scan())

    async def cancel(self):
        self.cancel_requested = True

    async def _scan(self):
        try:
            for index in range(1, self.db.current_db_index + 1):
                storage_key = f"storage_{index}"
                storage = self.db.dbs.get(storage_key)
                if storage is None:
                    continue
                async for movie in storage["movie"].find({}, {"title": 1, "release_year": 1, "telegram": 1}):
                    if self.cancel_requested:
                        self.state["status"] = "cancelled"
                        return
                    self.state["scanned"] += 1
                    candidates, replacements = eligible_files(movie)
                    for q in candidates:
                        key = uuid.uuid4().hex
                        self.entries[key] = dict(key=key, id=q["id"], name=q.get("name") or "",
                                                 title=movie.get("title") or "Unknown", year=movie.get("release_year"),
                                                 replacements=[r.get("name") or "" for r in replacements],
                                                 storage=storage_key, document_id=movie["_id"])
                    await asyncio.sleep(0)
            self.state["status"] = "completed"
        except Exception as exc:
            self.state.update(status="error", error=str(exc))

    async def purge(self, scan_id, keys=None, delete_all=False):
        async with self.lock:
            if self.state["status"] in ("running", "deleting"):
                raise ValueError("Wait for the current operation to finish.")
            if not scan_id or scan_id != self.state["scan_id"]:
                raise ValueError("The scan has changed. Refresh the cleaner results.")
            if not isinstance(delete_all, bool):
                raise ValueError("delete_all must be true or false.")
            if keys is not None and (not isinstance(keys, list) or any(not isinstance(k, str) for k in keys)):
                raise ValueError("Select valid cleaner entries.")
            selected = list(self.entries) if delete_all else list(dict.fromkeys(keys or []))
            if not selected or any(k not in self.entries for k in selected):
                raise ValueError("No valid cleaner entries selected. Refresh and try again.")
            self.state.update(status="deleting", processed=0, total=len(selected), skipped=0, failed=0, error=None)
            self.task = asyncio.create_task(self._purge(selected))

    async def _purge(self, keys):
        for key in keys:
            entry = self.entries[key]
            try:
                collection = self.db.dbs[entry["storage"]]["movie"]
                movie = await collection.find_one({"_id": entry["document_id"]})
                candidates, _ = eligible_files(movie or {})
                candidate = next((q for q in candidates if q["id"] == entry["id"]), None)
                if candidate is None:
                    self.state["skipped"] += 1
                    del self.entries[key]
                    continue
                # Compare-and-swap: a concurrently removed digital copy or newly
                # uploaded file must never be lost through a stale whole-doc write.
                result = await collection.update_one(
                    {"_id": movie["_id"], "telegram": movie["telegram"]},
                    {"$pull": {"telegram": {"id": entry["id"]}}, "$set": {"updated_on": datetime.now(timezone.utc)}})
                if not result.modified_count:
                    self.state["skipped"] += 1
                    continue
                self.state["removed"] += 1
                await self.db._queue_quality_deletion(candidate)
                del self.entries[key]
            except Exception as exc:
                self.state["failed"] += 1
                self.state["error"] = str(exc)
            finally:
                self.state["processed"] += 1
            await asyncio.sleep(0)
        self.state["status"] = "error" if self.state["failed"] else "completed"
