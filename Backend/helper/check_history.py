"""Durable successful DB-check timestamps and elapsed times."""
from Backend.logger import LOGGER

COLLECTION = "checker_history"
DOC_ID = "db_checker"


class CheckHistory:
    def __init__(self):
        self.db = None
        self.manual = None
        self.automatic = None

    async def load(self, db):
        self.db = db
        try:
            doc = await db.dbs["tracking"][COLLECTION].find_one({"_id": DOC_ID}) or {}
            self.manual = doc.get("manual")
            self.automatic = doc.get("automatic")
        except Exception as exc:
            LOGGER.error(f"[CheckHistory] Failed to load history: {exc}")

    async def save_success(self, mode, finished_at, duration_seconds):
        if mode not in ("manual", "automatic"):
            raise ValueError("Invalid check history mode")
        value = {"finished_at": finished_at, "duration_seconds": round(max(0, duration_seconds), 2)}
        if self.db is None:
            LOGGER.error("[CheckHistory] Database not initialized; cannot save history")
            return
        try:
            await self.db.dbs["tracking"][COLLECTION].update_one(
                {"_id": DOC_ID}, {"$set": {mode: value}}, upsert=True)
        except Exception as exc:
            LOGGER.error(f"[CheckHistory] Failed to save {mode} history: {exc}")
            return
        setattr(self, mode, value)


check_history = CheckHistory()
