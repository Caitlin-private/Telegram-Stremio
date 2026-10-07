"""Process-local ingestion metrics; never imply that Telegram history is queued."""
from contextlib import contextmanager
from functools import wraps


class IngestionStatus:
    def __init__(self):
        self.handlers = 0
        self.writing = 0
        self.write_errors = 0
        self.queue = None

    def track(self, function):
        @wraps(function)
        async def wrapped(*args, **kwargs):
            self.handlers += 1
            try:
                return await function(*args, **kwargs)
            finally:
                self.handlers -= 1
        return wrapped

    @contextmanager
    def write(self):
        self.writing += 1
        try:
            yield
        except Exception:
            self.write_errors += 1
            raise
        finally:
            self.writing -= 1

    def snapshot(self, client=None):
        queued = self.queue.qsize() if self.queue is not None else 0
        updates = getattr(getattr(client, 'dispatcher', None), 'updates_queue', None)
        return {
            'pending_media': self.handlers + queued + self.writing,
            'media_handlers': self.handlers,
            'queued_media': queued,
            'writing_media': self.writing,
            'ingestion_write_errors': self.write_errors,
            # Unclassified updates include edits and non-media events.
            'telegram_updates_waiting': updates.qsize() if updates is not None else None,
        }


ingestion_status = IngestionStatus()
