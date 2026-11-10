"""Exclusive scan ownership for the application's single asyncio event loop."""
import asyncio
from functools import wraps


class ScanCoordinator:
    def __init__(self):
        self.owner = None
        self._token = None
        self._automatic_task = None

    def start_automatic(self, operation):
        """Atomically reserve the scanner before scheduling background work."""
        if self.owner is not None:
            return None
        token = self._claim("automatic")
        task = asyncio.create_task(operation())
        self._automatic_task = task
        task.add_done_callback(lambda _: self.release(token))
        return task

    def _claim(self, owner):
        self.owner = owner
        self._token = object()
        return self._token

    async def acquire(self, owner):
        if self.owner not in (None, "automatic"):
            return None
        # Reserve before awaiting cancellation: a concurrent start cannot pass.
        task = self._automatic_task
        token = self._claim(owner)
        try:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            return token
        except BaseException:
            self.release(token)
            raise

    def release(self, token):
        # An old task's completion must never release a newer reservation.
        if self._token is token:
            self.owner = self._token = self._automatic_task = None

    def exclusive_start(self, owner):
        """Hold ownership until all work/cleanup ends, even on early cancel."""
        def decorate(start):
            @wraps(start)
            async def wrapped(manager, *args, **kwargs):
                token = await self.acquire(owner)
                if token is None:
                    return {"ok": False, "message": f"A {self.owner} scan is already running. Stop it before starting another scan."}
                transferred = False
                try:
                    result = await start(manager, *args, **kwargs)
                    if result.get("ok"):
                        manager._task.add_done_callback(lambda _: self.release(token))
                        transferred = True
                    return result
                finally:
                    if not transferred:
                        self.release(token)
            return wrapped
        return decorate


scan_coordinator = ScanCoordinator()
