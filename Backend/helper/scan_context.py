"""Task-local scan controls; inherited by replacement-cleanup tasks."""
from contextlib import asynccontextmanager
from contextvars import ContextVar


preserve_source_media = ContextVar('preserve_source_media', default=False)
processing_lease = ContextVar('processing_lease', default=None)


class ProcessingLease:
    """Release scarce processing capacity while a bot waits for Telegram.

    The task and its batch stay alive, so a parked operation resumes in place
    and the existing contiguous checkpoint cannot jump over it.
    """
    def __init__(self, semaphore):
        self.semaphore = semaphore
        self.held = False

    async def __aenter__(self):
        await self.semaphore.acquire()
        self.held = True
        return self

    async def __aexit__(self, *_):
        if self.held:
            self.held = False
            self.semaphore.release()

    @asynccontextmanager
    async def parked(self):
        held = self.held
        if held:
            self.held = False
            self.semaphore.release()
        # On cancellation/error the caller exits its lease; do not reacquire.
        yield
        if held:
            await self.semaphore.acquire()
            self.held = True


@asynccontextmanager
async def park_processing():
    lease = processing_lease.get()
    if lease is None:
        yield
    else:
        async with lease.parked():
            yield
