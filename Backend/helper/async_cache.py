"""Bounded metadata caches with cancellation-safe request coalescing."""
import asyncio
import time
from collections import OrderedDict
from collections.abc import MutableMapping


class TTLCache(MutableMapping):
    def __init__(self, maxsize=2048, ttl=3600, negative_ttl=30):
        self.maxsize = maxsize
        self.ttl = ttl
        self.negative_ttl = negative_ttl
        self._entries = OrderedDict()

    def __getitem__(self, key):
        expires, value = self._entries[key]
        if expires <= time.monotonic():
            del self._entries[key]
            raise KeyError(key)
        self._entries.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        ttl = self.ttl if value else self.negative_ttl
        self._entries[key] = (time.monotonic() + ttl, value)
        self._entries.move_to_end(key)
        while len(self._entries) > self.maxsize:
            self._entries.popitem(last=False)

    def __delitem__(self, key):
        del self._entries[key]

    def __iter__(self):
        return iter(self._entries)

    def __len__(self):
        return len(self._entries)


_INFLIGHT = {}


async def cached_call(store, key, ns, producer):
    try:
        return store[key]
    except KeyError:
        pass
    flight_key = (id(store), ns, key)
    future = _INFLIGHT.get(flight_key)
    if future is not None:
        # Cancelling one consumer must not cancel another consumer's result.
        return await asyncio.shield(future)
    future = asyncio.get_running_loop().create_future()
    _INFLIGHT[flight_key] = future
    try:
        result = await producer()
        store[key] = result
        future.set_result(result)
        return result
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            future.cancel()
        else:
            future.set_exception(exc)
            future.exception()  # Consume when there are no other waiters.
        raise
    finally:
        _INFLIGHT.pop(flight_key, None)
