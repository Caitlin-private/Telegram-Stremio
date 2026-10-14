"""Scan-local HTTP pooling and per-host backpressure; no persistent settings."""
import asyncio
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import httpx


quick_scan_http = ContextVar('quick_scan_http', default=None)


def retry_after_seconds(value, default=2.0):
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return default


class _HostGate:
    def __init__(self):
        self.slots = asyncio.Semaphore(12)
        self.lock = asyncio.Lock()
        self.cooldown = 0.0


class ScanHTTPClient(httpx.AsyncClient):
    """Pool connections; back off per host only when it returns HTTP 429.

    The connection pool bounds total HTTP concurrency at twelve.
    Healthy providers have no artificial inter-request delay.
    A throttled request stays pending until retry or Stop, rather than becoming
    a metadata miss (which Quick Scan could interpret as a rejected file).
    """
    def __init__(self, stopped, metrics):
        super().__init__(timeout=20.0, follow_redirects=True,
                         limits=httpx.Limits(max_connections=12, max_keepalive_connections=12))
        self.stopped = stopped
        self.metrics = metrics
        self.hosts = {}
        self.tmdb_clients = {}

    async def wait(self, delay):
        if self.stopped.is_set():
            raise asyncio.CancelledError()
        if delay <= 0:
            return
        started = time.monotonic()
        try:
            await asyncio.wait_for(self.stopped.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return
        finally:
            self.metrics['api_wait_seconds'] += time.monotonic() - started
        raise asyncio.CancelledError()

    async def request(self, method, url, **kwargs):
        gate = self.hosts.setdefault(urlsplit(str(url)).netloc, _HostGate())
        attempt = 0
        while True:
            async with gate.slots:
                async with gate.lock:
                    while True:
                        await self.wait(gate.cooldown - time.monotonic())
                        if time.monotonic() >= gate.cooldown:
                            break
                self.metrics['api_requests'] += 1
                response = await super().request(method, url, **kwargs)
                if response.status_code != 429:
                    return response
                attempt += 1
                delay = retry_after_seconds(response.headers.get('Retry-After'), min(60, 2 ** min(attempt, 6)))
                gate.cooldown = max(gate.cooldown, time.monotonic() + max(1.0, delay))
                self.metrics['api_throttles'] += 1
                await response.aclose()

    def tmdb_client(self, key):
        from themoviedb import aioTMDb
        if key not in self.tmdb_clients:
            self.tmdb_clients[key] = aioTMDb(key=key, language='en-US', region='US', session=_TMDBSession(self))
        return self.tmdb_clients[key]


class _TMDBResponse:
    def __init__(self, response):
        self.response = response

    async def json(self):
        return self.response.json()


class _TMDBSession:
    """The session.request / async response.json interface used by aioTMDb."""
    def __init__(self, client):
        self.client = client

    async def request(self, method, url, **kwargs):
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return _TMDBResponse(response)
