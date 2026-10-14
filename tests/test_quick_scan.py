"""Offline regression tests. No MongoDB, Telegram or provider connection is used."""
import asyncio
import os
import unittest
from collections import Counter
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault('DATABASE', 'mongodb://localhost/tracking,mongodb://localhost/storage')

from pyrogram.errors import FloodWait
from Backend.helper import scan_manager as scans
from Backend.helper.scan_manager import ScanManager
from Backend.helper.scan_http import ScanHTTPClient, quick_scan_http, retry_after_seconds
from Backend.helper.metadata.common import cached_call
import httpx


class Worker:
    def __init__(self, number):
        self.me = SimpleNamespace(id=number)
        self.name = str(number)
        self.fetches = []

    async def get_chat(self, chat):
        return SimpleNamespace(title='Test')

    async def get_messages(self, chat, ids):
        self.fetches.append(list(ids))
        await asyncio.sleep(0)
        return [SimpleNamespace(id=i, empty=False, video=True, document=None) for i in ids]


class ScanTests(unittest.IsolatedAsyncioTestCase):
    def manager(self, end=80):
        manager = ScanManager()
        manager.state.update(mode='quick', end_id=end, pause_for_playback=False)
        manager._probe_last_message_id = AsyncMock(return_value=end + 1)
        manager._persist = AsyncMock()
        manager._announce_import = AsyncMock()
        manager._flush_scan_catalogs = AsyncMock()
        return manager

    async def run_scan(self, manager, workers):
        with patch('Backend.helper.scan_resources.select_scan_clients', AsyncMock(return_value=workers)), \
             patch.object(scans, 'SCAN_BATCH_SIZE', 16), patch.object(scans, 'SCAN_BATCH_DELAY', 0):
            return await asyncio.wait_for(manager._scan_channel(workers[0], -100123, '-100123'), 5)

    async def test_all_workers_share_twelve_slots_and_disjoint_ranges(self):
        manager = self.manager(320)
        workers = [Worker(i) for i in range(1, 6)]
        active = maximum = 0
        seen = []
        async def process(worker, message, chat):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(.001)
            seen.append(message.id)
            active -= 1
        manager._process_message = process
        self.assertTrue(await self.run_scan(manager, workers))
        self.assertEqual(maximum, 12)  # Previously five workers yielded five slots.
        self.assertEqual(Counter(seen), Counter(range(1, 321)))
        self.assertTrue(all(worker.fetches for worker in workers))
        self.assertEqual(manager.state['cursors']['-100123'], 321)
        manager._flush_scan_catalogs.assert_awaited_once()

    async def test_flooded_batch_parks_while_healthy_bot_indexes_and_then_resumes(self):
        manager = self.manager(96)
        workers = [Worker(1), Worker(2)]
        flooded = asyncio.Event()
        healthy_done = asyncio.Event()
        successful = []
        raised = False
        healthy_active = healthy_maximum = 0
        async def process(worker, message, chat):
            nonlocal raised, healthy_active, healthy_maximum
            if worker is workers[0] and not raised:
                raised = True
                flooded.set()
                raise FloodWait(0)  # Real retry window: two seconds with safety margin.
            if worker is workers[1]:
                healthy_active += 1
                healthy_maximum = max(healthy_maximum, healthy_active)
                await asyncio.sleep(.01)
                healthy_active -= 1
                healthy_done.set()
            successful.append(message.id)
        manager._process_message = process
        task = asyncio.create_task(self.run_scan(manager, workers))
        await flooded.wait()
        await asyncio.wait_for(healthy_done.wait(), .8)
        await asyncio.sleep(.1)
        self.assertEqual(healthy_maximum, 12)
        self.assertNotIn(1, successful)
        self.assertLessEqual(manager.state['cursors'].get('-100123', 1), 1)
        self.assertTrue(manager.state['import_report']['completed_ranges'])
        self.assertTrue(await task)
        self.assertEqual(Counter(successful), Counter(range(1, 97)))
        self.assertEqual(manager.state['cursors']['-100123'], 97)

    async def test_quick_scan_keeps_resolution_and_metadata_rejects(self):
        manager = self.manager()
        manager._stream_id_exists = AsyncMock(return_value=False)
        worker = Worker(1)
        worker.delete_messages = AsyncMock()
        msg = SimpleNamespace(id=1, caption='Example 2020 1080p',
                              document=None, video=SimpleNamespace(file_name='Example.mkv', file_size=100))
        with patch.object(scans, 'is_skip_channel', return_value=False), \
             patch('Backend.helper.resolution_policy.rejection_reason', return_value='resolution rejected'), \
             patch.object(scans, 'route_to_skip_channel', AsyncMock()) as route:
            await manager._process_message(worker, msg, -100123)
            route.assert_not_awaited()
        with patch.object(scans, 'is_skip_channel', return_value=False), \
             patch('Backend.helper.resolution_policy.rejection_reason', return_value=None), \
             patch.object(scans, 'metadata', AsyncMock(return_value=None)):
            await manager._process_message(worker, msg, -100123)
        worker.delete_messages.assert_not_awaited()
        self.assertEqual(manager.state['counters']['skipped_resolution'], 1)
        self.assertEqual(manager.state['counters']['skipped_meta'], 1)

    async def test_replacement_cleanup_inherits_keep_media_context(self):
        from Backend.helper import task_manager, database
        from Backend.helper.scan_context import preserve_source_media
        tasks = []
        real_create_task = asyncio.create_task
        def capture(coro):
            task = real_create_task(coro)
            tasks.append(task)
            return task
        db = database.Database()
        with patch.object(database, 'create_task', capture), \
             patch.object(database, 'decode_string', AsyncMock(return_value={'chat_id': 123, 'msg_id': 1})), \
             patch.object(task_manager, '_delete_chunk', AsyncMock(return_value=[])) as delete:
            token = preserve_source_media.set(True)
            try:
                await db._queue_quality_deletion({'id': 'old-stream'})
                await db._delete_split_part({'chat_id': 123, 'msg_id': 2})
            finally:
                preserve_source_media.reset(token)
            await asyncio.gather(*tasks)
            delete.assert_not_awaited()
            await task_manager.delete_message(-100123, 3)
            delete.assert_awaited_once()  # Normal/manual deletion remains available.

    async def test_cancelled_parked_lease_does_not_leak_slots(self):
        from Backend.helper.scan_context import ProcessingLease
        sem = asyncio.Semaphore(1)
        parked = asyncio.Event()
        async def work():
            async with ProcessingLease(sem) as lease:
                async with lease.parked():
                    parked.set()
                    await asyncio.Event().wait()
        task = asyncio.create_task(work())
        await parked.wait()
        await asyncio.wait_for(sem.acquire(), .2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(sem._value, 0)
        sem.release()
        self.assertEqual(sem._value, 1)

    async def test_normal_scan_retains_batch_catalog_flushes(self):
        manager = self.manager(32)
        manager.state['mode'] = 'scan'
        manager._process_message = AsyncMock()
        self.assertTrue(await self.run_scan(manager, [Worker(1)]))
        self.assertEqual(manager._flush_scan_catalogs.await_count, 2)
        self.assertEqual(manager.state['cursors']['-100123'], 33)

    async def test_prefetch_is_bounded_and_cursor_does_not_cross_gap(self):
        manager = self.manager(160)
        workers = [Worker(1), Worker(2)]
        release = asyncio.Event()
        reached = asyncio.Event()
        async def process(worker, message, chat):
            if message.id == 1:
                reached.set()
                await release.wait()
            await asyncio.sleep(0)
        manager._process_message = process
        task = asyncio.create_task(self.run_scan(manager, workers))
        await reached.wait()
        await asyncio.sleep(.05)
        self.assertEqual(len(workers[0].fetches), 2)
        self.assertLessEqual(manager.state['cursors'].get('-100123', 1), 1)
        self.assertTrue(manager.state['import_report']['completed_ranges'])
        release.set()
        self.assertTrue(await task)
        self.assertEqual(manager.state['cursors']['-100123'], 161)

    async def test_stop_and_resume_skips_completed_ranges(self):
        manager = self.manager(160)
        workers = [Worker(1), Worker(2)]
        entered = asyncio.Event()
        async def process(worker, message, chat):
            if message.id == 1:
                entered.set()
                await manager._stop_event.wait()
            await asyncio.sleep(0)
        manager._process_message = process
        task = asyncio.create_task(self.run_scan(manager, workers))
        await entered.wait()
        await asyncio.sleep(.02)
        manager._cancel = True
        manager._stop_event.set()
        try:
            await task
        except scans._ScanStopped:
            pass
        completed = list(manager.state['import_report']['completed_ranges'])
        self.assertTrue(completed)
        self.assertLessEqual(manager.state['cursors'].get('-100123', 1), 1)
        manager._cancel = False
        manager._stop_event.clear()
        workers = [Worker(1), Worker(2)]
        manager._process_message = AsyncMock()
        self.assertTrue(await self.run_scan(manager, workers))
        fetched = [i for worker in workers for batch in worker.fetches for i in batch]
        for lo, hi in completed:
            self.assertTrue(set(range(lo, hi)).isdisjoint(fetched))
        self.assertEqual(manager.state['cursors']['-100123'], 161)

    async def test_fetch_failure_cancels_prefetch_without_committing_gap(self):
        manager = self.manager()
        worker = Worker(1)
        worker.get_messages = AsyncMock(side_effect=RuntimeError('fetch failed'))
        manager._process_message = AsyncMock()
        with self.assertRaisesRegex(RuntimeError, 'fetch failed'):
            await self.run_scan(manager, [worker])
        self.assertFalse(manager.state['cursors'])

    async def test_flood_wait_is_per_bot_and_stop_interrupts_it(self):
        manager = self.manager()
        worker1, worker2 = Worker(1), Worker(2)
        operation = AsyncMock(side_effect=FloodWait(30))
        blocked = asyncio.create_task(manager._telegram_call(operation, scan_client=worker1))
        await asyncio.sleep(.01)
        other = AsyncMock(return_value='ok')
        self.assertEqual(await asyncio.wait_for(manager._telegram_call(other, scan_client=worker2), 1), 'ok')
        manager._cancel = True
        manager._stop_event.set()
        with self.assertRaises(scans._ScanStopped):
            await asyncio.wait_for(blocked, 1)
        self.assertEqual(operation.await_count, 1)

    async def test_pending_catalog_entry_survives_stop_and_requeue(self):
        from Backend.helper import auto_catalog
        manager = ScanManager()
        manager._persist = AsyncMock()
        original = {'tmdb_id': 1, 'media_type': 'movie'}
        manager.state['catalog_pending'] = {'movie:1': original}
        async def interrupted(*args, **kwargs):
            raise asyncio.CancelledError()
        with patch.object(auto_catalog, 'sync_single_media', interrupted):
            with self.assertRaises(asyncio.CancelledError):
                await manager._flush_scan_catalogs()
        self.assertIs(manager.state['catalog_pending']['movie:1'], original)
        async def requeue(*args, **kwargs):
            manager.state['catalog_pending']['movie:1'] = dict(original)
            return {'ok': True}
        with patch.object(auto_catalog, 'sync_single_media', requeue):
            await manager._flush_scan_catalogs()
        self.assertIn('movie:1', manager.state['catalog_pending'])
        with patch.object(auto_catalog, 'sync_single_media', AsyncMock(return_value={'ok': False, 'reason': 'sync_running'})):
            await manager._flush_scan_catalogs()
        self.assertIn('movie:1', manager.state['catalog_pending'])

    async def test_auto_selects_more_than_eight_eligible_workers(self):
        from Backend.helper.scan_resources import select_scan_clients
        from Backend.pyrofork import bot
        from Backend.helper.settings_manager import SettingsManager
        workers = [Worker(i) for i in range(1, 13)]
        for worker in workers:
            worker.is_connected = True
            worker.get_chat_member = AsyncMock(return_value=SimpleNamespace(status='administrator', privileges=SimpleNamespace(can_delete_messages=True)))
        main = Worker(100)
        main.get_chat_member = AsyncMock()
        async def call(operation, *args, **kwargs):
            kwargs.pop('scan_client', None)
            return await operation(*args, **kwargs)
        with patch.object(bot, 'multi_clients', dict(enumerate(workers))), \
             patch.object(SettingsManager, 'current', return_value=SimpleNamespace(skip_channel='')):
            self.assertEqual(await select_scan_clients(main, 1, 0, True, call), workers)
            self.assertEqual(len(await select_scan_clients(main, 1, 0, False, call)), 8)
            self.assertEqual(len(await select_scan_clients(main, 1, 3, True, call)), 3)
        main.get_chat_member.assert_not_awaited()


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Test HTTP is mocked; ignore the runner's optional SOCKS proxy.
        proxy_env = {k: '' for k in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy')}
        proxy_patch = patch.dict(os.environ, proxy_env)
        proxy_patch.start()
        self.addCleanup(proxy_patch.stop)

    def metrics(self):
        return {'api_wait_seconds': 0, 'api_requests': 0, 'api_throttles': 0}

    async def test_provider_throttle_does_not_block_another_host_and_stop_is_prompt(self):
        stop = asyncio.Event()
        metrics = self.metrics()
        throttled = asyncio.Event()
        async def request(client, method, url, **kwargs):
            if 'slow.example' in url:
                throttled.set()
                return httpx.Response(429, headers={'Retry-After': '60'}, request=httpx.Request(method, url))
            return httpx.Response(200, json={'ok': True}, request=httpx.Request(method, url))
        async with ScanHTTPClient(stop, metrics) as client:
            with patch.object(httpx.AsyncClient, 'request', request):
                task = asyncio.create_task(client.get('https://slow.example/item'))
                await throttled.wait()
                response = await asyncio.wait_for(client.get('https://other.example/item'), 1)
                self.assertEqual(response.status_code, 200)
                stop.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 1)
        self.assertEqual(metrics['api_throttles'], 1)
        self.assertEqual(metrics['api_requests'], 2)

    async def test_healthy_provider_has_twelve_slots_and_no_artificial_wait(self):
        active = maximum = 0
        all_started = asyncio.Event()
        async def request(client, method, url, **kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            if active == 12:
                all_started.set()
            await asyncio.wait_for(all_started.wait(), 1)
            active -= 1
            return httpx.Response(200, json={}, request=httpx.Request(method, url))
        metrics = self.metrics()
        async with ScanHTTPClient(asyncio.Event(), metrics) as client:
            with patch.object(httpx.AsyncClient, 'request', request):
                await asyncio.gather(*(client.get(f'https://healthy.example/{i}') for i in range(12)))
        self.assertEqual(maximum, 12)
        self.assertEqual(metrics['api_wait_seconds'], 0)
        self.assertEqual(metrics['api_throttles'], 0)

    async def test_429_retries_same_request_until_success(self):
        attempts = []
        async def request(client, method, url, **kwargs):
            attempts.append((method, url))
            return httpx.Response(429 if len(attempts) == 1 else 200,
                                  headers={'Retry-After': '0'}, json={'ok': True},
                                  request=httpx.Request(method, url))
        metrics = self.metrics()
        async with ScanHTTPClient(asyncio.Event(), metrics) as client:
            with patch.object(httpx.AsyncClient, 'request', request):
                response = await asyncio.wait_for(client.get('https://retry.example/item'), 3)
                self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, [('GET', 'https://retry.example/item')] * 2)
        self.assertEqual(metrics['api_throttles'], 1)
        self.assertGreaterEqual(metrics['api_wait_seconds'], .9)

    async def test_sdk_session_adapter_and_client_reuse(self):
        async def request(client, method, url, **kwargs):
            return httpx.Response(200, json={'id': 123, 'title': 'Test'}, request=httpx.Request(method, url))
        async with ScanHTTPClient(asyncio.Event(), self.metrics()) as client:
            with patch.object(httpx.AsyncClient, 'request', request):
                sdk = client.tmdb_client('test-key')
                self.assertIs(sdk, client.tmdb_client('test-key'))
                result = await sdk.movie(123).details()
                self.assertEqual(result.id, 123)
                self.assertEqual(result.title, 'Test')

    async def test_cancelled_cache_owner_does_not_strand_future(self):
        store = {}
        entered = asyncio.Event()
        async def produce():
            entered.set()
            await asyncio.Event().wait()
        owner = asyncio.create_task(cached_call(store, 'x', 'test', produce))
        await entered.wait()
        follower = asyncio.create_task(cached_call(store, 'x', 'test', produce))
        await asyncio.sleep(0)
        owner.cancel()
        results = await asyncio.wait_for(asyncio.gather(owner, follower, return_exceptions=True), 1)
        self.assertTrue(all(isinstance(r, asyncio.CancelledError) for r in results))
        self.assertEqual(await cached_call(store, 'x', 'test', AsyncMock(return_value=42)), 42)

    def test_retry_after(self):
        self.assertEqual(retry_after_seconds('12'), 12)
        self.assertEqual(retry_after_seconds('invalid'), 2)
        self.assertEqual(retry_after_seconds('Wed, 21 Oct 2015 07:28:00 GMT'), 0)


if __name__ == '__main__':
    unittest.main()
