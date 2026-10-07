"""Mongo-backed channel inbox with incremental Telegram message discovery.

Discovery advances only after inbox writes commit. Jobs are removed only after
processing succeeds; replay after a crash is expected and must be idempotent.
"""
import asyncio
from time import time

from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER


class DurableIngestion:
    def __init__(self):
        self.db = None
        self.client = None
        self.task = None
        self.lock = asyncio.Lock()
        self.control_lock = asyncio.Lock()
        self.discovery_lock = asyncio.Lock()
        self.active = None
        self.paused = False
        self.disabled_channels = set()
        self.refresh_heads = True
        self.error = None
        self.last_head_check = 0

    def bind(self, db):
        self.db = db
        self.state = db.dbs['tracking']['ingestion_state']
        self.channels = db.dbs['tracking']['ingestion_channels']
        self.jobs = db.dbs['tracking']['ingestion_jobs']

    async def load(self, db):
        self.bind(db)
        doc = await self.state.find_one({'_id': 'control'}) or {}
        self.paused = bool(doc.get('paused', False))
        self.disabled_channels = set(doc.get('disabled_channels', []))

    def authorized(self, chat_id):
        return str(chat_id) in SettingsManager.current().auth_channels

    def enabled(self, chat_id):
        return self.authorized(chat_id) and str(chat_id) not in self.disabled_channels

    async def set_channel_enabled(self, chat_id, enabled):
        chat_id = str(chat_id)
        if not self.authorized(chat_id):
            raise ValueError('Select an AUTH channel.')
        if not isinstance(enabled, bool):
            raise ValueError('enabled must be true or false.')
        # Snapshot the current head at every transition.  This switch is a
        # permanent live-ingestion boundary: messages already in the channel,
        # including messages posted while disabled, must not be replayed when
        # the channel is enabled again.
        async with self.discovery_lock, self.control_lock, self.lock:
            latest = await self.head(chat_id)
            await self.channels.update_one(
                {'_id': chat_id},
                {'$set': {'discovered_id': latest, 'target_id': latest}},
                upsert=True,
            )
            # Anything waiting from before this boundary is intentionally
            # discarded.  It is no longer a live event for this channel.
            await self.jobs.delete_many({'chat_id': chat_id})
            disabled = self.disabled_channels.copy()
            if enabled:
                disabled.discard(chat_id)
            else:
                disabled.add(chat_id)
            await self.state.update_one({'_id': 'control'},
                {'$set': {'disabled_channels': sorted(disabled)}}, upsert=True)
            self.disabled_channels = disabled
            self.refresh_heads = True
        return await self.status()

    async def start(self, client, processor, edited_processor):
        self.client = client
        self.processor = processor
        self.edited_processor = edited_processor
        doc = await self.state.find_one({'_id': 'control'}) or {}
        self.paused = bool(doc.get('paused', False))
        self.disabled_channels = set(doc.get('disabled_channels', []))
        await self.jobs.create_index([('chat_id', 1), ('msg_id', 1)])
        self.task = asyncio.create_task(self.run())

    async def context(self, message):
        import Backend
        from Backend.helper.channel_auto_add import get_session, accepts_message
        capture = await get_session(self.db)
        stamp = message.date.timestamp()
        # Evaluate at upload time, not resume time: delayed files keep their session.
        if not accepts_message(capture, stamp, now=stamp):
            capture = None
        return {'capture': capture, 'manual': Backend.MANUAL_SESSION}

    async def enqueue(self, message, kind='new', discovered=False):
        chat_id = str(message.chat.id)
        if not self.authorized(chat_id):
            return
        async with self.lock:
            if not self.enabled(chat_id):
                return
            row = await self.channels.find_one({'_id': chat_id}) or {}
            if not discovered and kind == 'new' and message.id <= row.get('discovered_id', -1):
                return
            if message.video or message.document:
                job = {'chat_id': chat_id, 'msg_id': message.id, 'kind': kind,
                       'context': await self.context(message), 'attempts': 0}
                await self.jobs.update_one({'_id': f'{chat_id}:{message.id}:{kind}'},
                                           {'$setOnInsert': job}, upsert=True)
            await self.channels.update_one({'_id': chat_id},
                {'$max': {'target_id': message.id}}, upsert=True)

    async def head(self, chat_id):
        # Bot accounts cannot get chat history. Use the existing scanner's tiny
        # send/delete probe only at startup/resume, never download video bytes.
        from Backend.helper.scan_manager import scan_manager
        latest = await scan_manager._probe_last_message_id(self.client, int(chat_id))
        if latest is None:
            raise RuntimeError(f'Cannot discover the latest message in {chat_id}; check bot post/delete permissions.')
        return latest

    async def discover(self, chat_id, refresh=False):
        async with self.discovery_lock:
            if not self.enabled(chat_id):
                return
            await self._discover(chat_id, refresh)

    async def _discover(self, chat_id, refresh=False):
        row = await self.channels.find_one({'_id': chat_id}) or {}
        if refresh or 'discovered_id' not in row:
            latest = await self.head(chat_id)
            async with self.lock:
                row = await self.channels.find_one({'_id': chat_id}) or {}
                if 'discovered_id' not in row:
                    first = await self.jobs.find_one({'chat_id': chat_id}, sort=[('msg_id', 1)])
                    baseline = min(latest, first['msg_id'] - 1) if first else latest
                    await self.channels.update_one({'_id': chat_id},
                        {'$set': {'discovered_id': baseline},
                         '$max': {'target_id': latest}}, upsert=True)
                else:
                    await self.channels.update_one({'_id': chat_id}, {'$max': {'target_id': latest}})
            row = await self.channels.find_one({'_id': chat_id})
        start = row['discovered_id'] + 1
        end = min(row.get('target_id', start - 1), start + 99)
        if start > end:
            return
        messages = await self.client.get_messages(int(chat_id), list(range(start, end + 1)))
        if not isinstance(messages, list):
            messages = [messages]
        # Only commit the cursor once every nonempty result has been journaled.
        for message in messages:
            if message and not getattr(message, 'empty', False):
                await self.enqueue(message, discovered=True)
        await self.channels.update_one({'_id': chat_id}, {'$max': {'discovered_id': end}})

    async def process_one(self, chat_id):
        row = await self.channels.find_one({'_id': chat_id}) or {}
        job = await self.jobs.find_one({'chat_id': chat_id, 'msg_id': {'$lte': row.get('discovered_id', -1)}},
                                       sort=[('msg_id', 1), ('kind', -1)])
        if not job or job.get('retry_at', 0) > time():
            return
        async with self.control_lock:
            if self.paused or not self.enabled(chat_id):
                return
            self.active = {'channel': chat_id, 'message_id': job['msg_id']}
        try:
            message = await self.client.get_messages(int(chat_id), job['msg_id'])
            if message and not getattr(message, 'empty', False):
                processor = self.edited_processor if job['kind'] == 'edit' else self.processor
                await processor(self.client, message, durable_context=job.get('context') or {})
            # If interrupted here, the stored source ID prevents duplicate indexing.
            await self.channels.update_one({'_id': chat_id}, {'$max': {'last_completed_id': job['msg_id']}})
            await self.jobs.delete_one({'_id': job['_id']})
        except Exception as exc:
            wait = max(5, int(getattr(exc, 'value', 30) or 30))
            await self.jobs.update_one({'_id': job['_id']}, {
                '$inc': {'attempts': 1}, '$set': {'retry_at': time() + wait,
                'error': f'{type(exc).__name__}: {str(exc)[:300]}'}})
            LOGGER.exception(f'[Ingestion] Retaining {chat_id}/{job["msg_id"]} for retry')
        finally:
            self.active = None

    async def run(self):
        while True:
            try:
                if not self.paused:
                    refresh = self.refresh_heads or time() - self.last_head_check > 300
                    self.refresh_heads = False
                    all_ok = True
                    for chat_id in list(SettingsManager.current().auth_channels):
                        if self.paused:
                            self.refresh_heads = True
                            break
                        if not self.enabled(chat_id):
                            continue
                        try:
                            await self.discover(str(chat_id), refresh)
                            await self.process_one(str(chat_id))
                        except Exception as exc:
                            all_ok = False
                            self.error = f'{type(exc).__name__}: {str(exc)[:300]}'
                            LOGGER.exception(f'[Ingestion] Channel {chat_id} retry deferred')
                    if all_ok:
                        if refresh:
                            self.last_head_check = time()
                        self.error = None
                    elif refresh:
                        self.refresh_heads = True
            except Exception as exc:
                self.error = type(exc).__name__
                LOGGER.exception('[Ingestion] Inbox temporarily unavailable')
            await asyncio.sleep(1 if not self.error else 15)

    async def set_paused(self, paused):
        async with self.control_lock:
            await self.state.update_one({'_id': 'control'}, {'$set': {'paused': paused}}, upsert=True)
            self.paused = paused
            if not paused:
                self.refresh_heads = True
        return await self.status()

    async def set_start(self, chat_id, message_id):
        if not self.authorized(chat_id):
            raise ValueError('Select an AUTH channel.')
        if isinstance(message_id, bool) or not str(message_id).isdigit() or int(message_id) < 1:
            raise ValueError('Starting message ID must be a positive integer.')
        async with self.discovery_lock, self.control_lock:
            if not self.paused or self.active:
                raise ValueError('Pause ingestion and wait for the current file to finish first.')
            async with self.lock:
                row = await self.channels.find_one({'_id': str(chat_id)}) or {}
                if int(message_id) > row.get('discovered_id', int(message_id) - 1) + 1:
                    raise ValueError('Use this control to replay earlier messages, not skip pending files.')
                await self.channels.update_one({'_id': str(chat_id)},
                    {'$set': {'discovered_id': int(message_id) - 1},
                     '$max': {'target_id': int(message_id)}}, upsert=True)
                self.refresh_heads = True
        return await self.status()

    async def status(self):
        authorized = list(SettingsManager.current().auth_channels)
        rows = await self.channels.find({'_id': {'$in': authorized}}).to_list(length=None)
        by_channel = {row['_id']: row for row in rows}
        rows = [by_channel.get(str(channel), {'_id': str(channel)}) for channel in authorized]
        failed = await self.jobs.find({'chat_id': {'$in': authorized}, 'attempts': {'$gt': 0}},
                                      {'chat_id': 1, 'msg_id': 1, 'error': 1}).limit(5).to_list(length=5)
        return {'paused': self.paused, 'active': self.active, 'error': self.error,
                'pending': await self.jobs.count_documents({'chat_id': {'$in': authorized}}),
                'retrying': await self.jobs.count_documents({'chat_id': {'$in': authorized}, 'attempts': {'$gt': 0}}),
                'failures': [{'channel': j['chat_id'], 'message_id': j['msg_id'], 'reason': j.get('error')} for j in failed],
                'channels': [{'channel': row['_id'], 'enabled': self.enabled(row['_id']), 'discovered_id': row.get('discovered_id'),
                              'last_completed_id': row.get('last_completed_id'), 'target_id': row.get('target_id', 0)} for row in rows]}


durable_ingestion = DurableIngestion()
