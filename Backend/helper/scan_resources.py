"""Worker selection and playback priority for the channel scanner only."""
import asyncio
import time

from Backend.helper.stats_display import live_counts


def playback_active():
    from Backend.helper.custom_dl import ACTIVE_STREAMS, STALE_STREAM_IDLE
    return live_counts(list(ACTIVE_STREAMS.values()), time.time(), STALE_STREAM_IDLE)['live_streams'] > 0


async def select_scan_clients(main, channel, limit, quick, call):
    from Backend.pyrofork.bot import multi_clients
    from Backend.helper.settings_manager import SettingsManager
    from Backend.logger import LOGGER
    candidates = [c for key, c in sorted(multi_clients.items())
                  if key >= 0 and c is not main and getattr(c, 'is_connected', False)]
    selected = []
    skip = SettingsManager.current().skip_channel
    for client in candidates + [main]:
        if client is main and selected:
            break  # Reserve the main bot for live ingestion when workers qualify.
        if any(client is c for c in selected):
            continue
        try:
            member = await call(client.get_chat_member, channel, 'me', scan_client=client)
            role = str(getattr(member.status, 'value', member.status)).lower()
            privileges = getattr(member, 'privileges', None)
            if role not in ('owner', 'creator') and not (
                    role == 'administrator' and privileges and privileges.can_delete_messages):
                continue
            if skip and not quick:
                target = int(skip) if str(skip).lstrip('-').isdigit() else skip
                member = await call(client.get_chat_member, target, 'me', scan_client=client)
                role = str(getattr(member.status, 'value', member.status)).lower()
                privileges = getattr(member, 'privileges', None)
                if role not in ('owner', 'creator') and not (
                        role == 'administrator' and privileges and privileges.can_post_messages):
                    continue
            selected.append(client)
        except Exception as exc:
            # A manual Stop must propagate, rather than selecting another bot.
            if type(exc).__name__ == '_ScanStopped':
                raise
            LOGGER.warning(f'[Scan] Worker is not eligible for {channel}: {type(exc).__name__}')
        if len(selected) >= (limit or 8):
            break
    if not selected:
        raise RuntimeError('No eligible scan bot. Grant a connected bot delete permission in the source channel and post permission in the skip channel.')
    return selected


class PlaybackPriority:
    def __init__(self, stopped, enabled=True, grace=60):
        self.stopped = stopped
        self.enabled = enabled
        self.grace = grace
        self.resume_after = 0
        self.paused = False

    async def wait(self):
        while self.enabled and not self.stopped.is_set():
            now = time.monotonic()
            if playback_active():
                self.resume_after = now + self.grace
            self.paused = now < self.resume_after
            if not self.paused:
                return
            try:
                await asyncio.wait_for(self.stopped.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
        self.paused = False
