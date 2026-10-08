"""HTML-safe scan lifecycle notices, independent of per-title announcements."""
import asyncio
from html import escape

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

from Backend.config import Telegram
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.system_memory import memory_status
from Backend.logger import LOGGER


def format_notice(event, report, counts, elapsed, totals=None):
    settings = SettingsManager.current()
    title = {'start': '🚀 Starting Media Import', 'stop': '🛑 Media Import Stopped',
             'finish': '✅ Media Import Finished', 'cancel': '❌ Media Import Cancelled', 'error': '⚠️ Media Import Stopped'}[event]
    mode = {'scan': 'Scan', 'quick': 'Quick Scan', 'rescan': 'Rescan (Wipe & Re-index)'}.get(report['mode'], 'Scan')
    owner_id = int(Telegram.OWNER_ID or 0)
    owner = f'<a href="tg://user?id={owner_id}">Owner</a>' if owner_id > 0 else 'Owner'
    lines = [f'<b>{title}</b>', '', '━━━━━━━━━━━━━━━━━━', '']
    target = report.get('target')
    lines += [f'📨 <b>Total Media Detected:</b> {target:,}' if target is not None else
              '📨 <b>Total Media Detected:</b> Unknown', '']
    if event != 'start':
        lines += [f'⏱️ <b>Elapsed time:</b> {escape(elapsed)}',
                  f'📂 <b>Processed files:</b> {counts.get("media_processed", 0):,}',
                  f'✅ <b>Indexed files:</b> {counts.get("indexed", 0):,}',
                  f'🚫 <b>Resolution skips:</b> {counts.get("skipped_resolution", 0):,}', '']
        if counts.get('skipped_delete_forbidden'):
            lines += [f'🔒 <b>Skipped (deletion forbidden):</b> {counts["skipped_delete_forbidden"]:,}', '']
    lines += [f'🔍 <b>Scan type:</b> {escape(mode)}',
              f'📢 <b>Channel:</b> {escape(str(report["name"]))}',
              f'🆔 <b>Channel ID:</b> <code>{escape(str(report["channel"]))}</code>']
    if event == 'start':
        if report.get('cursor', 1) > 1:
            lines += [f'🔄 <b>Continuing from message ID:</b> <code>{report["cursor"]}</code>']
        excluded = [r for r in ('360p', '480p', '720p', '1080p', '1440p', '2160p', 'other')
                    if r not in settings.ingestion_resolutions]
        excluded = ['Other resolutions (including 540p)' if r == 'other' else r for r in excluded]
        if not settings.allow_unknown_resolution:
            excluded.append('Unknown / unlabelled')
        lines += ['', f'🚫 <b>Skipped resolutions:</b> {escape(", ".join(excluded) or "None")}',
                  f'🧠 <b>Available RAM:</b> {escape(memory_status()["display"])}',
                  f'👤 <b>Started by</b> {owner}', '', '━━━━━━━━━━━━━━━━━━']
    elif event == 'finish':
        totals = totals or {}
        lines.append('')
        for key, label in (('movies', '🎬 Total movies now'),
                           ('series', '📺 Total TV series now'),
                           ('files', '📂 Total Files in the Database'),
                           ('size', '🗄️ Database size')):
            value = totals.get(key, 'Unavailable')
            value = f'{value:,}' if isinstance(value, int) else escape(str(value))
            lines.append(f'<b>{label}:</b> {value}')
        lines.append(f'👤 <b>Started by</b> {owner}')
    elif event == 'stop':
        lines += ['', f'👤 <b>Stopped by</b> {owner}', '🔄 Resume from Channel Scanner to continue.']
    elif event == 'cancel':
        lines += ['', f'👤 <b>Cancelled by</b> {owner}', 'Scan job cleared. Already indexed media remains in the library.']
    else:
        lines += ['', '⚠️ The scan stopped because of an error. Check the dashboard for details.']
    return '\n'.join(lines)


async def send_notice(client, channel, text, previous=None):
    # Keep notices ordered without blocking ingestion or the Stop button.
    if previous is not None:
        await previous
    while True:
        try:
            await client.send_message(channel, text, parse_mode=ParseMode.HTML,
                                      disable_web_page_preview=True)
            return
        except FloodWait as exc:
            await asyncio.sleep(max(1, exc.value) + 1)
        except Exception as exc:
            LOGGER.warning(f'[Scan announcement] Delivery failed: {type(exc).__name__}: {exc}')
            return
