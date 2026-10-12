from pyrogram import Client, filters, enums

from Backend.config import Telegram
from Backend.helper.stats_display import format_stats
from Backend.logger import LOGGER
from Backend import db
from Backend.helper.settings_manager import SettingsManager
from pyrogram.types import InlineKeyboardButton as Button, InlineKeyboardMarkup as Keyboard, ForceReply
from html import escape
from urllib.parse import urlsplit
from time import monotonic
import asyncio
from pyrogram.errors import FloodWait

# Only unfinished owner edits are transient; saved notices live in MongoDB.
_notice_edit = {}
_dm_draft = None
_dm_job = None


@Client.on_message(filters.command('message') & filters.private & filters.user(Telegram.OWNER_ID), group=-3)
async def subscriber_message_menu(client, message):
    global _dm_draft
    _dm_draft = None
    if _dm_job and not _dm_job['task'].done():
        await message.reply_text(f"Sending: {_dm_job['sent']} sent · {_dm_job['failed']} failed",
            reply_markup=Keyboard([[Button('Stop sending',callback_data='dm:stop')]]))
    else:
        if _dm_job:
            await message.reply_text(f"Last delivery: {_dm_job['sent']} sent · {_dm_job['failed']} failed/unconfirmed · {_dm_job['total']-_dm_job['sent']-_dm_job['failed']} not attempted")
        await message.reply_text('✉️ Message subscribers\nAll registered users includes expired subscriptions. Text messages only. Preview before sending.',
            reply_markup=Keyboard([[Button('All registered subscribers',callback_data='dm:scope:all')],
                                   [Button('Select user',callback_data='dm:users:0')]]))
    message.stop_propagation()


@Client.on_callback_query(filters.regex(r'^dm:'))
async def subscriber_message_action(client, query):
    global _dm_draft, _dm_job
    if (not query.from_user or query.from_user.id != Telegram.OWNER_ID
            or not query.message or query.message.chat.id != Telegram.OWNER_ID):
        return await query.answer('Owner only.',show_alert=True)
    await query.answer()
    fields=query.data.split(':');action=fields[1];scope=fields[2] if len(fields)>2 else ''
    if action=='stop':
        if _dm_job: _dm_job['stop'].set()
        return await query.message.reply_text('Stop requested. A message already being sent may still arrive.')
    if _dm_job and not _dm_job['task'].done():
        return await query.message.reply_text('A delivery is running. Use /message for status or to stop it.')
    if action=='home':return await subscriber_message_menu(client,query.message)
    if action=='users':return await _subscriber_picker(query.message,scope,'dm')
    if action=='scope' and (scope=='all' or (scope.isdigit() and int(scope)>0)):
        prompt=await query.message.reply_text('Reply with the text to send (up to 3500 characters). Use /message to cancel.',reply_markup=ForceReply(selective=True))
        _dm_draft=dict(scope=scope,prompt=prompt.id,until=monotonic()+600)
        return
    if action=='send':
        draft=_dm_draft
        if not draft or draft.get('preview')!=query.message.id or draft['until']<monotonic():
            return await query.message.reply_text('Preview expired. Start again with /message.')
        _dm_draft=None  # Consume before the first await: prevents double-send taps.
        job=dict(sent=0,failed=0,stop=asyncio.Event(),total=len(draft['users']))
        _dm_job=job
        job['task']=asyncio.create_task(_deliver_subscriber_message(client,dict(draft),job))
        await query.message.reply_text(f"Sending to {job['total']} users. Use /message to check or stop. Restarting the service stops delivery; it will not automatically resend.")


@Client.on_message(filters.private & filters.user(Telegram.OWNER_ID) & filters.text & ~filters.regex(r'^/'), group=-3)
async def subscriber_message_reply(client,message):
    global _dm_draft
    draft=_dm_draft
    if not draft or not message.reply_to_message or message.reply_to_message.id!=draft['prompt']:
        return
    if draft['until']<monotonic():
        _dm_draft=None
        await message.reply_text('Draft expired. Use /message again.')
        message.stop_propagation()
    text=message.text.strip()
    if not text or len(text.encode('utf-16-le'))//2>3500:
        await message.reply_text('Use 1–3500 characters. Reply to the original prompt to retry.')
        message.stop_propagation()
    match={'user_id':{'$gt':0}} if draft['scope']=='all' else {'user_id':int(draft['scope'])}
    users=await db.dbs['tracking']['api_tokens'].distinct('user_id',match)
    if not users:
        await message.reply_text('No registered recipients found.')
        message.stop_propagation()
    draft.update(text=text,users=users)
    await message.reply_text(text,parse_mode=enums.ParseMode.DISABLED,disable_web_page_preview=True)
    label='All registered subscribers' if draft['scope']=='all' else f"User {draft['scope']}"
    confirm=await message.reply_text(f'Preview above\nRecipient: {label}\nTotal: {len(users)}\nSend this exact text?',
        reply_markup=Keyboard([[Button('✅ Send',callback_data='dm:send'),Button('Cancel',callback_data='dm:home')]]))
    draft['preview']=confirm.id
    message.stop_propagation()


async def _deliver_subscriber_message(client,draft,job):
    async def wait(seconds):
        try:await asyncio.wait_for(job['stop'].wait(),timeout=seconds)
        except asyncio.TimeoutError:pass
    try:
        for uid in draft['users']:
            if job['stop'].is_set():break
            while not job['stop'].is_set():
                try:
                    await client.send_message(uid,draft['text'],parse_mode=enums.ParseMode.DISABLED,disable_web_page_preview=True)
                    job['sent']+=1
                    break
                except FloodWait as exc:
                    await wait(max(1,exc.value)+1)
                except Exception as exc:
                    # Do not retry uncertain network outcomes, which can duplicate a DM.
                    job['failed']+=1
                    LOGGER.warning(f'Subscriber message failed: {type(exc).__name__}')
                    break
            await wait(1.0)
    finally:
        remaining=job['total']-job['sent']-job['failed']
        summary=f"✉️ Delivery {'stopped' if remaining else 'finished'}\n✅ Sent: {job['sent']}\n⚠️ Failed/unconfirmed: {job['failed']}\n⏳ Not attempted: {remaining}"
        try:await client.send_message(Telegram.OWNER_ID,summary,parse_mode=enums.ParseMode.DISABLED)
        except Exception:LOGGER.warning('Could not send subscriber delivery summary; use /message.')


async def _notice_menu(message, scope):
    if scope == 'all':
        notice = SettingsManager.current().stream_notice
        label = 'All subscribers (global default)'
    else:
        token = await db.get_api_token_by_user(int(scope))
        if not token:
            return await message.reply_text('User no longer has a linked addon token.')
        notice = token.get('stream_notice', SettingsManager.current().stream_notice)
        label = f"{token.get('name') or 'User'} · {scope}"
    text = f'📢 <b>Stream message</b>\n<b>Applies to:</b> {escape(label)}\n'
    text += f"<b>Status:</b> {'Enabled' if notice.get('enabled') else 'Disabled'}\n"
    if notice.get('enabled'):
        text += f"\n<b>{escape(notice.get('name') or '')}</b>\n{escape(notice.get('message') or '')}\n{escape(notice.get('url') or '')}\n"
    text += '\nUser overrides take priority over the global default.'
    rows = [[Button('✏️ Set message & link', callback_data=f'sn:edit:{scope}'),
             Button('🚫 Disable', callback_data=f'sn:off:{scope}')]]
    if scope != 'all':
        rows.append([Button('↩️ Use global default', callback_data=f'sn:inherit:{scope}')])
    rows.append([Button('Back', callback_data='sn:home')])
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True, reply_markup=Keyboard(rows))


@Client.on_message(filters.command('streammessage') & filters.private & filters.user(Telegram.OWNER_ID), group=-2)
async def stream_message_menu(client, message):
    _notice_edit.pop(message.chat.id, None)
    await message.reply_text('📢 Stream message\nSet a custom label, message and donation/website link in the stream list.',
        reply_markup=Keyboard([[Button('🌍 All subscribers', callback_data='sn:scope:all')],
                               [Button('👤 Select user', callback_data='sn:users:0')]]))
    message.stop_propagation()


@Client.on_callback_query(filters.regex(r'^sn:'))
async def stream_message_action(client, query):
    if (not query.from_user or query.from_user.id != Telegram.OWNER_ID
            or not query.message or query.message.chat.id != Telegram.OWNER_ID):
        return await query.answer('Owner only.', show_alert=True)
    await query.answer()
    parts = query.data.split(':')
    action = parts[1]
    scope = parts[2] if len(parts) > 2 else ''
    if action == 'home':
        return await stream_message_menu(client, query.message)
    if action == 'users':
        return await _subscriber_picker(query.message, scope, "sn")
    if scope != 'all' and (not scope.isdigit() or int(scope) <= 0):
        return
    if action == 'scope':
        _notice_edit.pop(query.message.chat.id, None)
        return await _notice_menu(query.message, scope)
    if action == 'edit':
        prompt = await query.message.reply_text('Reply with exactly three lines:\n1. Stream label (up to 80 characters)\n2. Custom message (up to 500 characters)\n3. https:// donation or website link\n\nExample:\n💛 Support Caitlin Stream\nHelp keep the service running.\nhttps://example.com/donate\n\nUse /streammessage to cancel.', reply_markup=ForceReply(selective=True))
        _notice_edit[query.message.chat.id] = dict(scope=scope,prompt=prompt.id,until=monotonic()+600)
        return
    if action not in ('save','off','inherit') or (action=='inherit' and scope=='all'):
        return
    if action == 'save':
        draft = _notice_edit.get(query.message.chat.id) or {}
        if (draft.get('scope') != scope or draft.get('until',0)<monotonic()
                or draft.get('preview') != query.message.id or not draft.get('notice')):
            return await query.message.reply_text('This preview expired. Open /streammessage again.')
        notice = draft['notice']
    else:
        notice = {'enabled':False}
    try:
        if scope == 'all':
            await SettingsManager.update(db, {'stream_notice': notice})
        else:
            change = {'$unset': {'stream_notice': ''}} if action=='inherit' else {'$set': {'stream_notice':notice}}
            result = await db.dbs['tracking']['api_tokens'].update_many({'user_id':int(scope)},change)
            if not result.matched_count:
                return await query.message.reply_text('User no longer has a linked addon token.')
    except Exception:
        LOGGER.exception('Could not save stream message')
        return await query.message.reply_text('Save failed. Please try again.')
    _notice_edit.pop(query.message.chat.id, None)
    await query.message.reply_text('✅ Saved. Applies when the client refreshes its stream list.')
    await _notice_menu(query.message,scope)


@Client.on_message(filters.private & filters.user(Telegram.OWNER_ID) & filters.text & ~filters.regex(r'^/'), group=-2)
async def stream_message_reply(client, message):
    draft = _notice_edit.get(message.chat.id)
    if not draft or not message.reply_to_message or message.reply_to_message.id != draft.get('prompt'):
        return
    if draft['until'] < monotonic():
        _notice_edit.pop(message.chat.id,None)
        await message.reply_text('Edit expired. Open /streammessage again.')
        message.stop_propagation()
    lines = [s.strip() for s in message.text.splitlines()]
    error = 'Use three nonempty lines: label, message, HTTP(S) link.'
    valid = len(lines)==3 and all(lines) and len(lines[0])<=80 and len(lines[1])<=500 and len(lines[2])<=2048
    if valid:
        try:
            url=urlsplit(lines[2]);url.port
            valid=url.scheme in ('http','https') and bool(url.hostname) and not (url.username or url.password or any(c.isspace() for c in lines[2]))
        except ValueError:
            valid=False
    if not valid:
        await message.reply_text(error+' Reply to the original prompt to retry.')
        message.stop_propagation()
    draft['notice']=dict(enabled=True,name=lines[0],message=lines[1],url=lines[2])
    preview=await message.reply_text(f"<b>Preview · {'All subscribers' if draft['scope']=='all' else 'User '+draft['scope']}</b>\n\n<b>{escape(lines[0])}</b>\n{escape(lines[1])}\n{escape(lines[2])}",
        parse_mode=enums.ParseMode.HTML,disable_web_page_preview=True,
        reply_markup=Keyboard([[Button('✅ Save',callback_data=f"sn:save:{draft['scope']}"),Button('Cancel',callback_data='sn:home')]]))
    draft['preview']=preview.id
    message.stop_propagation()


@Client.on_message(filters.command('stats') & filters.private & filters.user(Telegram.OWNER_ID), group=-1)
async def stats_status(client, message):
    from Backend.fastapi.routes.api_routes import get_db_stats_api
    try:
        result = await get_db_stats_api()
        if result.get('status') != 'success':
            raise RuntimeError('Stats query failed')
        text = format_stats(result['data'])
    except Exception:
        LOGGER.exception('Owner stats could not be loaded')
        text = '⚠️ Could not load system stats. Please try again shortly.'
    await message.reply_text(text, parse_mode=enums.ParseMode.DISABLED)
    message.stop_propagation()


async def _subscriber_picker(message, page_text, prefix):
    page = max(0, int(page_text)) if page_text.isdigit() else 0
    # Fetch only display data, never tokens, with database-side pagination.
    users = await db.dbs['tracking']['api_tokens'].aggregate([
        {'$match': {'user_id': {'$gt': 0}}},
        {'$group': {'_id': '$user_id', 'name': {'$first': '$name'}}},
        {'$sort': {'_id': 1}}, {'$skip': page * 8}, {'$limit': 9},
    ]).to_list(9)
    rows = [[Button(f"{str(u.get('name') or 'User')[:35]} · {u['_id']}", callback_data=f"{prefix}:scope:{u['_id']}")] for u in users[:8]]
    nav = []
    if page: nav.append(Button('Previous', callback_data=f'{prefix}:users:{page-1}'))
    if len(users)>8: nav.append(Button('Next', callback_data=f'{prefix}:users:{page+1}'))
    if nav: rows.append(nav)
    rows.append([Button('Back', callback_data=f'{prefix}:home')])
    return await message.reply_text('Select a registered user:' if users else 'No linked users found.',reply_markup=Keyboard(rows))
