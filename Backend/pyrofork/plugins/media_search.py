from pyrogram import Client, filters
from Backend import db
from Backend.helper.bot_media import registered_token, search_titles, send_card
from Backend.logger import LOGGER


@Client.on_message(filters.private & filters.text & ~filters.regex(r'^/'), group=20)
async def search_media_chat(client, message):
    query = (message.text or '').strip()
    if not 2 <= len(query) <= 100:
        await message.reply_text('Send a movie or series name between 2 and 100 characters.')
        return
    try:
        token = await registered_token(message)
        if not token:
            return
        matches = await search_titles(db, query)
        if not matches:
            await message.reply_text('No matching movies or series found. Try another title.')
            return
        for doc in matches[:5]:
            await send_card(message, doc, token)
        if len(matches) > 5:
            await message.reply_text('Showing the first 5 matches. Send a more specific title to narrow the search.')
    except Exception as exc:
        # Telegram exceptions can include request URLs containing a personal token.
        LOGGER.warning(f'Bot media search failed: {type(exc).__name__}')
        await message.reply_text('Unable to search right now. Please try again later.')
