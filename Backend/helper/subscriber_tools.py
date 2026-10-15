from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Backend import db


# ----- Shared paginated user picker for owner commands
async def subscriber_picker(message, page_text, prefix):
    page = max(0, int(page_text)) if page_text.isdigit() else 0
    # Fetch only display data, never tokens, with database-side pagination.
    users = await db.list_registered_token_users(page)
    rows = [
        [
            InlineKeyboardButton(
                f"{str(u.get('name') or 'User')[:35]} · {u['_id']}", callback_data=f"{prefix}:scope:{u['_id']}"
            )
        ]
        for u in users[:8]
    ]
    nav = []
    if page:
        nav.append(InlineKeyboardButton("Previous", callback_data=f"{prefix}:users:{page-1}"))
    if len(users) > 8:
        nav.append(InlineKeyboardButton("Next", callback_data=f"{prefix}:users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("Back", callback_data=f"{prefix}:home")])
    return await message.reply_text(
        "Select a registered user:" if users else "No linked users found.", reply_markup=InlineKeyboardMarkup(rows)
    )
