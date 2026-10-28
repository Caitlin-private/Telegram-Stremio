from wzgram import Client, filters, enums

from Backend import db
from Backend.config import Telegram
from Backend.helper.stats_display import collect_system_stats, format_stats
from Backend.logger import LOGGER


# ----- Owner system statistics use the same snapshot as the dashboard
@Client.on_message(filters.command("stats") & filters.private & filters.user(Telegram.OWNER_ID), group=-1)
async def stats_status(client, message):
    try:
        result = await collect_system_stats(db, client)
        if result.get("status") != "success":
            raise RuntimeError("Stats query failed")
        text = format_stats(result["data"])
    except Exception:
        LOGGER.exception("Owner stats could not be loaded")
        text = "⚠️ Could not load system stats. Please try again shortly."
    await message.reply_text(text, parse_mode=enums.ParseMode.DISABLED)
    message.stop_propagation()
