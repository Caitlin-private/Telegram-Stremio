from html import escape

from wzgram import Client, enums, filters

from Backend import db
from Backend.helper import insights_agg
from Backend.logger import LOGGER


@Client.on_message(filters.command("history") & filters.private, group=5)
async def viewer_history(client: Client, message):
    """Show a registered viewer their own recent playback history."""
    user = getattr(message, "from_user", None)
    user_id = getattr(user, "id", None)
    if not user_id:
        return
    try:
        token_doc = await db.get_api_token_by_user(int(user_id))
        if not token_doc or not token_doc.get("token"):
            await message.reply_text(
                "I couldn't find a registered Stremio account for you. Use /start first, then try /history.",
                quote=True,
            )
            return

        # A Telegram subscriber may own several addon tokens. The web Insights
        # page groups those sessions by Telegram identity, so /history must do
        # the same instead of filtering to the single token returned by lookup.
        records = await db.fetch_insight_records(days=30, limit=5000)
        token_map = await db.get_token_user_map()
        sessions = insights_agg.build_sessions(records)
        profile = insights_agg.viewer_profile(
            sessions, token_map, viewer_key=f"user:{int(user_id)}"
        )
        totals = profile.get("totals") or {}
        lines = [
            "📺 <b>Your viewing insights</b>",
            "<i>Last 30 days · private to you</i>",
            "",
            f"▶️ Total plays: <b>{totals.get('plays', 0)}</b>",
            f"⏱ Total watch time: <b>{totals.get('watch_time', '0m')}</b>",
            f"🎞 Total titles: <b>{totals.get('unique_titles', 0)}</b>",
        ]
        completion = totals.get("avg_completion")
        lines.append(f"📊 Average completion: <b>{round(completion * 100)}%</b>" if isinstance(completion, (int, float)) else "📊 Average completion: <b>—</b>")
        lines.extend(["", "<b>Last 3 titles played</b>"])
        history = profile.get("history") or []
        if not history:
            lines.append("No recent titles found.")
        for index, item in enumerate(history[:3], 1):
            pct = item.get("completion")
            pct_text = f" · {round(float(pct) * 100)}% complete" if isinstance(pct, (int, float)) else ""
            lines.append(
                f"{index}. <b>{escape(str(item.get('title') or 'Unknown title')[:100])}</b>"
                f"\n   {escape(str(item.get('when') or 'Unknown date'))} · {escape(str(item.get('watch_time') or '—'))}{pct_text}"
            )
        await message.reply_text("\n".join(lines), quote=True, parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        LOGGER.warning("/history failed for user %s: %s", user_id, exc)
        await message.reply_text("Sorry, I couldn't load your viewing history right now. Please try again later.", quote=True)
