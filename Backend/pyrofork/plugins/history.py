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

        token = str(token_doc["token"])
        records = await db.fetch_insight_records(days=30, limit=2000, token=token)
        if not records:
            await message.reply_text(
                "📺 <b>Your viewing history</b>\n\nNo playback history has been recorded in the last 30 days yet.",
                quote=True, parse_mode=enums.ParseMode.HTML,
            )
            return

        token_map = await db.get_token_user_map()
        sessions = insights_agg.build_sessions(records)
        profile = insights_agg.viewer_profile(sessions, token_map, token=token)
        totals = profile.get("totals") or {}
        viewer = profile.get("viewer") or {}
        lines = [
            "📺 <b>Your viewing history</b>",
            "<i>Last 30 days · private to you</i>",
            "",
            f"▶️ Plays: <b>{totals.get('plays', 0)}</b>",
            f"⏱ Watch time: <b>{totals.get('watch_time', '0m')}</b>",
            f"🎞 Titles: <b>{totals.get('unique_titles', 0)}</b>",
        ]
        completion = totals.get("avg_completion")
        if isinstance(completion, (int, float)):
            lines.append(f"📊 Average completion: <b>{round(completion * 100)}%</b>")
        if viewer.get("place"):
            lines.append(f"📍 Last known location: <b>{escape(str(viewer['place']))}</b>")
        lines.extend(["", "<b>Recently watched</b>"])
        for index, item in enumerate((profile.get("history") or [])[:10], 1):
            pct = item.get("completion")
            pct_text = f" · {round(float(pct) * 100)}%" if isinstance(pct, (int, float)) else ""
            device = item.get("device") or item.get("app") or "Unknown device"
            place = ", ".join(x for x in (item.get("city"), item.get("country")) if x)
            place_text = f" · {place}" if place else ""
            lines.append(
                f"{index}. <b>{escape(str(item.get('title') or 'Unknown title')[:100])}</b>\n"
                f"   {escape(str(item.get('when') or 'Unknown date'))} · {escape(str(item.get('watch_time') or '—'))}{pct_text}\n"
                f"   {escape(str(device)[:40])}{escape(str(place_text)[:70])}"
            )
        await message.reply_text("\n".join(lines), quote=True, parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        LOGGER.warning("/history failed for user %s: %s", user_id, exc)
        await message.reply_text("Sorry, I couldn't load your viewing history right now. Please try again later.", quote=True)
