"""Telegram draft links preserve full Unicode titles without a start payload."""
import re
from urllib.parse import quote, urlsplit


def normalize_search_bot(value):
    if not isinstance(value, str):
        raise ValueError("Search bot must be a username or https://t.me/username link.")
    value = value.strip()
    if not value:
        return ""
    if value.startswith(("https://", "http://", "t.me/")):
        parsed = urlsplit(value if "://" in value else "https://" + value)
        if parsed.hostname != "t.me" or parsed.query or parsed.fragment or parsed.port:
            raise ValueError("Use a plain https://t.me/username link.")
        value = parsed.path.strip("/")
    value = value.removeprefix("@")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", value) or not value.lower().endswith("bot"):
        raise ValueError("Enter a valid search bot username ending in bot, or leave blank to disable.")
    return value


def search_bot_link(username, title):
    try:
        username = normalize_search_bot(username)
    except ValueError:
        return None
    title = str(title or "").strip()
    if not username or not title:
        return None
    return f"https://t.me/{username}?text={quote(title, safe='')}"
