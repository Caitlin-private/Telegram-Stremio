"""Server-side permission for playback through the connected Telegram session."""

def can_stream_session(token_data):
    token_data = token_data or {}
    return bool(token_data.get("is_admin")) or token_data.get("session_streaming_allowed") is True
