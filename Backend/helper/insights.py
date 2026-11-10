"""Viewer-insights helpers: stream-meta enrichment + media-identity resolution.

Everything collected here feeds the admin Insights page (per-title audience,
per-viewer profile, uploader identity, completion).  All fields are additive
on the stream record and admin-only by policy (30-day raw retention).

Identity modes (both live at the same time):

- **per-user tokens** — ``token_user_id`` (api_tokens.user_id) identifies the
  Telegram person directly;
- **shared token** (today's setup) — every viewer uses the same token, so the
  aggregation layer distinguishes viewers by ``client_ip`` (+ device): the UI
  labels those entries "shared token" and shows IP/ISP/city instead of a
  Telegram name.  Issuing per-user tokens later upgrades the same rows to
  name-based identities automatically.
"""

import hashlib
import time
from typing import Optional

from Backend.helper.analytics import client_ip_from, parse_app, parse_device

_MEDIA_IDENTITY_CACHE: dict = {}
_MEDIA_IDENTITY_TTL = 300.0
_MEDIA_IDENTITY_CACHE_MAX = 512


def playback_id_for(token: str, stream_id_hash: str) -> Optional[str]:
    """Stable id stitching a player's many ranged GETs into one playback.

    Tokens and the stream-id hash are stable for a given title-quality per
    viewer, so seeks/pauses/reconnects land on the same playback; distinct
    viewing sessions are split at read time by time gaps."""
    if not token or not stream_id_hash:
        return None
    digest = hashlib.sha1(f"{token}:{stream_id_hash}".encode("utf-8", "ignore")).hexdigest()
    return digest[:20]


async def resolve_media_identity(stream_id_hash) -> Optional[dict]:
    """Cached stream-id -> catalogue identity. One lookup per id per five minutes."""
    key = str(stream_id_hash or "").strip()
    if not key:
        return None
    now = time.time()
    cached = _MEDIA_IDENTITY_CACHE.get(key)
    if cached and now < cached[1]:
        return cached[0]
    identity = None
    try:
        from Backend import db

        identity = await db.find_media_identity_by_stream_id(key)
    except Exception:
        identity = None
    if len(_MEDIA_IDENTITY_CACHE) >= _MEDIA_IDENTITY_CACHE_MAX:
        _MEDIA_IDENTITY_CACHE.clear()
    _MEDIA_IDENTITY_CACHE[key] = (identity, now + _MEDIA_IDENTITY_TTL)
    return identity


def enrich_stream_meta(
    meta: dict,
    *,
    request=None,
    token: Optional[str] = None,
    stream_id_hash: Optional[str] = None,
    media_identity: Optional[dict] = None,
) -> dict:
    """Add per-view viewer fields to a stream meta dict (mutates + returns)."""
    try:
        if request is not None:
            meta["client_ip"] = client_ip_from(request)
            ua = request.headers.get("user-agent")
            if ua:
                meta["app"] = parse_app(ua)
                meta["device"] = parse_device(ua)
    except Exception:
        pass
    try:
        if token and stream_id_hash:
            meta["playback_id"] = playback_id_for(token, stream_id_hash)
    except Exception:
        pass
    if media_identity:
        meta["media"] = media_identity
    if token:
        meta["token"] = token
    if request is not None:
        try:
            meta["token_user_id"] = meta.get("token_user_id")
            meta["user_agent"] = request.headers.get("user-agent") or ""
        except Exception:
            pass
    return meta
