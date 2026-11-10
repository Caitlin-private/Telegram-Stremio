"""Viewer-Insights aggregation: sessions, identities, rollups, CSV rows.

Pure functions over raw ``stream_analytics`` records — no DB access here, so
everything is unit-testable with dict fixtures.  The data layer feeds these
with ``Database.fetch_insight_records()`` + ``Database.get_token_user_map()``.

Session model: one playback = many ranged GET records sharing a
``playback_id`` (legacy rows fall back to token+chat+msg).  Records in the
same playback bucket (2 h) merge into one session; bigger gaps split a
re-watch into a new session.

Identity model (both live at once):
- ``user``  — token linked to a Telegram user (``token_user_id``);
- ``ip``    — shared/unknown tokens: viewers distinguished by client IP.
"""

from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

SESSION_BUCKET_SEC = 2 * 3600


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------
def session_key(record: dict) -> str:
    """Stable key for (playback, 2 h bucket): merges a player's ranged GETs."""
    playback = record.get("playback_id")
    if not playback:
        playback = "|".join(
            [
                str(record.get("token") or "?"),
                str(record.get("chat_id") or 0),
                str(record.get("msg_id") or 0),
            ]
        )
    logged = record.get("logged_at")
    try:
        epoch = logged.timestamp() if isinstance(logged, datetime) else float(logged or 0)
    except Exception:
        epoch = 0.0
    bucket = int(epoch // SESSION_BUCKET_SEC)
    return f"{playback}@{bucket}"


def _ts(record: dict) -> float:
    logged = record.get("logged_at")
    try:
        return logged.timestamp() if isinstance(logged, datetime) else float(logged or 0)
    except Exception:
        return 0.0


def build_sessions(records: Iterable[dict]) -> list[dict]:
    """Merge per-request records into playback sessions (newest last)."""
    sessions: dict[str, dict] = {}
    for record in records or []:
        if not isinstance(record, dict):
            continue
        key = session_key(record)
        session = sessions.get(key)
        ts = _ts(record)
        if session is None:
            session = {
                "session_key": key,
                "playback_id": record.get("playback_id"),
                "token": record.get("token"),
                "user_name": record.get("user_name"),
                "token_user_id": record.get("token_user_id"),
                "client_ip": record.get("client_ip"),
                "device": record.get("device"),
                "app": record.get("app"),
                "user_agent": record.get("user_agent"),
                "geo": record.get("geo"),
                "media": record.get("media"),
                "title": record.get("title"),
                "filename": record.get("filename"),
                "chat_id": record.get("chat_id"),
                "msg_id": record.get("msg_id"),
                "source_type": record.get("source_type"),
                "completion": None,
                "watched_offset": 0,
                "file_size": 0,
                "bytes": 0,
                "seconds": 0.0,
                "requests": 0,
                "first_ts": ts,
                "last_ts": ts,
            }
            sessions[key] = session
        # last non-null identity/context wins (a session never changes viewer)
        for field in ("token", "user_name", "token_user_id", "client_ip", "device",
                      "app", "user_agent", "geo", "media", "title", "filename",
                      "chat_id", "msg_id", "source_type"):
            value = record.get(field)
            if value not in (None, "", {}, []):
                session[field] = value
        completion = record.get("completion")
        if isinstance(completion, (int, float)):
            session["completion"] = max(session["completion"] or 0.0, float(completion))
        try:
            session["watched_offset"] = max(
                int(session["watched_offset"] or 0), int(record.get("watched_offset") or 0)
            )
            session["file_size"] = max(
                int(session["file_size"] or 0), int(record.get("file_size") or 0)
            )
        except (TypeError, ValueError):
            pass
        session["bytes"] += int(record.get("total_bytes") or 0)
        session["seconds"] += float(record.get("duration_sec") or 0.0)
        session["requests"] += 1
        session["first_ts"] = min(session["first_ts"], ts)
        session["last_ts"] = max(session["last_ts"], ts)

    out = [s for s in sessions.values() if _is_play(s)]
    out.sort(key=lambda s: s.get("last_ts") or 0)
    return out


def _is_play(session: dict) -> bool:
    return bool(session.get("watched_offset") or session.get("bytes") or session.get("completion"))


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------
def classify_identity(session: dict, token_map: Optional[dict] = None) -> dict:
    """Resolve the viewer identity of a session (user / token / ip mode)."""
    token_map = token_map or {}
    info = token_map.get(str(session.get("token") or "")) or {}
    user_id = session.get("token_user_id") or info.get("user_id")
    name = info.get("full_name") or info.get("name") or session.get("user_name") or "Unknown"
    username = info.get("username")

    if user_id:
        return {
            "mode": "user",
            "key": f"user:{user_id}",
            "telegram_id": user_id,
            "label": name,
            "username": username,
            "is_owner": bool(info.get("is_owner")),
            "shared_token": False,
        }
    if info.get("shared"):
        ip = session.get("client_ip") or ""
        geo = session.get("geo") or {}
        device = session.get("device") or session.get("app") or ""
        place = ", ".join(p for p in (geo.get("city"), geo.get("country")) if p)
        identity_key = f"ip:{ip}|{device.casefold()}" if ip and device else f"ip:{ip or 'unknown'}"
        return {
            "mode": "ip",
            "key": identity_key,
            "telegram_id": None,
            "label": f"Viewer · {ip}" if ip else f"Viewer · {session.get('user_name') or 'shared'}",
            "username": None,
            "place": place,
            "isp": geo.get("isp") or "",
            "shared_token": True,
        }
    return {
        "mode": "token",
        "key": f"token:{session.get('token') or 'unknown'}",
        "telegram_id": None,
        "label": name,
        "username": None,
        "shared_token": False,
    }


def media_key(session: dict) -> str:
    media = session.get("media") or {}
    if media.get("imdb_id"):
        key = f"imdb:{media['imdb_id']}"
        if media.get("season") is not None and media.get("episode") is not None:
            key += f":s{media['season']}e{media['episode']}"
        return key
    if media.get("tmdb_id"):
        return f"tmdb:{media['tmdb_id']}"
    if session.get("chat_id") and session.get("msg_id"):
        return f"origin:{session['chat_id']}:{session['msg_id']}"
    return f"title:{(session.get('title') or 'unknown').strip().lower()}"


def media_label_of_session(session: dict) -> str:
    media = session.get("media") or {}
    title = media.get("title") or session.get("title") or "Unknown"
    if media.get("media_type") == "tv" and media.get("season") is not None:
        return f"{title} S{int(media['season']):02d}E{int(media.get('episode') or 0):02d}"
    return title


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------
def _avg(values: list) -> Optional[float]:
    values = [v for v in values if isinstance(v, (int, float))]
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _fmt_seconds(seconds: float) -> str:
    total = int(seconds or 0)
    hours, rem = divmod(total, 3600)
    minutes = rem // 60
    return f"{hours}h {minutes:02d}m" if hours else f"{minutes}m"


def summarize_overview(sessions: list[dict], token_map: Optional[dict] = None) -> dict:
    """Totals + per-day trend + top titles/viewers + device/country splits."""
    token_map = token_map or {}
    viewers: dict[str, dict] = {}
    titles: dict[str, dict] = {}
    per_day: dict[str, dict] = {}
    devices: dict[str, int] = {}
    apps: dict[str, int] = {}
    countries: dict[str, int] = {}
    identity_mix = {"user": 0, "ip": 0, "token": 0}

    total_seconds = 0.0
    total_bytes = 0
    completions: list = []

    for session in sessions:
        ident = classify_identity(session, token_map)
        identity_mix[ident["mode"]] = identity_mix.get(ident["mode"], 0) + 1

        viewer = viewers.setdefault(ident["key"], {
            "key": ident["key"], "label": ident["label"], "username": ident.get("username"),
            "telegram_id": ident.get("telegram_id"), "mode": ident["mode"],
            "shared_token": ident.get("shared_token", False),
            "place": ident.get("place") or "", "isp": ident.get("isp") or "",
            "plays": 0, "seconds": 0.0, "last_ts": 0.0,
        })
        viewer["plays"] += 1
        viewer["seconds"] += session.get("seconds") or 0.0
        viewer["last_ts"] = max(viewer["last_ts"], session.get("last_ts") or 0)
        geo = session.get("geo") or {}
        if not viewer.get("place"):
            viewer["place"] = ", ".join(p for p in (geo.get("city"), geo.get("country")) if p)
        if not viewer.get("isp"):
            viewer["isp"] = geo.get("isp") or ""

        tkey = media_key(session)
        title = titles.setdefault(tkey, {
            "key": tkey, "label": media_label_of_session(session),
            "media": session.get("media"), "plays": 0, "viewers": set(),
            "seconds": 0.0, "last_ts": 0.0,
        })
        title["plays"] += 1
        title["viewers"].add(ident["key"])
        title["seconds"] += session.get("seconds") or 0.0
        title["last_ts"] = max(title["last_ts"], session.get("last_ts") or 0)

        day = None
        try:
            if session.get("last_ts"):
                day = datetime.utcfromtimestamp(session["last_ts"]).strftime("%Y-%m-%d")
        except Exception:
            day = None
        if day:
            bucket = per_day.setdefault(day, {"date": day, "plays": 0, "viewers": set(), "seconds": 0.0})
            bucket["plays"] += 1
            bucket["viewers"].add(ident["key"])
            bucket["seconds"] += session.get("seconds") or 0.0

        device = session.get("device") or "Unknown"
        devices[device] = devices.get(device, 0) + 1
        app = session.get("app") or "Unknown"
        apps[app] = apps.get(app, 0) + 1
        geo = session.get("geo") or {}
        country = geo.get("country") or "Unknown"
        countries[country] = countries.get(country, 0) + 1

        total_seconds += session.get("seconds") or 0.0
        total_bytes += session.get("bytes") or 0
        if isinstance(session.get("completion"), (int, float)):
            completions.append(session["completion"])

    top_titles = sorted(titles.values(), key=lambda t: (-t["plays"], t["label"]))
    for t in top_titles:
        t["viewer_count"] = len(t.pop("viewers"))
        t["seconds"] = round(t["seconds"], 1)
    top_viewers = sorted(viewers.values(), key=lambda v: (-v["plays"], v["label"]))
    for v in top_viewers:
        v["seconds"] = round(v["seconds"], 1)
        v["watch_time"] = _fmt_seconds(v["seconds"])
        v["last_seen"] = (
            datetime.utcfromtimestamp(v["last_ts"]).strftime("%Y-%m-%d %H:%M")
            if v["last_ts"] else None
        )
    days_list = [
        {"date": d, "plays": b["plays"], "viewers": len(b["viewers"]), "seconds": round(b["seconds"], 1)}
        for d, b in sorted(per_day.items())
    ]

    return {
        "totals": {
            "plays": len(sessions),
            "unique_viewers": len(viewers),
            "unique_titles": len(titles),
            "total_seconds": round(total_seconds, 1),
            "total_watch_time": _fmt_seconds(total_seconds),
            "total_bytes": total_bytes,
            "avg_completion": _avg(completions),
            "identity_mix": identity_mix,
        },
        "per_day": days_list,
        "top_titles": top_titles[:25],
        "top_viewers": top_viewers[:100],
        "devices": [{"device": k, "plays": v} for k, v in sorted(devices.items(), key=lambda kv: -kv[1])],
        "apps": [{"app": k, "plays": v} for k, v in sorted(apps.items(), key=lambda kv: -kv[1])],
        "countries": [{"country": k, "plays": v} for k, v in sorted(countries.items(), key=lambda kv: -kv[1])][:12],
    }


def title_audience(
    sessions: list[dict],
    token_map: Optional[dict] = None,
    *,
    imdb_id: Optional[str] = None,
    tmdb_id: Optional[Any] = None,
    db_index: Optional[int] = None,
    title: Optional[str] = None,
) -> dict:
    """Per-title viewer breakdown (who watched this, how much, from where)."""
    token_map = token_map or {}
    selected = _select_title_sessions(
        sessions, imdb_id=imdb_id, tmdb_id=tmdb_id, db_index=db_index, title=title
    )
    rows: dict[str, dict] = {}
    for session in selected:
        ident = classify_identity(session, token_map)
        row = rows.setdefault(ident["key"], {
            "key": ident["key"], "label": ident["label"], "username": ident.get("username"),
            "telegram_id": ident.get("telegram_id"), "mode": ident["mode"],
            "shared_token": ident.get("shared_token", False),
            "plays": 0, "seconds": 0.0, "completions": [], "devices": {},
            "place": ident.get("place") or "", "isp": ident.get("isp") or "",
            "ip": session.get("client_ip"), "last_ts": 0.0, "sources": {},
        })
        row["plays"] += 1
        row["seconds"] += session.get("seconds") or 0.0
        if isinstance(session.get("completion"), (int, float)):
            row["completions"].append(session["completion"])
        device = session.get("device") or "Unknown"
        row["devices"][device] = row["devices"].get(device, 0) + 1
        source = session.get("source_type") or "telegram"
        row["sources"][source] = row["sources"].get(source, 0) + 1
        row["last_ts"] = max(row["last_ts"], session.get("last_ts") or 0)
        geo = session.get("geo") or {}
        if not row["place"]:
            row["place"] = ", ".join(p for p in (geo.get("city"), geo.get("country")) if p)
        if not row["isp"]:
            row["isp"] = geo.get("isp") or ""
        if not row["ip"]:
            row["ip"] = session.get("client_ip")

    viewer_rows = sorted(rows.values(), key=lambda r: (-r["plays"], r["label"]))
    for row in viewer_rows:
        row["watch_time"] = _fmt_seconds(row["seconds"])
        row["seconds"] = round(row["seconds"], 1)
        row["avg_completion"] = _avg(row.pop("completions"))
        row["last_seen"] = (
            datetime.utcfromtimestamp(row["last_ts"]).strftime("%Y-%m-%d %H:%M")
            if row["last_ts"] else None
        )

    return {
        "viewers": viewer_rows,
        "plays": len(selected),
        "unique_viewers": len(viewer_rows),
        "total_seconds": round(sum(s.get("seconds") or 0.0 for s in selected), 1),
        "sessions": [
            {
                "when": datetime.utcfromtimestamp(s["last_ts"]).strftime("%Y-%m-%d %H:%M")
                if s.get("last_ts") else None,
                "viewer": classify_identity(s, token_map)["label"],
                "device": s.get("device"),
                "app": s.get("app"),
                "seconds": round(s.get("seconds") or 0.0, 1),
                "completion": s.get("completion"),
                "city": (s.get("geo") or {}).get("city"),
                "country": (s.get("geo") or {}).get("country"),
                "source_type": s.get("source_type"),
            }
            for s in sorted(selected, key=lambda s: s.get("last_ts") or 0, reverse=True)[:100]
        ],
    }


def _select_title_sessions(
    sessions: list[dict], *, imdb_id=None, tmdb_id=None, db_index=None, title=None
) -> list[dict]:
    selected = []
    title_norm = (title or "").strip().lower()
    for session in sessions:
        media = session.get("media") or {}
        if imdb_id and media.get("imdb_id") == imdb_id:
            selected.append(session)
            continue
        if tmdb_id is not None and media.get("tmdb_id") is not None and str(media.get("tmdb_id")) == str(tmdb_id):
            if db_index is None or media.get("db_index") is None or int(media.get("db_index")) == int(db_index):
                selected.append(session)
                continue
        if imdb_id is None and tmdb_id is None and title_norm:
            label = (media.get("title") or session.get("title") or "").strip().lower()
            if label and (label == title_norm or title_norm in label or label in title_norm):
                selected.append(session)
    return selected


def viewer_profile(
    sessions: list[dict], token_map: Optional[dict] = None, *, viewer_key: Optional[str] = None, token: Optional[str] = None
) -> dict:
    """Full history + splits for one viewer identity (or one token)."""
    token_map = token_map or {}
    mine = []
    for session in sessions:
        ident = classify_identity(session, token_map)
        if viewer_key and ident["key"] == viewer_key:
            mine.append((session, ident))
        elif token and str(session.get("token")) == str(token):
            mine.append((session, ident))
    if not mine:
        return {"viewer": None, "history": [], "totals": {}, "devices": [], "places": [], "apps": []}

    ident = dict(mine[-1][1])
    latest_geo = mine[-1][0].get("geo") or {}
    ident["place"] = ", ".join(p for p in (latest_geo.get("city"), latest_geo.get("country")) if p)
    ident["isp"] = latest_geo.get("isp") or ""
    devices: dict[str, int] = {}
    places: dict[str, int] = {}
    apps: dict[str, int] = {}
    total_seconds = 0.0
    total_bytes = 0
    completions: list = []
    history = []
    for session, _ident in sorted(mine, key=lambda p: p[0].get("last_ts") or 0, reverse=True):
        seconds = session.get("seconds") or 0.0
        total_seconds += seconds
        total_bytes += session.get("bytes") or 0
        if isinstance(session.get("completion"), (int, float)):
            completions.append(session["completion"])
        device = session.get("device") or "Unknown"
        devices[device] = devices.get(device, 0) + 1
        app = session.get("app") or "Unknown"
        apps[app] = apps.get(app, 0) + 1
        geo = session.get("geo") or {}
        place = ", ".join(p for p in (geo.get("city"), geo.get("country")) if p) or "Unknown"
        places[place] = places.get(place, 0) + 1
        history.append({
            "when": datetime.utcfromtimestamp(session["last_ts"]).strftime("%Y-%m-%d %H:%M")
            if session.get("last_ts") else None,
            "title": media_label_of_session(session),
            "media": session.get("media"),
            "device": session.get("device"),
            "app": session.get("app"),
            "seconds": round(seconds, 1),
            "watch_time": _fmt_seconds(seconds),
            "completion": session.get("completion"),
            "city": geo.get("city"),
            "country": geo.get("country"),
            "ip": session.get("client_ip"),
            "isp": geo.get("isp"),
            "source_type": session.get("source_type"),
        })

    return {
        "viewer": ident,
        "history": history[:200],
        "totals": {
            "plays": len(mine),
            "watch_time": _fmt_seconds(total_seconds),
            "seconds": round(total_seconds, 1),
            "bytes": total_bytes,
            "unique_titles": len({media_key(s) for s, _ in mine}),
            "avg_completion": _avg(completions),
        },
        "devices": [{"device": k, "plays": v} for k, v in sorted(devices.items(), key=lambda kv: -kv[1])],
        "apps": [{"app": k, "plays": v} for k, v in sorted(apps.items(), key=lambda kv: -kv[1])],
        "places": [{"place": k, "plays": v} for k, v in sorted(places.items(), key=lambda kv: -kv[1])],
    }


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------
def csv_rows(kind: str, sessions: list[dict], token_map: Optional[dict] = None):
    """(headers, rows) for scope: plays | titles | viewers."""
    token_map = token_map or {}
    if kind == "titles":
        summary = summarize_overview(sessions, token_map)
        headers = ["title", "plays", "unique_viewers", "watch_time", "last_watched",
                   "imdb_id", "tmdb_id", "media_type", "season", "episode"]
        rows = []
        for t in summary["top_titles"]:
            media = t.get("media") or {}
            rows.append([
                t["label"], t["plays"], t["viewer_count"], _fmt_seconds(t["seconds"]),
                datetime.utcfromtimestamp(t["last_ts"]).strftime("%Y-%m-%d %H:%M") if t["last_ts"] else "",
                media.get("imdb_id") or "", media.get("tmdb_id") or "", media.get("media_type") or "",
                media.get("season") if media.get("season") is not None else "",
                media.get("episode") if media.get("episode") is not None else "",
            ])
        return headers, rows

    if kind == "viewers":
        summary = summarize_overview(sessions, token_map)
        headers = ["viewer", "mode", "telegram_id", "username", "plays", "watch_time",
                   "last_seen", "place", "isp", "shared_token"]
        rows = [
            [v["label"], v["mode"], v.get("telegram_id") or "", v.get("username") or "",
             v["plays"], v["watch_time"], v.get("last_seen") or "", v.get("place") or "",
             v.get("isp") or "", "yes" if v.get("shared_token") else "no"]
            for v in summary["top_viewers"]
        ]
        return headers, rows

    headers = ["when", "viewer", "mode", "telegram_id", "username", "title", "imdb_id",
               "season", "episode", "device", "app", "city", "country", "isp", "ip",
               "watch_time", "completion", "bytes", "source_type"]
    rows = []
    for session in sorted(sessions, key=lambda s: s.get("last_ts") or 0, reverse=True):
        ident = classify_identity(session, token_map)
        media = session.get("media") or {}
        geo = session.get("geo") or {}
        completion = session.get("completion")
        rows.append([
            datetime.utcfromtimestamp(session["last_ts"]).strftime("%Y-%m-%d %H:%M") if session.get("last_ts") else "",
            ident["label"], ident["mode"], ident.get("telegram_id") or "",
            ident.get("username") or "", media_label_of_session(session),
            media.get("imdb_id") or "",
            media.get("season") if media.get("season") is not None else "",
            media.get("episode") if media.get("episode") is not None else "",
            session.get("device") or "", session.get("app") or "",
            geo.get("city") or "", geo.get("country") or "", geo.get("isp") or "",
            session.get("client_ip") or "",
            _fmt_seconds(session.get("seconds") or 0.0),
            f"{round(completion * 100)}%" if isinstance(completion, (int, float)) else "",
            session.get("bytes") or 0, session.get("source_type") or "",
        ])
    return headers, rows


def digest_message(sessions: list[dict], token_map: Optional[dict] = None, *, day_label: str = "yesterday") -> Optional[str]:
    """Owner DM text for one day of activity (None when nothing to report)."""
    token_map = token_map or {}
    if not sessions:
        return None
    summary = summarize_overview(sessions, token_map)
    totals = summary["totals"]
    lines = [
        f"📊 <b>Viewer Insights — {day_label}</b>",
        f"▶️ {totals['plays']} play(s) · 👤 {totals['unique_viewers']} viewer(s) · ⏱ {totals['total_watch_time']}",
    ]
    if totals.get("avg_completion") is not None:
        lines.append(f"✅ Avg completion: {round(totals['avg_completion'] * 100)}%")
    if summary["top_titles"]:
        lines.append("\n<b>Top titles</b>")
        for t in summary["top_titles"][:5]:
            lines.append(f"• {t['label']} — {t['plays']} play(s), {t['viewer_count']} viewer(s)")
    if summary["top_viewers"]:
        lines.append("\n<b>Viewers</b>")
        for v in summary["top_viewers"][:6]:
            who = v["label"]
            place = f" · {v['place']}" if v.get("place") else ""
            lines.append(f"• {who}{place} — {v['plays']} play(s), {v['watch_time']}")
    return "\n".join(lines)
