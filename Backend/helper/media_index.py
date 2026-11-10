"""Container seek-index parsing for Telegram-backed streams.

Parses MKV Cues and MP4 moov/stbl boxes to map keyframe time <-> byte offset
and extract duration. Builds lazily on the first playback request, using only
small aligned Telegram range reads. The index is an in-memory aid for future
seek-aware streaming decisions; HTTP byte-range behavior remains unchanged.

Everything is best-effort: parse surprises return ``None`` and are
negative-cached. The cache is bounded and memory-only: no disk cache, no Mongo
persistence, no ingestion-time crawl, and no third-party parser dependencies.
"""

import asyncio
import math
import time
from bisect import bisect_right
from typing import List, Optional, Tuple

from Backend.config import Telegram
from Backend.logger import LOGGER

# ---------------------------------------------------------------------------
# Matroska element IDs
# ---------------------------------------------------------------------------
_EBML_MAGIC = b"\x1a\x45\xdf\xa3"
_MKV_SEGMENT_ID = 0x18538067
_MKV_SEEKHEAD_ID = 0x114D9B74
_MKV_SEEK_ID = 0x4DBB
_MKV_SEEKID_ID = 0x53AB
_MKV_SEEKPOS_ID = 0x53AC
_MKV_INFO_ID = 0x1549A966
_MKV_TIMESTAMP_SCALE_ID = 0x2AD7B1
_MKV_DURATION_ID = 0x4489
_MKV_CUES_ID = 0x1C53BB6B
_MKV_CUES_BYTES = b"\x1c\x53\xbb\x6b"
_MKV_CUE_POINT_ID = 0xBB
_MKV_CUE_TIME_ID = 0xB3
_MKV_CUE_TRACK_POS_ID = 0xB7
_MKV_CUE_CLUSTER_POS_ID = 0xF1

_MP4_FTYP = b"ftyp"
_MP4_MOOV = b"moov"
_MP4_MVHD = b"mvhd"
_MP4_TRAK = b"trak"
_MP4_MDIA = b"mdia"
_MP4_HDLR = b"hdlr"
_MP4_MINF = b"minf"
_MP4_STBL = b"stbl"
_MP4_STTS = b"stts"
_MP4_STSC = b"stsc"
_MP4_STCO = b"stco"
_MP4_CO64 = b"co64"
_MP4_STSS = b"stss"


# ---------------------------------------------------------------------------
# MediaIndex
# ---------------------------------------------------------------------------
class MediaIndex:
    """Parsed seek index: sorted (time_sec, byte_offset) keyframe pairs."""

    __slots__ = ("container", "duration_sec", "keyframes", "_times", "_bytes")

    def __init__(self, container: str, duration_sec: Optional[float], keyframes: List[Tuple[float, int]]):
        self.container = container
        self.duration_sec = duration_sec if duration_sec and duration_sec > 0 else None
        self.keyframes = keyframes
        self._times = [t for t, _ in keyframes]
        self._bytes = [b for _, b in keyframes]

    # -- queries -----------------------------------------------------------
    def byte_to_time(self, byte_offset: int) -> Optional[float]:
        """Approximate media time (sec) at a byte offset."""
        if not self._bytes:
            return None
        i = bisect_right(self._bytes, byte_offset) - 1
        if i < 0:
            return self._times[0]
        return self._times[i]

    def time_to_byte(self, time_sec: float) -> Optional[int]:
        """Byte offset of the last keyframe at or before ``time_sec`` — the
        keyframe a player actually requests when seeking to that time."""
        if not self._times:
            return None
        i = bisect_right(self._times, time_sec) - 1
        if i < 0:
            return self._bytes[0]
        return self._bytes[i]

    def seek_delta_byte(self, current_byte: int, delta_sec: float, file_size: int = 0) -> Optional[int]:
        """Byte offset for ``delta_sec`` seconds from the position at ``current_byte``."""
        t = self.byte_to_time(current_byte)
        if t is None:
            return None
        target = t + delta_sec
        if target < 0:
            target = 0.0
        if self.duration_sec and target > self.duration_sec:
            target = self.duration_sec
        off = self.time_to_byte(target)
        if off is None:
            return None
        if file_size and off >= file_size:
            off = self._bytes[-1]
        return max(0, off)

    def bitrate_bps(self, file_size: int) -> Optional[float]:
        """Average bytes/sec needed to play this file in real time."""
        if self.duration_sec and file_size > 0:
            return file_size / self.duration_sec
        return None

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def downsample(keyframes: List[Tuple[float, int]], cap: int) -> List[Tuple[float, int]]:
        if cap <= 0 or len(keyframes) <= cap:
            return keyframes
        step = math.ceil(len(keyframes) / cap)
        out = keyframes[::step]
        if out and out[-1] is not keyframes[-1]:
            out.append(keyframes[-1])
        return out


# ---------------------------------------------------------------------------
# Minimal EBML (Matroska) reading
# ---------------------------------------------------------------------------
def _ebml_read_vint(data: bytes, pos: int):
    """Return (raw_id_int, data_value, vint_len, size_unknown)."""
    first = data[pos]
    if not first:
        raise ValueError("zero byte where vint expected")
    n = 1
    mask = 0x80
    while not (first & mask):
        mask >>= 1
        n += 1
        if n > 8:
            raise ValueError("bad vint length")
    if pos + n > len(data):
        raise ValueError("truncated vint")
    raw = data[pos : pos + n]
    raw_int = int.from_bytes(raw, "big")
    data_val = raw_int & ((1 << (7 * n)) - 1)
    unknown = n < 8 and data_val == (1 << (7 * n)) - 1
    return raw_int, data_val, n, unknown


def _iter_ebml_elements(data: bytes, start: int, end: int):
    """Yield (element_id, data_start, data_end) over an EBML buffer slice."""
    pos = start
    while pos + 2 <= end:
        try:
            eid, _eid_data, id_len, _id_unknown = _ebml_read_vint(data, pos)
            _sz_raw, size_val, size_len, size_unknown = _ebml_read_vint(data, pos + id_len)
        except (ValueError, IndexError):
            return
        header_len = id_len + size_len
        data_start = pos + header_len
        if size_unknown or size_val < 0:
            data_end = end
        else:
            data_end = data_start + size_val
        if data_end > end:
            data_end = end
        yield eid, data_start, data_end
        if data_end <= pos:
            return
        pos = data_end


def _parse_mkv_segment_start(head_bytes: bytes) -> Optional[int]:
    """Byte offset of the Segment element's payload (cluster positions are relative to it)."""
    try:
        for eid, dstart, dend in _iter_ebml_elements(head_bytes, 0, len(head_bytes)):
            if eid == _MKV_SEGMENT_ID:
                return dstart
            if eid == _EBML_MAGIC_INT:
                continue
            # Info / SeekHead usually come after Segment; if we see them first
            # something is odd — keep scanning anyway.
        return None
    except Exception:
        return None


_EBML_MAGIC_INT = int.from_bytes(_EBML_MAGIC, "big")


def _parse_mkv_info(head_bytes: bytes) -> Tuple[float, Optional[float]]:
    """Return (timestamp_scale_ns, duration_sec_or_None) from the Segment Info."""
    scale_ns = 1_000_000.0
    duration = None
    try:
        for eid, dstart, dend in _iter_ebml_elements(head_bytes, 0, len(head_bytes)):
            if eid != _MKV_SEGMENT_ID:
                continue
            for cid, cstart, cend in _iter_ebml_elements(head_bytes, dstart, dend):
                if cid != _MKV_INFO_ID:
                    continue
                for fid, fstart, fend in _iter_ebml_elements(head_bytes, cstart, cend):
                    if fid == _MKV_TIMESTAMP_SCALE_ID:
                        raw = head_bytes[fstart:fend]
                        if raw:
                            scale_ns = float(int.from_bytes(raw, "big")) or 1_000_000.0
                    elif fid == _MKV_DURATION_ID:
                        raw = head_bytes[fstart:fend]
                        if len(raw) in (4, 8):
                            val = int.from_bytes(raw, "big", signed=False)
                            if len(raw) == 4:
                                import struct as _struct

                                val = _struct.unpack(">f", raw)[0]
                            else:
                                import struct as _struct

                                val = _struct.unpack(">d", raw)[0]
                            duration = val
                break
    except Exception:
        pass
    if duration is not None and scale_ns > 0:
        duration_sec = (duration * scale_ns) / 1_000_000_000.0
        if duration_sec > 0:
            return scale_ns, duration_sec
    return scale_ns, None


def parse_mkv_index(
    head_bytes: bytes,
    tail_bytes: bytes,
    file_size: int,
    cues_window: Optional[Tuple[bytes, int]] = None,
    tail_base: Optional[int] = None,
) -> Optional[MediaIndex]:
    """Build a MediaIndex from cached head/tail bytes of a Matroska file.

    ``cues_window`` (bytes, base_offset) is an explicitly-provided window that
    contains the Cues element (e.g. a targeted fetch at the offset resolved
    from the SeekHead) — tried first when present. ``tail_base`` is the
    absolute file offset where ``tail_bytes`` starts; when omitted it is
    derived as ``file_size - len(tail_bytes)``.
    """
    try:
        if not head_bytes or head_bytes[:4] != _EBML_MAGIC:
            return None
        segment_data_start = _parse_mkv_segment_start(head_bytes)
        if segment_data_start is None:
            return None
        scale_ns, duration_sec = _parse_mkv_info(head_bytes)
        if tail_base is None:
            tail_base = file_size - len(tail_bytes) if tail_bytes else file_size
        max_kf = max(64, int(getattr(Telegram, "STREAM_INDEX_MAX_KEYFRAMES", 4096) or 4096))

        def _parse_cues(buf: bytes, cstart: int, cend: int) -> List[Tuple[float, int]]:
            """Parse + validate CuePoints; empty list = not really Cues."""
            keyframes: List[Tuple[float, int]] = []
            for eid, pstart, pend in _iter_ebml_elements(buf, cstart, cend):
                if eid != _MKV_CUE_POINT_ID:
                    continue
                cue_time = None
                cluster_pos = None
                for cid, c2s, c2e in _iter_ebml_elements(buf, pstart, pend):
                    if cid == _MKV_CUE_TIME_ID:
                        raw = buf[c2s:c2e]
                        if raw:
                            cue_time = int.from_bytes(raw, "big")
                    elif cid == _MKV_CUE_TRACK_POS_ID:
                        for fid, f2s, f2e in _iter_ebml_elements(buf, c2s, c2e):
                            if fid == _MKV_CUE_CLUSTER_POS_ID:
                                raw = buf[f2s:f2e]
                                if raw:
                                    cluster_pos = int.from_bytes(raw, "big")
                if cue_time is None or cluster_pos is None:
                    continue
                abs_off = segment_data_start + cluster_pos
                if abs_off < 0 or abs_off >= file_size:
                    continue
                keyframes.append(((cue_time * scale_ns) / 1_000_000_000.0, abs_off))
                if len(keyframes) >= max_kf * 4:
                    break
            return keyframes

        def _try_window(buf: bytes, base: int) -> Optional[List[Tuple[float, int]]]:
            """Try every `Cues`-pattern match in a window; first that parses to
            >=2 keyframes wins (rejects SeekID false-positives)."""
            search_from = 0
            while True:
                idx = buf.find(_MKV_CUES_BYTES, search_from)
                if idx < 0:
                    return None
                search_from = idx + 1
                span = _find_mkv_cues_element(buf, idx)
                if span is None:
                    continue
                kfs = _parse_cues(buf, span[0], span[1])
                if len(kfs) >= 2:
                    return kfs
            return None

        keyframes = None
        if cues_window is not None:
            keyframes = _try_window(cues_window[0], cues_window[1])
        if keyframes is None:
            keyframes = _try_window(head_bytes, 0)
        if keyframes is None and tail_bytes:
            keyframes = _try_window(tail_bytes, tail_base)

        if keyframes is None:
            # Resolve Cues via the SeekHead (points at the absolute Cues
            # position; may live in neither cached window for big files).
            cues_abs = _resolve_cues_offset_via_seekhead(head_bytes, segment_data_start)
            if cues_abs is not None:
                if 0 <= cues_abs < len(head_bytes):
                    span = _find_mkv_cues_element(head_bytes, cues_abs)
                    if span:
                        keyframes = _parse_cues(head_bytes, span[0], span[1])
                elif tail_bytes and tail_base <= cues_abs < file_size:
                    rel = cues_abs - tail_base
                    span = _find_mkv_cues_element(tail_bytes, rel)
                    if span:
                        keyframes = _parse_cues(tail_bytes, span[0], span[1])

        if not keyframes or len(keyframes) < 2:
            return None
        keyframes.sort(key=lambda p: p[0])
        if duration_sec is None:
            duration_sec = keyframes[-1][0]
        return MediaIndex("mkv", duration_sec, MediaIndex.downsample(keyframes, max_kf))
    except Exception as exc:
        LOGGER.debug("mkv index parse failed: %s", exc)
        return None


def _find_mkv_cues_element(data: bytes, idx: int) -> Optional[Tuple[int, int]]:
    """Given the offset of a Cues element ID inside ``data``, return its
    (data_start, data_end) if the element fits in the buffer, else None."""
    try:
        _, size_val, size_len, size_unknown = _ebml_read_vint(data, idx + 4)
    except (ValueError, IndexError):
        return None
    data_start = idx + 4 + size_len
    if size_unknown:
        return (data_start, len(data))
    data_end = data_start + size_val
    if data_end <= len(data):
        return (data_start, data_end)
    return None


def _resolve_cues_offset_via_seekhead(head_bytes: bytes, segment_data_start: int) -> Optional[int]:
    """Absolute file offset of the Cues element from SeekHead entries."""
    try:
        for eid, ds, de in _iter_ebml_elements(head_bytes, segment_data_start, len(head_bytes)):
            if eid == _MKV_CUES_ID:
                return ds
            if eid != _MKV_SEEKHEAD_ID:
                continue
            for sid, ss, se in _iter_ebml_elements(head_bytes, ds, de):
                if sid != _MKV_SEEK_ID:
                    continue
                target_id = None
                pos = None
                for fid, fs, fe in _iter_ebml_elements(head_bytes, ss, se):
                    if fid == _MKV_SEEKID_ID:
                        raw = head_bytes[fs:fe]
                        if raw:
                            target_id = int.from_bytes(raw, "big")
                    elif fid == _MKV_SEEKPOS_ID:
                        raw = head_bytes[fs:fe]
                        if raw:
                            pos = int.from_bytes(raw, "big")
                if target_id == _MKV_CUES_ID and pos is not None:
                    return segment_data_start + pos
        return None
    except Exception:
        return None


def resolve_mkv_cues_offset(head_bytes: bytes) -> Optional[int]:
    """Absolute file offset of the Cues element, resolved from the SeekHead."""
    if not head_bytes or head_bytes[:4] != _EBML_MAGIC:
        return None
    seg = _parse_mkv_segment_start(head_bytes)
    if seg is None:
        return None
    return _resolve_cues_offset_via_seekhead(head_bytes, seg)


# ---------------------------------------------------------------------------
# Minimal MP4 box reading
# ---------------------------------------------------------------------------
def _iter_boxes(data: bytes, start: int, end: int):
    """Yield (box_type_bytes, payload_start, payload_end) over an MP4 buffer slice."""
    pos = start
    while pos + 8 <= end:
        size = int.from_bytes(data[pos : pos + 4], "big")
        typ = data[pos + 4 : pos + 8]
        header = 8
        if size == 1:
            if pos + 16 > end:
                return
            size = int.from_bytes(data[pos + 8 : pos + 16], "big")
            header = 16
        elif size == 0:
            size = end - pos
        if size < header or pos + size > end:
            # Box claims to extend past the buffer — only usable if this is the
            # final fragment of the slice; stop instead of yielding garbage.
            return
        yield typ, pos + header, pos + size
        pos += size


def _find_box(data: bytes, start: int, end: int, box_type: bytes):
    for typ, ps, pe in _iter_boxes(data, start, end):
        if typ == box_type:
            return ps, pe
    return None


def _parse_mvhd(data: bytes, start: int, end: int) -> Optional[Tuple[int, float]]:
    """Return (timescale, duration_sec) from an mvhd box payload."""
    try:
        version = data[start]
        if version == 1:
            timescale = int.from_bytes(data[start + 20 : start + 24], "big")
            duration = int.from_bytes(data[start + 24 : start + 32], "big")
        else:
            timescale = int.from_bytes(data[start + 12 : start + 16], "big")
            duration = int.from_bytes(data[start + 16 : start + 20], "big")
        if timescale > 0:
            return timescale, duration / timescale
        return None
    except Exception:
        return None


def _is_video_trak(data: bytes, trak_start: int, trak_end: int) -> bool:
    span = _find_box(data, trak_start, trak_end, _MP4_MDIA)
    if not span:
        return False
    hdlr = _find_box(data, span[0], span[1], _MP4_HDLR)
    if not hdlr:
        return False
    try:
        return data[hdlr[0] + 8 : hdlr[0] + 12] == b"vide"
    except Exception:
        return False


def _parse_stbl(data: bytes, stbl_start: int, stbl_end: int, timescale: int) -> Optional[List[Tuple[float, int]]]:
    """Build (time_sec, byte) keyframe pairs from stts/stsc/stco/stss."""
    stts = stsc = stco = stss = None
    for typ, ps, pe in _iter_boxes(data, stbl_start, stbl_end):
        if typ == _MP4_STTS:
            stts = (ps, pe)
        elif typ == _MP4_STSC:
            stsc = (ps, pe)
        elif typ in (_MP4_STCO, _MP4_CO64):
            stco = (ps, pe, typ == _MP4_CO64)
        elif typ == _MP4_STSS:
            stss = (ps, pe)
    if not stts or not stsc or not stco:
        return None
    try:
        wide = stco[2]

        # stts: expand (count, delta) runs lazily
        t_ps, t_pe = stts
        t_entries = int.from_bytes(data[t_ps + 4 : t_ps + 8], "big")
        t_runs = []
        pos = t_ps + 8
        for _ in range(t_entries):
            if pos + 8 > t_pe:
                break
            cnt = int.from_bytes(data[pos : pos + 4], "big")
            delta = int.from_bytes(data[pos + 4 : pos + 8], "big")
            t_runs.append([cnt, delta])
            pos += 8

        # stsc: runs of (first_chunk, samples_per_chunk)
        c_ps, c_pe = stsc
        c_entries = int.from_bytes(data[c_ps + 4 : c_ps + 8], "big")
        c_runs = []
        pos = c_ps + 8
        for _ in range(c_entries):
            if pos + 12 > c_pe:
                break
            first_chunk = int.from_bytes(data[pos : pos + 4], "big")
            per_chunk = int.from_bytes(data[pos + 4 : pos + 8], "big")
            c_runs.append((first_chunk, per_chunk))
            pos += 12

        # stco: chunk byte offsets
        o_ps, o_pe = stco[0], stco[1]
        o_entries = int.from_bytes(data[o_ps + 4 : o_ps + 8], "big")
        esize = 8 if wide else 4
        chunk_offsets = []
        pos = o_ps + 8
        for _ in range(o_entries):
            if pos + esize > o_pe:
                break
            chunk_offsets.append(int.from_bytes(data[pos : pos + esize], "big"))
            pos += esize
        if not chunk_offsets or not c_runs:
            return None

        # stss: 1-based sync sample numbers (absent → every sample is sync)
        sync_set = None
        if stss:
            s_ps, s_pe = stss
            s_entries = int.from_bytes(data[s_ps + 4 : s_ps + 8], "big")
            sync_set = set()
            pos = s_ps + 8
            for _ in range(s_entries):
                if pos + 4 > s_pe:
                    break
                sync_set.add(int.from_bytes(data[pos : pos + 4], "big"))
                pos += 4

        max_kf = max(64, int(getattr(Telegram, "STREAM_INDEX_MAX_KEYFRAMES", 4096) or 4096))
        keyframes: List[Tuple[float, int]] = []

        # Walk chunks, expanding stts runs as we consume samples.
        run_i = 0
        run_remaining = t_runs[0][0] if t_runs else 0
        run_delta = t_runs[0][1] if t_runs else 0
        sample_dts = 0  # in timescale units
        sample_number = 1  # 1-based

        for chunk_i, chunk_off in enumerate(chunk_offsets):
            per_chunk_now = per_chunk_for_chunk(c_runs, chunk_i + 1)
            chunk_has_sync = False
            chunk_first_time = sample_dts
            for _ in range(per_chunk_now):
                if sync_set is None or sample_number in sync_set:
                    chunk_has_sync = True
                # advance time by this sample's delta
                while run_remaining <= 0 and run_i + 1 < len(t_runs):
                    run_i += 1
                    run_remaining = t_runs[run_i][0]
                    run_delta = t_runs[run_i][1]
                sample_dts += run_delta
                run_remaining -= 1
                sample_number += 1
                if run_remaining <= 0 and run_i + 1 < len(t_runs):
                    run_i += 1
                    run_remaining = t_runs[run_i][0]
                    run_delta = t_runs[run_i][1]
            if chunk_has_sync:
                keyframes.append((chunk_first_time / timescale, chunk_off))
                if len(keyframes) >= max_kf * 4:
                    break
        if len(keyframes) < 2:
            return None
        keyframes.sort(key=lambda p: p[0])
        return MediaIndex.downsample(keyframes, max_kf)
    except Exception as exc:
        LOGGER.debug("mp4 stbl parse failed: %s", exc)
        return None


def per_chunk_for_chunk(c_runs, chunk_number: int) -> int:
    """Samples per chunk for a 1-based chunk number from stsc runs."""
    per = c_runs[0][1]
    for i, (first_chunk, samples) in enumerate(c_runs):
        if chunk_number >= first_chunk:
            per = samples
        else:
            break
    return per


def parse_mp4_index(
    head_bytes: bytes,
    tail_bytes: bytes,
    file_size: int,
) -> Optional[MediaIndex]:
    """Build a MediaIndex from cached head/tail bytes of an MP4 file."""
    try:
        if not head_bytes or head_bytes[4:8] != _MP4_FTYP:
            return None

        moov_span = _find_box(head_bytes, 0, len(head_bytes), _MP4_MOOV)
        if moov_span:
            moov_bytes, moov_base = head_bytes, 0
        else:
            # moov at end (non-faststart): locate in tail by pattern + size check
            idx = tail_bytes.find(_MP4_MOOV)
            moov_bytes = None
            while idx >= 4:
                size = int.from_bytes(tail_bytes[idx - 4 : idx], "big")
                if 8 <= size and idx - 4 + size <= len(tail_bytes):
                    moov_bytes = tail_bytes
                    moov_span = (idx + 4, idx - 4 + size)
                    moov_base = 0
                    break
                idx = tail_bytes.find(_MP4_MOOV, idx + 1)
            if moov_bytes is None:
                return None
        if moov_span is None:
            return None

        mvhd = _find_box(moov_bytes, moov_span[0], moov_span[1], _MP4_MVHD)
        if not mvhd:
            return None
        parsed = _parse_mvhd(moov_bytes, mvhd[0], mvhd[1])
        if not parsed:
            return None
        timescale, duration_sec = parsed
        if timescale <= 0:
            return None

        pos = moov_span[0]
        end = moov_span[1]
        while pos + 8 <= end:
            size = int.from_bytes(moov_bytes[pos : pos + 4], "big")
            typ = moov_bytes[pos + 4 : pos + 8]
            if size < 8 or pos + size > end:
                break
            if typ == _MP4_TRAK and _is_video_trak(moov_bytes, pos + 8, pos + size):
                mdia = _find_box(moov_bytes, pos + 8, pos + size, _MP4_MDIA)
                if not mdia:
                    break
                minf = _find_box(moov_bytes, mdia[0], mdia[1], _MP4_MINF)
                if not minf:
                    break
                stbl = _find_box(moov_bytes, minf[0], minf[1], _MP4_STBL)
                if not stbl:
                    break
                kfs = _parse_stbl(moov_bytes, stbl[0], stbl[1], timescale)
                if kfs:
                    return MediaIndex("mp4", duration_sec, kfs)
                return None
            pos += size
        return None
    except Exception as exc:
        LOGGER.debug("mp4 index parse failed: %s", exc)
        return None


# ---------------------------------------------------------------------------


# Caitlin-safe media index integration. No disk writes and no Mongo persistence.
# Builds are lazy (first playback only), globally serialized, and strictly bounded.
import asyncio
import time
from collections import OrderedDict
from typing import Optional, Tuple

from Backend.config import Telegram
from Backend.logger import LOGGER

_INDEX_CACHE = OrderedDict()
_INDEX_LOCK = asyncio.Lock()
_BUILD_SEMAPHORE = asyncio.Semaphore(1)
_IN_FLIGHT = {}
_POSITIVE_TTL = 4 * 60 * 60
_NEGATIVE_TTL = 15 * 60

def _cache_limit():
    try:
        return max(1, min(8, int(getattr(Telegram, "STREAM_INDEX_CACHE_MAX_ENTRIES", 8))))
    except Exception:
        return 8

def get_index_cache_stats():
    parsed = sum(1 for item in _INDEX_CACHE.values() if item[0] is not None)
    return {"enabled": bool(getattr(Telegram, "STREAM_INDEX_ENABLED", True)),
            "entries": len(_INDEX_CACHE), "parsed": parsed,
            "negative": len(_INDEX_CACHE)-parsed, "max_entries": _cache_limit(),
            "persistent": False, "disk_bytes": 0}

async def _cached_entry(key):
    async with _INDEX_LOCK:
        item=_INDEX_CACHE.get(key)
        if item is None:
            return False, None
        idx, stamp=item
        ttl=_POSITIVE_TTL if idx is not None else _NEGATIVE_TTL
        if time.monotonic()-stamp > ttl:
            _INDEX_CACHE.pop(key, None)
            return False, None
        _INDEX_CACHE.move_to_end(key)
        return True, idx

async def get_media_index(chat_id: int, message_id: int):
    _present, idx = await _cached_entry((int(chat_id), int(message_id)))
    return idx

async def _store_index(key, idx):
    async with _INDEX_LOCK:
        _INDEX_CACHE[key]=(idx, time.monotonic())
        _INDEX_CACHE.move_to_end(key)
        while len(_INDEX_CACHE)>_cache_limit():
            _INDEX_CACHE.popitem(last=False)

async def build_media_index(file_id, streamer, chat_id=None, message_id=None):
    """Build a best-effort seek index on demand; never persists bytes or indexes."""
    if not bool(getattr(Telegram, "STREAM_INDEX_ENABLED", True)):
        return None
    chat_id=int(chat_id if chat_id is not None else getattr(file_id, "chat_id", 0) or 0)
    message_id=int(message_id if message_id is not None else getattr(file_id, "message_id", 0) or 0)
    if not chat_id or not message_id:
        return None
    key=(chat_id, message_id)
    present, cached=await _cached_entry(key)
    if present:
        return cached
    existing=_IN_FLIGHT.get(key)
    if existing and not existing.done():
        try:
            return await existing
        except Exception:
            return None
    task=asyncio.create_task(_build(file_id, streamer, key))
    _IN_FLIGHT[key]=task
    try:
        return await task
    finally:
        if _IN_FLIGHT.get(key) is task:
            _IN_FLIGHT.pop(key, None)

async def _build(file_id, streamer, key):
    async with _BUILD_SEMAPHORE:
        try:
            size=int(getattr(file_id, "file_size", 0) or 0)
            grid=512*1024
            if size <= 2*grid:
                await _store_index(key, None)
                return None
            session=await streamer._get_media_session(file_id)
            location=await streamer._get_location(file_id)
            fetched=0
            async def fetch_window(offset, wanted):
                nonlocal fetched
                start=max(0, (int(offset)//grid)*grid)
                end=min(size, int(offset)+int(wanted))
                out=bytearray()
                pos=start
                # Hard cap per file. Never allocate a full media file.
                while pos<end and fetched < 4*1024*1024:
                    request_size=grid
                    # Telegram may return a short final block at EOF; still
                    # request the full aligned grid size to satisfy its offset rules.
                    from Backend.helper.custom_dl import _request_file
                    from wzgram import raw
                    result=await asyncio.wait_for(_request_file(session, raw.functions.upload.GetFile(
                        location=location, offset=pos, limit=request_size)), timeout=12)
                    chunk=getattr(result, "bytes", b"") or b""
                    if not chunk:
                        break
                    fetched += len(chunk)
                    out.extend(chunk)
                    if len(chunk)<request_size:
                        break
                    pos += len(chunk)
                return bytes(out), start

            head, _=await fetch_window(0, grid)
            if not head:
                await _store_index(key, None); return None
            tail_offset=max(0, size-grid)
            tail, tail_base=await fetch_window(tail_offset, grid)
            if not tail:
                await _store_index(key, None); return None
            kind=container_guess(head)
            idx=None
            if kind=="mkv":
                idx=parse_mkv_index(head, tail, size, tail_base=tail_base)
            elif kind=="mp4":
                idx=parse_mp4_index(head, tail, size)
            else:
                idx=parse_mkv_index(head, tail, size, tail_base=tail_base) or parse_mp4_index(head, tail, size)

            # Targeted MKV Cues retry: one additional 512 KiB-aligned window.
            if idx is None and kind in ("", "mkv") and head[:4]==_EBML_MAGIC and fetched < 3*1024*1024:
                cues=resolve_mkv_cues_offset(head)
                if cues is not None and cues>=len(head):
                    cue_buf, cue_base=await fetch_window(cues, grid)
                    if cue_buf:
                        idx=parse_mkv_index(head, b"", size, cues_window=(cue_buf, cue_base))

            # MP4 moov-at-end fallback is bounded to at most 2 MiB of extra data.
            if idx is None and kind in ("", "mp4") and fetched < 2*1024*1024:
                big_tail, big_base=await fetch_window(max(0, size-2*1024*1024), 2*1024*1024)
                if big_tail:
                    idx=parse_mp4_index(head, big_tail, size)
                    if idx is None and kind=="":
                        idx=parse_mkv_index(head, big_tail, size, tail_base=big_base)
            await _store_index(key, idx)
            if idx is not None:
                LOGGER.info("MediaIndex: parsed %s for %s (%d keyframes, %.1fs)", idx.container, key, len(idx.keyframes), idx.duration_sec or 0)
            else:
                LOGGER.debug("MediaIndex: no usable index for %s (%s)", key, kind)
            return idx
        except Exception as exc:
            LOGGER.debug("MediaIndex build skipped for %s: %s", key, exc)
            try:
                await _store_index(key, None)
            except Exception:
                pass
            return None

def container_guess(head_bytes: bytes) -> str:
    if head_bytes[:4] == _EBML_MAGIC:
        return "mkv"
    if head_bytes[4:8] == _MP4_FTYP:
        return "mp4"
    return ""
