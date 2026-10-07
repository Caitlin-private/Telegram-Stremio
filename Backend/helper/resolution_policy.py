"""Shared source-resolution admission rules; never transcode or invent quality."""
import re
from Backend.helper.ingestion_rules import resolution_hint

RESOLUTIONS = ('360p', '480p', '720p', '1080p', '1440p', '2160p', 'other')


def source_resolution(*texts):
    # Include uncommon pixel labels in "other", with explicit pixels first.
    for text in texts:
        match = re.search(r'(?i)(?<![a-z0-9])(\d{3,5})[pi](?![a-z0-9])', text or '')
        if match:
            return match[1] + 'p'
    return resolution_hint(*texts)


def rejection_reason(quality, settings=None):
    if settings is None:
        from Backend.helper.settings_manager import SettingsManager
        settings = SettingsManager.current()
    if not quality or str(quality).lower() in ('unknown', 'n/a', 'none'):
        return None if settings.allow_unknown_resolution else 'No resolution was found in the filename or caption; unknown-resolution ingestion is disabled.'
    quality = str(quality).lower().replace('i', 'p')
    category = quality if quality in RESOLUTIONS else 'other'
    if category not in settings.ingestion_resolutions:
        return f'{quality} is excluded by the ingestion resolution settings.'
    return None
