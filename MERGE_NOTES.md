# Upstream merge

Merged commit `03100371d521c5c3c33cbd1df7ebe6bb4b2cdf75` from
https://github.com/Caitlin-private/Telegram-Stremio into the supplied fork archive
(archive source identifier `40a093920dca696c09f0a16d4d02c384094a32b7`).

## Added

- Default library streaming via bots or connected userbot, with bot fallback during file lookup. Global search continues using the userbot.
- AIOStreams-compatible custom stream formatting, seven presets, editable templates and live preview. Default formatting remains the default.
- SpatialPosters with movie/series URL substitution, integrated into the shared poster provider and announcement provider options.
- Upstream clipboard fallback for plain HTTP, IMDb media-type search correction, subtitle input layout fix and mobile speed-test cards.

## Conflict resolutions

- Retained fork version 6.0.1 in all three version locations instead of applying upstream's 5.2.0 version bump.
- Retained Pictorium and existing poster providers; added SpatialPosters to their mutual-exclusion controls.
- Kept multipart video enable/disable behavior, labels, grouping and ordering alongside formatter support.
- Kept existing quality filtering/sorting using internal resolution labels even when custom stream names omit resolution.
- Retained fork access controls and stream notices; did not restore upstream donation rows or duplicate-name numbering removed by the fork.
- All original archive files retained. Unrelated fork code was left intact.

## Validation

- All Python source compiled successfully.
- Four offline regression tests passed, including upstream formatter self-checks for every preset, userbot/bot selection and fallback, poster selection, and multipart formatted stream variants across Cloudflare/proxy/direct routes.
- All Jinja templates parsed successfully; 12 rendered JavaScript blocks across changed pages passed Node syntax checks.
- Verified all original Python function/class names remain present.
- No new runtime dependencies required. Offline tests run with `python -m unittest discover -s tests -v` after installing project dependencies.

Live Telegram playback, MongoDB persistence and browser interactions were not exercised against a running deployment. Existing deployment configuration is still required.
