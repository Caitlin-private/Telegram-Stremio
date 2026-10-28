# WZGram migration

The dashboard now uses **WZGram 3.1.5** in place of PyroFork. All earlier changes (Digital Release Cleaner, Get File, and shared normal/quick scan scheduling) are included. The separately hosted Caitlin Search project is unchanged.

## Changes

- Application imports use `wzgram`, including handlers, filters, errors, raw Telegram types, file IDs and sessions.
- Both dependency manifests use `wzgram==3.1.5`. PyroFork and TgCrypto are removed; WZGram supplies WarpCrypto. The regenerated lock pins WarpCrypto 2.1.0 and preserves unrelated existing dependency versions.
- requirements.txt now includes the full application dependency list from pyproject.toml, plus the uv launcher.
- Docker installs the committed lock with `uv sync --locked --no-dev` rather than regenerating it during the build.
- Streaming and prewarming use WZGram's `Client.get_session(..., is_media=True)`, sharing native authorization, locks and session activity tracking. File requests use its media request semaphore while keeping the application's existing retry/FloodWait loop in control.
- The internal `Backend/pyrofork` application directory remains in place so plugin paths and internal imports are stable. It no longer depends on the PyroFork distribution.

## Deploy

Rebuild the Docker image from this archive and restart your deployment. A restart without rebuilding dependencies is insufficient.

For a source deployment, use a fresh virtual environment or `uv sync --locked` to synchronize the project environment. Avoid installing Pyrogram, PyroFork and WZGram together: they provide overlapping Python modules. Keep your existing configuration, credentials and database volumes. Back up existing session files before the first WZGram start because session storage formats may be upgraded.

No database migration or worker-token changes are needed. A synthetic legacy Pyrogram/PyroFork session string was successfully opened by WZGram in the tests. Real account/session authentication must still be checked on your host. New WZGram session exports may not be readable by older forks, so retain the pre-migration session backup if you need to roll back.

## Validation

Validated in a clean Python 3.12 environment:

- `uv sync --locked` completed successfully.
- `uv lock --check --offline` passed.
- All 31 tests passed, including actual WZGram import checks, client construction, legacy session-string import, file-ID encoding, native media-session delegation and request retry settings.
- All application WZGram imports resolve.
- FastAPI imports with 185 routes, and Telegram plugin loading registers eight handler groups using dummy configuration without connecting clients.
- All Jinja templates compile, and Python compilation passes.

Run the suite after installing dependencies:

```sh
uv run --locked python -m unittest discover -s tests -v
uv run --locked python -m compileall -q Backend tests
```

No live Telegram streaming, account login, MongoDB access or throughput benchmark was performed. After deployment, check a normal scan, skip-channel routing, announcement, and playback using your configured workers.

Upstream references:
- https://github.com/rjriajul/wzgram
- https://pypi.org/project/wzgram/3.1.5/


## SQLite startup hotfix

The original migration missed an on-disk session compatibility issue. PyroFork
2.3.61 uses schema version 4 with an existing `update_state` table and a
`usernames(id, peer_id, last_update_on)` layout. WZGram interprets version 4
differently. Its default upgrade tries to create `update_state` again.

The main bot now uses a migration-aware SQLite storage adapter. It inspects the
schema, adds only missing tables/columns/indexes, converts username mappings,
and retains the old username table. Authorization keys, user IDs, peers and
update state are preserved. All schema changes and the version update commit in
one transaction; failures roll back and close the storage connection.

Before an old session is changed, the adapter creates a SQLite backup alongside
it, named `bot.session.pre-wzgram-*.bak`, with owner-only permissions. The SQLite
backup API includes committed WAL data. Already-upgraded sessions do not produce
additional backups. Worker bots and Userbot session-string storage are unchanged.

Deploy this fixed archive, stop the previous process, rebuild/restart, and keep
your existing `bot.session` and configuration. Do not delete the session or
manually change its version number. The adapter runs automatically during the
normal startup session load. Keep the backup private: it contains session keys.

Validation now includes 36 passing tests. The reported error was reproduced with
the exact PyroFork 2.3.61 schema, and the corrected storage passed WZGram's actual
`Client.load_session()` stage using synthetic authorization data. Tests also
cover existing columns/indexes after partial migration, repeated opening,
backup permissions/content, and rollback on unexpected schema. Live Telegram
authorization still requires verification on the deployment host.
