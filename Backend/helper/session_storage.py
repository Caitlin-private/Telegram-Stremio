"""Migrate legacy fork session files by schema, not incompatible version numbers."""
import logging
import os
from pathlib import Path
import sqlite3
import tempfile

from wzgram.storage.sqlite_storage import SQLiteStorage, fcntl
from wzgram.storage.storage import PROD, TEST

log = logging.getLogger(__name__)


class MigrationSafeSQLiteStorage(SQLiteStorage):
    """WZGram 3.1.5 storage with an atomic, repeatable legacy schema upgrade."""

    async def open(self):
        try:
            await super().open()
        except BaseException:
            if self.conn is not None:
                await self.conn.rollback()
            await self.close()
            raise

    async def _columns(self, table):
        # All table names are fixed application constants.
        rows = await self.conn.execute_fetchall(f'PRAGMA table_info("{table}")')
        return {row[1] for row in rows}

    async def _require_columns(self, table, required):
        missing = set(required) - await self._columns(table)
        if missing:
            raise RuntimeError(f"Cannot migrate session: {table} is missing columns {sorted(missing)}")

    async def _backup(self):
        path = Path(self.database)
        fd, name = tempfile.mkstemp(prefix=path.name + '.pre-wzgram-', suffix='.bak', dir=path.parent)
        os.close(fd)  # mkstemp creates a private (0600) file.
        try:
            with sqlite3.connect(name) as target:
                await self.conn.backup(target)
        except BaseException:
            Path(name).unlink(missing_ok=True)
            raise
        log.info("Saved pre-migration session backup: %s", name)

    async def update(self):
        if not self.in_memory and fcntl is not None and self._lock_fd is None:
            raise RuntimeError("Session is in use by another process; stop it before migrating.")
        version = await self.version()
        if not isinstance(version, int) or not 1 <= version <= self.VERSION:
            raise RuntimeError(f"Unsupported session schema version {version}; expected 1–{self.VERSION}.")
        if version == self.VERSION:
            return
        await self._backup()
        # executescript implicitly commits. Use individual statements so a failed
        # migration rolls back both DDL and data, including the version marker.
        await self.conn.execute('BEGIN IMMEDIATE')
        try:
            await self._require_columns('sessions', ('dc_id', 'test_mode', 'auth_key', 'date', 'user_id', 'is_bot'))
            await self._require_columns('peers', ('id', 'access_hash', 'type', 'phone_number', 'last_update_on'))
            columns = await self._columns('sessions')
            for name, sql_type in (('api_id', 'INTEGER'), ('server_address', 'TEXT'), ('port', 'INTEGER')):
                if name not in columns:
                    await self.conn.execute(f'ALTER TABLE sessions ADD COLUMN {name} {sql_type}')
            username_columns = await self._columns('usernames')
            legacy_usernames = {'id', 'peer_id'} <= username_columns and 'username' not in username_columns
            if legacy_usernames:
                # PyroFork v4 stores (username, peer_id); WZGram stores (peer_id, username).
                # Keep the original table as well as the complete pre-upgrade backup.
                await self.conn.execute('ALTER TABLE usernames RENAME TO legacy_pyrofork_usernames')
            await self.conn.execute('CREATE TABLE IF NOT EXISTS usernames (id INTEGER, username TEXT, FOREIGN KEY (id) REFERENCES peers(id))')
            if legacy_usernames:
                await self.conn.execute('INSERT INTO usernames (id, username) SELECT peer_id, id FROM legacy_pyrofork_usernames')
            await self._require_columns('usernames', ('id', 'username'))
            await self.conn.execute('CREATE TABLE IF NOT EXISTS update_state (id INTEGER PRIMARY KEY, pts INTEGER, qts INTEGER, date INTEGER, seq INTEGER)')
            await self._require_columns('update_state', ('id', 'pts', 'qts', 'date', 'seq'))
            await self.conn.execute('CREATE INDEX IF NOT EXISTS idx_usernames_id ON usernames (id)')
            await self.conn.execute('CREATE INDEX IF NOT EXISTS idx_usernames_username ON usernames (username)')
            if 'username' in await self._columns('peers'):
                await self.conn.execute('''INSERT INTO usernames (id, username)
                    SELECT p.id, p.username FROM peers p
                    WHERE p.username IS NOT NULL AND p.username != ''
                    AND NOT EXISTS (SELECT 1 FROM usernames u WHERE u.id = p.id AND u.username = p.username)''')
            for dc, test_mode in await self.conn.execute_fetchall('SELECT dc_id, test_mode FROM sessions'):
                address = (TEST if test_mode else PROD).get(dc)
                if address:
                    await self.conn.execute('''UPDATE sessions
                        SET server_address = COALESCE(NULLIF(server_address, ''), ?), port = COALESCE(port, ?)
                        WHERE dc_id = ?''', (address, 80 if test_mode else 443, dc))
            await self.conn.execute('UPDATE version SET number = ?', (self.VERSION,))
            await self.conn.commit()
        except BaseException:
            await self.conn.rollback()
            raise
        log.info("Session schema upgraded from %s to %s; existing authorization retained", version, self.VERSION)
