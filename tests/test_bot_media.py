import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

spec = importlib.util.spec_from_file_location('bot_media', Path(__file__).resolve().parents[1] / 'Backend/helper/bot_media.py')
media = importlib.util.module_from_spec(spec)
spec.loader.exec_module(media)


class Cursor:
    def __init__(self, rows):
        self.rows = rows
    def sort(self, *args):
        return self
    def limit(self, count):
        self.rows = self.rows[:count]
        return self
    def __aiter__(self):
        async def iterate():
            for row in self.rows:
                yield row
        return iterate()


class Collection:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
    def find(self, query):
        self.query = query
        return Cursor(self.rows)


class MediaTests(unittest.IsolatedAsyncioTestCase):
    def test_payloads_and_custom_negative_ids(self):
        for kind in ('movie', 'tv'):
            for ident in (123, -123):
                self.assertEqual(media.parse_media_payload(media.media_payload(kind, ident)), (kind, ident))
        for payload in ('media_movie_x', 'media_unknown_12', 'media_tv_12_extra', 'file_bad'):
            self.assertIsNone(media.parse_media_payload(payload))

    def test_personal_links_encoded_and_series_route(self):
        doc = {'media_type': 'tv', 'imdb_id': 'tt123'}
        first = media.card_links('https://host/', 'alice', '/TV Shows/A & B [2]', doc)
        second = media.card_links('https://host/', 'bob', '/TV Shows/A & B [2]', doc)
        self.assertEqual(first[0][1], 'https://host/webdav/alice/TV%20Shows/A%20%26%20B%20%5B2%5D/')
        self.assertNotIn('alice', second[0][1])
        self.assertEqual(first[1][1], 'https://host/open/stremio/series/tt123')
        self.assertNotIn('alice', first[1][1])

    def test_missing_imdb_keeps_webdav(self):
        self.assertEqual(len(media.card_links('https://host', 'token', '/Movies/A', {'media_type': 'movie'})), 1)

    async def test_actual_webdav_folder_handles_duplicate_titles(self):
        nodes = {
            'A': SimpleNamespace(tmdb_id=12, db_index=1, path='/Movies/A'),
            'A [2]': SimpleNamespace(tmdb_id=12, db_index=2, path='/Movies/A [2]'),
        }
        fs = SimpleNamespace(ensure_tree=AsyncMock(return_value=SimpleNamespace(children={
            'Movies': SimpleNamespace(children=nodes)})))
        path = await media.webdav_path(fs, {'media_type': 'movie', 'tmdb_id': 12, 'db_index': 2})
        self.assertEqual(path, '/Movies/A [2]')
        self.assertIsNone(await media.webdav_path(fs, {'media_type': 'movie', 'tmdb_id': 13, 'db_index': 2}))

    async def test_search_both_types_across_databases(self):
        movies = Collection([{'title': 'Alpha extended', 'tmdb_id': 1}])
        shows = Collection([{'title': 'Alpha', 'tmdb_id': 2}])
        db = SimpleNamespace(dbs={'tracking': {}, 'storage_1': {'movie': movies, 'tv': Collection([])},
                                 'storage_2': {'movie': Collection([]), 'tv': shows}})
        found = await media.search_titles(db, 'Alpha')
        self.assertEqual([(d['media_type'], d['db_index']) for d in found], [('tv', 2), ('movie', 1)])
        await media.search_titles(db, 'A.*')
        self.assertEqual(movies.query['$or'][0]['title']['$regex'], r'A\.\*')

    async def test_search_limit_and_invalid_query(self):
        collection = Collection([{'title': str(n), 'tmdb_id': n} for n in range(20)])
        db = SimpleNamespace(dbs={'storage_1': {'movie': collection, 'tv': Collection([])}})
        self.assertEqual(len(await media.search_titles(db, 'ab')), 6)
        self.assertEqual(await media.search_titles(db, 'a'), [])

    async def test_announcement_resolves_exact_title_type(self):
        db = SimpleNamespace(find_media_doc=AsyncMock(return_value=({'title': 'Series', 'tmdb_id': 1}, 3)))
        doc = await media.resolve_payload(db, 'media_tv_1')
        db.find_media_doc.assert_awaited_once_with('tv', 1)
        self.assertEqual(doc['db_index'], 3)
        self.assertEqual(doc['media_type'], 'tv')
        db.find_media_doc.return_value = None
        self.assertIsNone(await media.resolve_payload(db, 'media_movie_1'))

    async def test_access_denies_unknown_expired_and_limited_users(self):
        db = SimpleNamespace(get_user=AsyncMock(return_value=None),
                             get_api_token_by_user=AsyncMock(return_value=None),
                             is_subscription_active=Mock(return_value=False),
                             ensure_api_token_for_user=AsyncMock())
        verify = AsyncMock(return_value={})
        modules = {'Backend': SimpleNamespace(db=db),
                   'Backend.config': SimpleNamespace(Telegram=SimpleNamespace(OWNER_ID=1)),
                   'Backend.helper.settings_manager': SimpleNamespace(SettingsManager=SimpleNamespace(
                       current=lambda: SimpleNamespace(subscription=True))),
                   'Backend.fastapi.security.tokens': SimpleNamespace(verify_token=verify)}
        message = SimpleNamespace(from_user=SimpleNamespace(id=42, first_name='User'), reply_text=AsyncMock())
        with patch.dict('sys.modules', modules):
            self.assertIsNone(await media.registered_token(message))
            db.ensure_api_token_for_user.assert_not_awaited()
            db.get_api_token_by_user.return_value = {'token': 'personal'}
            for status in ({'subscription_expired': True}, {'limit_exceeded': 'daily'}):
                verify.return_value = status
                self.assertIsNone(await media.registered_token(message))
            verify.return_value = {}
            self.assertEqual(await media.registered_token(message), 'personal')
            verify.assert_awaited_with('personal')
            db.get_api_token_by_user.assert_awaited_with(42)

    def test_no_direct_forwarding_and_private_search(self):
        root = Path(__file__).resolve().parents[1]
        self.assertNotIn('forward_messages', (root / 'Backend/helper/bot_media.py').read_text())
        self.assertIn('filters.private & filters.text', (root / 'Backend/pyrofork/plugins/media_search.py').read_text())
        self.assertIn('🎬 View media', (root / 'Backend/helper/announcer.py').read_text())


if __name__ == '__main__':
    unittest.main()
