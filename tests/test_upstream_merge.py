"""Offline merge regression checks: python -m unittest discover -s tests -v."""
import ast
import asyncio
import logging
from pathlib import Path
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]

def load_nodes(path, names, namespace):
    tree = ast.parse((ROOT / path).read_text())
    nodes = [n for n in tree.body if getattr(n, 'name', None) in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, 'exec'), namespace)
    return namespace

class MergeTests(unittest.TestCase):
    def test_formatter_self_checks(self):
        path = ROOT / 'Backend/helper/stream_formatter.py'
        source = path.read_text().replace('from Backend.logger import LOGGER', '')
        ns = {'LOGGER': logging.getLogger('test'), '__name__': '__main__'}
        exec(compile(source, str(path), 'exec'), ns)
        settings = types.SimpleNamespace(stream_formatter='default')
        self.assertIsNone(ns['format_stream'](settings, 'Example.1080p.mkv', 10))

    def test_userbot_selection_and_fallback(self):
        setting = types.SimpleNamespace(default_streaming='userbot')
        ns = dict(SettingsManager=types.SimpleNamespace(current=lambda: setting),
                  USERBOT_CLIENT_INDEX=-1, _get_userbot_streamer=lambda: 'user',
                  select_best_client=lambda _: 0, multi_clients={0:'bot'},
                  _get_streamer=lambda client, _: client, LOGGER=logging.getLogger('test'),
                  get_parallel_prefetch=lambda _: (3, 3))
        load_nodes('Backend/fastapi/routes/stream_routes.py', {'_db_client','_prefetch_for'}, ns)
        async def fetch(client): return client + '-file'
        async def failing_user(client):
            if client == 'user': raise RuntimeError('unavailable')
            return client + '-file'
        self.assertEqual(asyncio.run(ns['_db_client'](fetch)), (-1,'user','user-file'))
        self.assertEqual(asyncio.run(ns['_db_client'](failing_user)), (0,'bot','bot-file'))
        setting.default_streaming = 'bots'
        self.assertEqual(asyncio.run(ns['_db_client'](fetch)), (0,'bot','bot-file'))
        self.assertEqual(ns['_prefetch_for'](-1), (1,1))
        self.assertEqual(ns['_prefetch_for'](0), (3,3))

    def test_poster_selection(self):
        settings = types.SimpleNamespace(pictorium_poster_enabled=False, spatial_poster_enabled=True,
            spatial_poster='https://example/{type}/{imdb_id}.jpg', better_poster_enabled=False,
            rpdb_enabled=False, rpdb_api_key='')
        ns = dict(SettingsManager=types.SimpleNamespace(current=lambda:settings), _abs_media_url=lambda s:s,
                  BETTERPOSTER_DEFAULT='better/{imdb_id}', RPDB_FREE='rpdb/{imdb_id}')
        load_nodes('Backend/helper/poster_provider.py', {'_poster_url'}, ns)
        poster = ns['_poster_url']
        self.assertEqual(poster('tt123','original',media_type='tv'), 'https://example/series/tt123.jpg')
        self.assertEqual(poster('tt123','original',media_type='movie'), 'https://example/movie/tt123.jpg')
        self.assertEqual(poster('tt123','original',provider='default'), 'original')
        self.assertEqual(poster('tt123','original',shape='landscape'), 'original')
        self.assertEqual(poster('tt123','original',provider='rpdb'), 'rpdb/tt123')

    def test_multipart_formatter_stream_variants(self):
        source = (ROOT/'Backend/fastapi/routes/stremio_routes.py').read_text()
        start = source.index('                sort_name = stream_name')
        end = source.index('                for stream in streams[stream_offset:]:',start)
        import textwrap
        block = textwrap.dedent(source[start:end])
        for custom in (None, ('Custom', 'Description')):
            streams = []
            settings = types.SimpleNamespace(base_url='https://example',cf_stream_mode='both',show_proxy_and_non_proxy_both=True)
            ns = dict(stream_name='1080p', stream_title='Original', streams=streams,
                SettingsManager=types.SimpleNamespace(current=lambda:settings),token='token',
                quality={'id':'file','video_part':2},filename='Movie.Part002.mkv',size_bytes=100,
                build_proxy_url=lambda u:'proxy:'+u,cf_stream_url=lambda *a:'cloudflare',cf_enabled=lambda:True,
                format_stream=lambda *a,**kw:custom,ADDON_NAME='Telegram',fmt_meta={},combined={'start':1,'end':3},
                episode_start=1,name_key='movie')
            exec(block, ns)
            self.assertEqual(len(streams),3)
            self.assertEqual([s['url'] for s in streams], ['cloudflare','proxy:https://example/dl/token/file/video.mkv','https://example/dl/token/file/video.mkv'])
            for stream in streams:
                self.assertIn('Multi-Part · Part 2', stream['name'])
                self.assertIn('E01-E03', stream['name'])
                self.assertEqual(stream['sort_name'],'1080p')
            if custom: self.assertTrue(all('Custom' in s['name'] for s in streams))

if __name__ == '__main__': unittest.main()
