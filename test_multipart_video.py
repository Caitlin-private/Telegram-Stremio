import ast
import asyncio
from pathlib import Path
import re
import runpy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

ROOT = Path(__file__).parent
helpers = runpy.run_path(str(ROOT / 'Backend/helper/multipart_video.py'))


class MultipartTests(unittest.TestCase):
    def test_parse_and_order(self):
        parse = helpers['video_part']
        self.assertEqual(parse('Movie.2025.1080p.part001.mkv')['number'], 1)
        self.assertEqual(parse('Movie.2025.1080p.part010.mkv')['clean'], 'Movie.2025.1080p.mkv')
        self.assertEqual(parse('Movie.2025.1080p.part001.mkv')['group'], parse('Movie.2025.1080p.part002.mkv')['group'])
        for name in ['Movie.mkv', 'Movie.zip.001', 'Movie.part001.rar', 'Movie.part000.mkv']:
            self.assertIsNone(parse(name))
        rows = [{'video_part': 10, 'video_group': 'a'}, {'video_part': 2, 'video_group': 'a'}, {}]
        self.assertEqual([q.get('video_part') for q in sorted(rows, key=helpers['part_sort_key'])], [None, 2, 10])

    def test_replacement_and_duplicate_slots(self):
        tree = ast.parse((ROOT / 'Backend/helper/database.py').read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'Database')
        names = {'_apply_quality_update', '_dup_key', '_matches_protected_duplicate'}
        body = [n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
        settings = SimpleNamespace(replace_mode=True, duplicate_protection=True)
        ns = {'re': re, 'SettingsManager': SimpleNamespace(current=lambda: settings),
              'List': list, 'Optional': __import__('typing').Optional,
              'LOGGER': SimpleNamespace(info=lambda *a: None)}
        mod = ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(name='DB', bases=[], keywords=[], body=body, decorator_list=[])], type_ignores=[]))
        exec(compile(mod, '<policy>', 'exec'), ns)
        db = ns['DB']()
        db._queue_quality_deletion = AsyncMock()
        full = {'quality': '1080p', 'name': 'full', 'size': '2 GB'}
        one = {'quality': '1080p', 'name': 'part1', 'size': '1 GB', 'video_part': 1, 'video_group': 'release'}
        two = {**one, 'video_part': 2, 'name': 'part2'}
        result = asyncio.run(db._apply_quality_update([full, one], two))
        self.assertEqual(result, [full, one, two])
        db._queue_quality_deletion.assert_not_awaited()
        replacement = {**two, 'size': '1.5 GB'}
        self.assertEqual(asyncio.run(db._apply_quality_update(result, replacement)), [full, one, replacement])
        db._queue_quality_deletion.assert_awaited_once_with(two)
        settings.replace_mode = False
        status = {}
        self.assertEqual(asyncio.run(db._apply_quality_update([one, two], dict(two), status=status)), [one, two])
        self.assertTrue(status['duplicate_skipped'])


if __name__ == '__main__':
    unittest.main()
