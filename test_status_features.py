"""Offline tests: python3 -m unittest test_status_features.py"""
import ast
import asyncio
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).parent


class StatusTests(unittest.TestCase):
    def test_memory_container_and_host(self):
        memory = runpy.run_path(str(ROOT / 'Backend/helper/system_memory.py'))['memory_status']
        files = {'/proc/meminfo': 'MemTotal: 8388608 kB\nMemAvailable: 4194304 kB\n',
                 '/sys/fs/cgroup/memory.max': str(2 * 1024**3),
                 '/sys/fs/cgroup/memory.current': str(512 * 1024**2)}
        def read(path):
            if str(path) not in files:
                raise FileNotFoundError(str(path))
            return files[str(path)]
        with patch.object(Path, 'read_text', read):
            self.assertEqual(memory()['display'], '1.50 / 2.00 GiB')
            files['/sys/fs/cgroup/memory.max'] = 'max'
            self.assertEqual(memory()['display'], '4.00 / 8.00 GiB')
            files.clear()
            self.assertEqual(memory()['display'], 'Unavailable')

    def test_skip_reply_targets_copy_before_delete(self):
        tree = ast.parse((ROOT / 'Backend/helper/skip_channel.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'route_to_skip_channel')
        class FloodWait(Exception):
            value = 0
        settings = SimpleNamespace(skip_channel='-10022', delete_on_metadata_fail=True)
        namespace = {'Client': object, 'Message': object, 'FloodWait': FloodWait,
                     'SettingsManager': SimpleNamespace(current=lambda: settings),
                     'ParseMode': SimpleNamespace(DISABLED='disabled'), 'asleep': AsyncMock(),
                     'LOGGER': SimpleNamespace(error=lambda *a: None, warning=lambda *a: None)}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<skip>', 'exec'), namespace)
        copy = AsyncMock(return_value=SimpleNamespace(id=99))
        message = SimpleNamespace(id=7, chat=SimpleNamespace(id=-10011), caption='<b>Movie</b>', document=None,
                                  video=SimpleNamespace(file_name='movie.mkv'), copy=copy)
        client = SimpleNamespace(send_message=AsyncMock())
        delete = AsyncMock()
        modules = {'Backend.helper.metadata.parse': SimpleNamespace(analyze_metadata_failure=lambda s: 'No resolution found.'),
                   'Backend.helper.pyro': SimpleNamespace(clean_filename=lambda s: s),
                   'Backend.helper.task_manager': SimpleNamespace(delete_message=delete)}
        with patch.dict(sys.modules, modules):
            asyncio.run(namespace['route_to_skip_channel'](client, message))
        self.assertEqual(client.send_message.call_args.kwargs['reply_to_message_id'], 99)
        self.assertEqual(client.send_message.call_args.kwargs['parse_mode'], 'disabled')
        self.assertIn('Reason: No resolution found.', client.send_message.call_args.args[1])
        delete.assert_awaited_once_with(-10011, 7)
        copy.reset_mock()
        client.send_message.reset_mock()
        delete.reset_mock()
        copy.side_effect = RuntimeError('Copy denied')
        with patch.dict(sys.modules, modules):
            asyncio.run(namespace['route_to_skip_channel'](client, message))
        client.send_message.assert_not_awaited()
        delete.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
