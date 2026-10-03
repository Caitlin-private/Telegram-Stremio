"""RAM available to this server, respecting container memory limits."""
from pathlib import Path


def _number(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def memory_status():
    try:
        values = {}
        for line in Path('/proc/meminfo').read_text().splitlines():
            key, value = line.split(':', 1)
            values[key] = int(value.split()[0]) * 1024
        total = values['MemTotal']
        available = values.get('MemAvailable', values.get('MemFree', 0))
        scope = 'System'
        for limit_path, used_path in (
            ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.current'),
            ('/sys/fs/cgroup/memory/memory.limit_in_bytes', '/sys/fs/cgroup/memory/memory.usage_in_bytes'),
        ):
            limit, used = _number(limit_path), _number(used_path)
            if limit is not None and used is not None and 0 < limit <= total:
                total = limit
                available = min(available, max(0, limit - used))
                scope = 'Container'
                break
        available = max(0, min(total, available))
        return {'free_bytes': available, 'total_bytes': total, 'scope': scope,
                'display': f'{available / 1024**3:.2f} / {total / 1024**3:.2f} GiB'}
    except (OSError, ValueError, KeyError, IndexError):
        return {'free_bytes': None, 'total_bytes': None, 'scope': 'Unavailable', 'display': 'Unavailable'}
