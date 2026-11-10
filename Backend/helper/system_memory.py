"""Memory headroom for the container, or host availability outside containers.

Container headroom is limit minus charged usage (including filesystem cache).
It must not be capped by the host's unrelated MemAvailable reading.
"""

from pathlib import Path


def _number(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def _cgroup_paths():
    # Resolve membership when the process's cgroup is nested under a standard
    # mount. Namespaced containers commonly expose it directly at the root.
    try:
        lines = Path('/proc/self/cgroup').read_text().splitlines()
    except OSError:
        lines = []
    for line in lines:
        parts = line.split(':', 2)
        if len(parts) != 3:
            continue
        _, controllers, group = parts
        relative = Path(group.lstrip('/'))
        if '..' in relative.parts:
            continue
        if not controllers:
            root = Path('/sys/fs/cgroup') / relative
            yield root / 'memory.max', root / 'memory.current'
        elif 'memory' in controllers.split(','):
            root = Path('/sys/fs/cgroup/memory') / relative
            yield root / 'memory.limit_in_bytes', root / 'memory.usage_in_bytes'
    yield Path('/sys/fs/cgroup/memory.max'), Path('/sys/fs/cgroup/memory.current')
    yield (Path('/sys/fs/cgroup/memory/memory.limit_in_bytes'),
           Path('/sys/fs/cgroup/memory/memory.usage_in_bytes'))


def _snapshot(total, available, scope):
    available = max(0, min(total, available))
    return {
        'free_bytes': available,
        'total_bytes': total,
        'used_bytes': total - available,
        'scope': scope,
        'display': f'{available / 1024**3:.2f} / {total / 1024**3:.2f} GiB',
    }


def memory_status():
    # Read the limit and usage from the same cgroup before touching host stats.
    # v2 uses "max" for unlimited; v1 uses a huge page-aligned integer sentinel.
    for limit_path, used_path in _cgroup_paths():
        limit, used = _number(limit_path), _number(used_path)
        if limit is not None and used is not None and 0 < limit < 2**60 and used >= 0:
            return _snapshot(limit, limit - used, 'Container')

    try:
        values = {}
        for line in Path('/proc/meminfo').read_text().splitlines():
            key, value = line.split(':', 1)
            values[key] = int(value.split()[0]) * 1024
        total = values['MemTotal']
        available = values.get('MemAvailable', values.get('MemFree'))
        if total <= 0 or available is None:
            raise ValueError('Missing system memory counters')
        return _snapshot(total, available, 'System')
    except (OSError, ValueError, KeyError, IndexError):
        return {'free_bytes': None, 'total_bytes': None, 'used_bytes': None,
                'scope': 'Unavailable', 'display': 'Unavailable'}
