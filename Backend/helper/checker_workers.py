"""Distribute existing checker requests without increasing their concurrency."""


def ordered_check_clients(registry, offset, fallback=None):
    clients = []
    for client in registry.values():
        if client is None or getattr(client, "is_connected", True) is False:
            continue
        if not any(client is existing for existing in clients):
            clients.append(client)
    if not clients:
        return [fallback] if fallback is not None else []
    offset %= len(clients)
    return clients[offset:] + clients[:offset]
