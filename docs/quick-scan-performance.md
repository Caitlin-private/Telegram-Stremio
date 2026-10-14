# Quick Scan scheduling

Quick Scan's **use all eligible worker bots** option is on by default. Uncheck it
to use the explicit worker count. Auto (0) uses every connected, eligible worker
in Quick Scan; normal Scan/Rescan Auto retains its eight-worker limit. The main
bot remains reserved for live ingestion unless no worker qualifies.

- Each bot fetches up to 200 distinct message IDs per batch, with one processing
  batch and at most one prefetched batch. The 0.5-second fetch delay remains.
- All bots share twelve message-processing slots. Each lane has at most twelve
  processing tasks, so one batch cannot enqueue 200 tasks ahead of other bots.
- Actual Quick Scan Telegram RPCs are serialized per bot. FloodWait cooldowns,
  retrying the same operation, playback pauses, and bounded checkpoint lookahead
  remain in place. Telegram RPC/lock waits and explicit FloodWait cooldowns park
  the task outside the shared processing pool. Healthy bots can use all twelve
  slots while the unfinished batch stays uncommitted. The bounded lookahead
  window can still eventually fill during a long cooldown; this bounds memory
  and preserves safe restart behavior. These controls cover Quick Scan, not
  unrelated bot tasks.
- Quick Scan catalogs are coalesced by title and deferred until the channel has
  finished indexing. Catalog settings are read once per flush. Pending entries
  use the existing scan-state map and survive cancellation or a busy catalog
  sync. Unfinished enrichment is shown as pending and retried on the next flush
  or scan; completion does not imply all deferred API work succeeded.
- One scan-local HTTP pool serves TMDb, TVDB, Cinemeta, Kitsu, ani.zip and catalog
  requests. The pool allows twelve concurrent connections in total. Healthy hosts have
  no artificial request spacing; each host retains its own HTTP 429 cooldown. Retry-After supports seconds and HTTP dates.
  These are conservative local ceilings, not guarantees about provider quotas.
  Repeated 429s keep the request pending until recovery or Stop. Catalog calls
  also retain their existing timeouts, leaving interrupted work pending.
- The tools page displays indexing/catalog phases, pending title count, API
  request/429 counts, and accumulated API/FloodWait/catalog wait/work time.
  Concurrent accumulated times are not wall-clock percentages.

No database migrations, indexes, media insert/update code, or duplicate-check
removal are included. Quick Scan performs full metadata matching, skips
thumbnails/individual announcements, and keeps rejected source media. It also
suppresses Telegram deletion tasks queued by replacement-mode inserts, including
multipart replacements. Catalog references still follow the existing replacement
policy; superseded source files remain in Telegram. Normal/manual deletion paths
are unchanged. The temporary scan probe is still deleted; it is not media.

## Why twelve slots

Twelve is a conservative starting point (50% more than eight), not a measured
optimum or a provider quota. Atlas Free documents 100 operations/second, and
basic Hugging Face Spaces have 2 CPU cores and 16 GB RAM. Each imported file can
require multiple DB operations. We retain serialized media writes, provider
pacing, 200-ID batches, and the 0.5-second fetch delay rather than scale processing
by bot count. No fixed slot count guarantees a bottleneck-free deployment.

References checked 2026-10-14:
- https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/
- https://huggingface.co/docs/hub/spaces-overview

## ZIP deployment

The ZIP contains the complete tracked project source, including Dockerfile,
dependency manifests, templates, and tests. It excludes Git history, local Python
environments, caches, logs, and runtime credentials. Keep your existing Hugging
Face secrets/environment settings when replacing source files. No database
migration is required. This package has not been deployed to a live Space.

## Verification

Run `python -m unittest discover -s tests -v` in the installed project environment.
Tests use fake bots, mocked HTTP and mocked persistence; no live service or
catalog is accessed. They cover worker selection, processing limits, disjoint
ranges, bounded prefetch, gap-safe stop/resume, fetch failures, FloodWait isolation,
HTTP 429 recovery/Stop, SDK session compatibility, pending catalog retention,
healthy-bot indexing during an actual simulated FloodWait, gap-safe retry,
non-deletion of rejects/replacements, cancellation slot accounting, and
normal scan behavior.

For deployment measurements, compare the same message range and worker count.
Record wall-clock time, indexed files/minute, API requests/429s, FloodWait time,
metadata time, DB-lock wait and catalog time. Distinguish cold and warm caches.
No production speedup has been measured by these offline tests.

## Speed-regression correction

The earlier ZIP added a four-request per-host ceiling and 100 ms spacing even
for healthy metadata providers. Those restrictions have been removed. The shared
HTTP connection pool still caps total active connections at twelve, and actual
HTTP 429 responses still trigger Retry-After backoff. The non-deletion policy,
parked Telegram cooldowns, database logic, and twelve processing slots remain.
A regression test checks that twelve healthy requests can start concurrently
without artificial wait. This is not a live deployment throughput benchmark.
