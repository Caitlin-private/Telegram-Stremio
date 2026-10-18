# Quick scan performance update

Based on `Caitlin-private/Telegram-Stremio`, branch `caitlin`, commit
`94f1d41264094dd325c9affd19947b2c44d3c3df`.

## What changed

- Quick scan prefetches Telegram batches through a bounded queue while metadata
  processing and writes finish. There is at most one queued batch and one fetch
  in flight per bot, in addition to its processing batch. The existing global
  look-ahead window, completed ranges, and contiguous checkpoint remain intact.
- Metadata concurrency defaults to 16 and is independent of the bot count.
  Optional environment variable `QUICK_SCAN_CONCURRENCY` accepts 1–64; malformed
  values fall back to 16. Start with the default and measure before increasing.
- Telegram fetch pacing starts at 20 ms, increases after FloodWait, then decays
  after successful requests. The server's full FloodWait is still honored.
- Source duplicates are checked with batched, indexed queries (movie and TV per
  storage), including split parts. A fresh batched check inside the shared live
  ingestion lock protects against concurrently indexed messages. Stream IDs are
  encoded in one executor job per batch, with byte-compatible existing IDs.
- A quick-scan batch groups records by title and reads/merges/writes each title
  once. It preserves duplicate protection, quality replacement, split files,
  independently playable parts, season/episode and absolute-episode matching.
  Existing titles remain in their current storage database to avoid repeated
  whole-series migration during import. New-title storage quota failover remains
  supported. Existing-title write failures stop the scan for safe retry instead
  of silently counting the source as processed.
- Deletions caused by quality replacement are scheduled only after the grouped
  media document write succeeds. The shared receiver lock remains in place;
  unrelated writer concurrency has not been made unsafe by removing it.
- Catalog processing runs outside batch commits, with three bounded workers,
  one HTTP client/configuration read per queue flush, and grouped catalog writes.
  Pending entries are persisted with scan state and acknowledged only after
  success; requeued title generations survive an older operation's completion.
  Duplicate replay recovers catalog work after a media-write/checkpoint crash.
  Failed catalog entries survive completion, stop, and discard, and retry on the
  next scan. The Tools screen shows pending count and accumulated timings.
- Cinemeta episode lookups reuse a cached series response and episode index.
  TVDB shares paginated episode responses across requests and searches beyond
  page zero. Returned TVDB episodes are copied before translation enrichment.
- Shared metadata and catalog caches now have bounded LRU capacity and TTLs:
  2,048 entries per store, one hour for populated results, 30 seconds for empty
  results. Exceptions are not cached. Cancelling a waiter cannot cancel other
  consumers; cancelling a producer clears the in-flight entry for retry.

Quick scan already skips artwork downloads/uploads, rejected-file routing, and
per-media announcements; it continues to do so. Normal scan retains those
behaviors. Shared metadata/provider cache fixes also benefit normal scans.
No source-reference registry migration or new database schema is required.

## Using this archive

This ZIP contains the complete updated source, including tests. Deploy it using
this repository's existing installation/Docker instructions and retain your
existing configuration, environment variables, sessions, and database volumes.
Do not overwrite them with sample configuration. Restart the application to load
these changes, then choose Quick Scan in Tools. No GitHub changes were pushed.

## Validation

Run from the extracted repository root with Python 3.11+ and Pydantic installed
(already an application dependency):

```sh
python -W ignore::DeprecationWarning -m unittest discover -s tests -v
python -m compileall -q Backend tests
```

The regression suite isolates production definitions from application startup;
Telegram, MongoDB, and metadata HTTP boundaries are faked. It covers shared cache
requests/cancellation/expiry, Cinemeta and TVDB request reuse, batch source checks,
movie/TV/split-file merging, failed writes and replacement deletion ordering,
concurrent workers, out-of-order completion, stop/restart from partial progress,
live-ingestion duplicate rechecks, catalog generations/retries and encoding
compatibility. It also exercises the actual normal-message insertion path.

Operation-count checks:

- 24 Cinemeta episodes plus series details: one metadata HTTP request (previous
  implementation: 25 requests to the same URL; title search excluded).
- 200 new episodes of one series in one batch: one document write.
- 450 unique messages with two scan workers: exactly 450 indexed, three grouped
  title writes, and a final contiguous cursor of 451.

These are deterministic mocked checks, not production throughput benchmarks.
No live Telegram account, MongoDB instance, or provider credentials were used.
For deployment measurements, compare the same channel/range with cold/warm
metadata caches, new imports and duplicate-heavy scans separately. Preserve
playback priority, and compare indexed counts and completed ranges as well as
elapsed time. Timings are accumulated task time and can exceed wall time when
operations run concurrently.
