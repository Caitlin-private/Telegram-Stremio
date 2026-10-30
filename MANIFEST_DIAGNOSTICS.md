# Caitlin Stream — manifest diagnostics build

This is the complete project source archive based on the uploaded `caitlin` branch archive.

## Manifest responsiveness changes

The Stremio manifest now places explicit time limits around optional manifest work:
- token lookup: 8 seconds
- subscription validation lookup: 4 seconds
- custom catalog loading: 8 seconds
- dynamic library/franchise collection scan: 8 seconds
- catalog ordering lookup: 4 seconds
- optional subscription-expiry display lookup: 3 seconds

Optional catalog stages log their start, completion time, failure, or timeout with the
`[Manifest]` prefix and are skipped if they fail, allowing the base manifest to return.
Token/subscription validation database timeouts return HTTP 503 rather than leaving the
manifest request hanging indefinitely.

These changes are diagnostic/resilience safeguards; they do not establish which Atlas
operation caused the original stall. After deploying, inspect the Space logs for
`[Manifest]` entries to identify the slow stage.

No test scripts were added. No repository test files were present in the uploaded archive.
