# Design & trade-off note

**Goal:** a correct, restartable, bounded-memory load of large event files, favouring clarity and evidence over scale tricks.

| Decision | Why | Trade-off |
|---|---|---|
| SQLite target, stdlib only | Zero setup, real SQL, window functions, FK/CHECK constraints, runs anywhere a reviewer has Python. | Single writer; not horizontally scalable. The loader is isolated in `loader.py`, so swapping the target is a localized change. |
| Streaming reader + `chunked()` | Memory is O(chunk), not O(file). | Row-level Python is slower than a vectorised engine (~27k rows/s). |
| One transaction per chunk containing facts, rejects **and** checkpoint | Crash can never leave "rows loaded but checkpoint not advanced" (or vice-versa) ⇒ exact resume. | Chunk is the unit of redo; a failure loses at most one chunk of work. |
| File-hash ledger + PK on `event_id` | Two layers of idempotency: whole-file skip, and event-level safety net for overlapping files/backfills. | Re-sending a *corrected* file with the same event ids is rejected as `CONFLICTING_DUPLICATE` rather than updating; corrections must be new events (preserves history). |
| First-write-wins duplicates, never overwrite | History is preserved and every conflict is visible in the quarantine. | Needs an operator decision for genuine corrections. |
| Current order state is a **view** over immutable events | Late/out-of-order data can't produce a stale "latest" value; no incremental-update bugs. | Computed at query time (window function); would be materialised at scale. |
| Money as integer cents, `Decimal` parsing | No float drift; exact reconciliation. | Needs a currency-minor-unit table if non-2-decimal currencies are added. |
| Timestamps normalised to UTC; naive ⇒ UTC | Deterministic daily bucketing. | Assumption documented; naive local times would be mis-bucketed. |
| Generator emits answer labels to a **separate** file, computed independently of the pipeline | Reconciliation is a real test, not the code grading itself. | Generator re-implements a few rules (kept deliberately tiny). |
| Schema mismatch fails the **file**; bad rows fail only themselves | A missing column means every row is wrong; a bad row is just bad data. | Operator must fix and resubmit the file (new hash ⇒ new load). |
| Reports are rebuilt, not incrementally updated | Idempotent by construction. | Full recompute each time (cheap here). |

**Failure modes covered:** malformed row, missing field, duplicate (identical/conflicting), schema drift, empty/zero-byte/missing file,
invalid reference key, crash mid-file, rerun of same file, overlapping backfill, late/out-of-order events, timezone offsets, amount boundaries.

**Not covered (explicitly):** concurrent writers on the same DB, huge single-line JSON arrays, schema evolution of *added required* columns,
currency conversion, data-retention/PII controls.
