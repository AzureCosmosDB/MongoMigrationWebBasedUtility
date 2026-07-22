# Partitioner Math Reference

Cheat-sheet for how the partitioner derives chunk count, segment count, and
`$sample` size across the supported partitioners, job types, and user-filter
states.

Implemented in:

- [OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs](../OnlineMongoMigrationProcessor/Workers/MigrationWorker.cs) — `CalculatePartitioningStrategy`
- [OnlineMongoMigrationProcessor/Partitioner/SamplePartitioner.cs](../OnlineMongoMigrationProcessor/Partitioner/SamplePartitioner.cs) — `CreatePartitions`, `GetMaxSamples`, `GetMaxSegments`, `SampleOversampleFactor`, `GetMinDocsPerChunk`, `GetMinDocsPerSegment`

Collections under **1,000,000** docs short-circuit to `(chunks=1, segments=1)` and are not partitioned at all.

> **Note**: This document covers only `DumpAndRestore` and `MongoDriver` job types. `RUOptimizedCopy` uses [`RUPartitioner`](../OnlineMongoMigrationProcessor/Partitioner/RUPartitioner.cs), which produces one chunk per Cosmos DB physical partition discovered via the `GetChangeStreamTokens` custom command. Chunk count for RU is therefore equal to the source's physical partition count and is independent of `documentCount`, `MaxSamples`, `MinDocsPerChunk`, or any of the symbols below.

## Symbols

| Symbol | Definition |
|---|---|
| `MaxSamples(noMatch)` | `300,000` — `SamplePartitioner.GetMaxSamples(false)` |
| `MaxSamples(withMatch)` | `3,000` — `SamplePartitioner.GetMaxSamples(true)`; also the absolute clamp on the final `$sample` size when a `$match` precedes `$sample` |
| `MaxSegments` | `max(20, ParallelThreads)` — defaults to `max(20, Environment.ProcessorCount × 5)` if `ParallelThreads` is unset |
| `DriverMultiplier` | `10` — MongoDriver wants 10× the sub-ranges that the matching DumpAndRestore plan would produce. **Dropped** when `useSampleCommand && hasMatchBeforeSample` because the absolute 3 K `$sample` cap means we can't get more usable boundaries than that |
| `SampleOversampleFactor` | `10` — `$sample` requests this many samples per desired boundary; quantile selection collapses the result back to the boundary count for equally-sized partitions. Effective oversample drops below `10` when the filter cap binds |
| `LargeCollectionThreshold` | `100,000,000` — collections strictly greater than this use the "large" tier chunk floor (`MinDocsPerChunkLarge`); the segment floor is derived from the chunk floor |
| `MinDocsPerChunk` | `100,000` — small/medium tier (`docCount ≤ 100 M`). Every chunk that runs as a single unit of work (DumpAndRestore always, or MongoDriver when segments collapse to 1) must hold at least this many documents |
| `MinDocsPerChunkLarge` | `1,000,000` — large tier (`docCount > 100 M`). Coarser chunks keep chunk metadata manageable on multi-billion-doc collections |
| `PartitionFactor` | `MigrationSettings` value (percentage, clamped to `25`-`100`, default `100`). `GetMinDocsPerChunk` multiplies the tiered floor by `PartitionFactor / 100`, so `100` keeps the full floor and `25` shrinks chunks to a quarter (producing roughly 4× more of them). The derived segment floor scales with it |
| `GetMinDocsPerChunk(docCount)` | `max(1, (docCount > LargeCollectionThreshold ? MinDocsPerChunkLarge : MinDocsPerChunk) × (PartitionFactor / 100))` |
| `GetMinDocsPerSegment(docCount)` | `max(1, GetMinDocsPerChunk(docCount) × 0.1)` — segments stay one order of magnitude finer than chunks and honor `PartitionFactor` through the chunk floor |
| Job-type bucket | `DumpAndRestore` vs `MongoDriver`. **`RUOptimizedCopy` is out of scope** for this document — see note above |

## General formulas

### `UseSampleCommand`

Two caps stack:

1. **Cap by `$match` ahead of `$sample`**: `B_target = MaxSamples(hasMatchBeforeSample)`. `hasMatchBeforeSample = hasUserFilter OR (collection has more than one `_id` type)` — both prepend a `$match` stage to the `$sample` pipeline, which pushes MongoDB off the fast random-cursor path into a full-scan + top-k sort. Without a `$match`, `MongoDriver` multiplies `B_target` by `DriverMultiplier` (10× more boundaries). With a `$match`, `MongoDriver` keeps the same `B_target` because the absolute `$sample` size cap means we can't get more usable boundaries regardless of oversampling.
2. **5% cap**: `B_actual = Min(B_target, docCount / 200)` — derived from `sampleSize ≤ docCount × 5%` combined with `SampleOversampleFactor = 10`.

The `$sample` size is `chunks × segments × SampleOversampleFactor`, **clamped at `MaxSamples(withMatch) = 3,000` when a `$match` precedes `$sample`** (top-k sort bound). The effective oversample factor drops below 10 as `chunks × segments` approaches 3,000.

For `MongoDriver`, the chunk floor is **`max(MinDocsPerChunk, MaxSegments × MinDocsPerSegment)`** — large enough that every chunk can saturate `MaxSegments` parallel segments at the tier's per-segment minimum. We reduce chunks rather than dropping segments below `MaxSegments` whenever the collection is itself large enough.

| Job Type | `$match` before `$sample` | `B_target` | `B_actual` | Chunks | Segments | `$sample` size |
|---|---|---|---|---|---|---|
| `DumpAndRestore` | No | `300,000` | `Min(B_target, docCount / 200)` | `Min(B_actual, docCount / GetMinDocsPerChunk(docCount))` | `1` | `chunks × 10` |
| `DumpAndRestore` | Yes | `3,000` | `Min(B_target, docCount / 200)` | `Min(B_actual, docCount / GetMinDocsPerChunk(docCount))` | `1` | `Min(chunks × 10, 3,000)` |
| `MongoDriver` | No | `3,000,000` | `Min(B_target, docCount / 200)` | `Min(Ceil(B_actual / MaxSegments), docCount / Max(GetMinDocsPerChunk(docCount), MaxSegments × GetMinDocsPerSegment(docCount)))` | `Min(MaxSegments, Max(1, docsPerChunk / GetMinDocsPerSegment(docCount)))` | `chunks × segments × 10` |
| `MongoDriver` | Yes | `3,000` | `Min(B_target, docCount / 200)` | `Min(Ceil(B_actual / MaxSegments), docCount / Max(GetMinDocsPerChunk(docCount), MaxSegments × GetMinDocsPerSegment(docCount)))` | `Min(MaxSegments, Max(1, docsPerChunk / GetMinDocsPerSegment(docCount)))` | `Min(chunks × segments × 10, 3,000)` |

> **`$match` triggers**: A `$match` stage is prepended to `$sample` when **either** a `UserFilter` is set **or** the collection has more than one `_id` BSON type (the `$type` predicate). The planner probes `_id` types in a separate phase before `CalculatePartitioningStrategy` runs so this decision is made up front. A collection with a single `_id` type (`SkipDataTypeFilterForId = true`) and no user filter stays on MongoDB's fast random-cursor path.

### `UseTimeBoundaries` / `UseAdjustedTimeBoundaries` / `UsePagination`

These analytical partitioners do **not** use MongoDB `$sample`; boundaries come from `MongoObjectIdSampler` (time bucketing or pagination). Chunk count is doc-count-driven and uses the same `GetMinDocsPerChunk` floor as the `UseSampleCommand` no-`$match` path, so for a given `docCount` they produce the same chunk/segment counts.

| Job Type | Filter | Chunks | Segments | `$sample` size |
|---|---|---|---|---|
| `DumpAndRestore` | any | `Max(1, docCount / GetMinDocsPerChunk(docCount))` | `1` | n/a |
| `MongoDriver` | any | `Min(Ceil((10 × docCount / GetMinDocsPerChunk(docCount)) / MaxSegments), docCount / Max(GetMinDocsPerChunk(docCount), MaxSegments × GetMinDocsPerSegment(docCount)))` | `Min(MaxSegments, Max(1, docsPerChunk / GetMinDocsPerSegment(docCount)))` | n/a |

> The user filter does not change the chunk/segment math for analytical
> partitioners, but it is still applied at read time (every chunk's read
> query AND-merges the user filter with the `_id` range).

## Worked examples

Assumptions for all tables:

- `ParallelThreads = 40` → `MaxSegments = max(20, 40) = 40`.
- Tier breakpoint: `100 M` docs and below use `MinDocsPerChunk = 100 K` (segment floor `10 K`). Above `100 M` uses `MinDocsPerChunkLarge = 1 M` (segment floor `100 K`). The segment floor is always `0.1 × GetMinDocsPerChunk`.
- `PartitionFactor = 100` (default) for every table below. Lowering it toward `25` scales each `GetMinDocsPerChunk`/`GetMinDocsPerSegment` value by `PartitionFactor / 100` (e.g. `25` → `0.25`, roughly quadrupling the resulting chunk and segment counts).
- For `MongoDriver`, the **effective** per-chunk floor is `max(MinDocsPerChunk, MaxSegments × MinDocsPerSegment)`: `400 K` for `≤ 100 M` docs (`40 × 10 K`), `4 M` for `> 100 M` docs (`40 × 100 K`).

### `UseSampleCommand`, **no `$match` before `$sample`** (single `_id` type and no `UserFilter`)

`B_target = 300,000` (dump) / `3,000,000` (driver); `B_actual = Min(B_target, docs / 200)`.

| Documents | Job Type | `B_actual` | Chunks | Segments | docs/chunk | docs/segment | `$sample` size | Notes |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 500 K | any | — | 1 | 1 | 500,000 | 500,000 | — | < 1 M short-circuit |
| 1 M | DumpAndRestore | 5,000 | 10 | 1 | 100,000 | 100,000 | 100 | 5% cap binds; chunk floor caps 5,000 → 10 |
| 1 M | MongoDriver | 5,000 | 2 | 40 | 500,000 | 12,500 | 800 | driver chunk floor 400 K caps 125 → 2; segs saturate `MaxSegments` |
| 10 M | DumpAndRestore | 50,000 | 100 | 1 | 100,000 | 100,000 | 1,000 | chunk floor caps 50,000 → 100 |
| 10 M | MongoDriver | 50,000 | 25 | 40 | 400,000 | 10,000 | 10,000 | driver chunk floor 400 K caps 1,250 → 25; segs saturate `MaxSegments` |
| 100 M | DumpAndRestore | 300,000 | 1,000 | 1 | 100,000 | 100,000 | 10,000 | `MaxSamples` cap binds; chunk floor caps 300,000 → 1,000 |
| 100 M | MongoDriver | 500,000 | 250 | 40 | 400,000 | 10,000 | 100,000 | driver chunk floor 400 K caps 12,500 → 250 |
| 1 B | DumpAndRestore | 300,000 | 1,000 | 1 | 1,000,000 | 1,000,000 | 10,000 | **large tier**; chunk floor caps 300,000 → 1,000 |
| 1 B | MongoDriver | 3,000,000 | 250 | 40 | 4,000,000 | 100,000 | 100,000 | **large tier**; driver chunk floor 4 M caps 75,000 → 250 |
| 2 B | DumpAndRestore | 300,000 | 2,000 | 1 | 1,000,000 | 1,000,000 | 20,000 | **large tier**; chunk floor caps 300,000 → 2,000 |
| 2 B | MongoDriver | 3,000,000 | 500 | 40 | 4,000,000 | 100,000 | 200,000 | **large tier**; driver chunk floor 4 M caps 75,000 → 500 |

### `UseSampleCommand`, **`$match` before `$sample`** (multi-`_id`-type collection **or** `UserFilter` present)

`B_target = 3,000` for both job types (`DriverMultiplier` is dropped). Raw `$sample` is clamped at `3,000`.

| Documents | Job Type | `B_actual` | Chunks | Segments | docs/chunk | docs/segment | Raw `$sample` | Final `$sample` | Notes |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 500 K | any | — | 1 | 1 | 500,000 | 500,000 | — | — | < 1 M short-circuit |
| 1 M | DumpAndRestore | 3,000 | 10 | 1 | 100,000 | 100,000 | 100 | 100 | chunk floor caps 3,000 → 10 |
| 1 M | MongoDriver | 3,000 | 2 | 40 | 500,000 | 12,500 | 800 | 800 | driver chunk floor 400 K caps 75 → 2; segs saturate `MaxSegments` |
| 10 M | DumpAndRestore | 3,000 | 100 | 1 | 100,000 | 100,000 | 1,000 | 1,000 | chunk floor caps 3,000 → 100 |
| 10 M | MongoDriver | 3,000 | 25 | 40 | 400,000 | 10,000 | 10,000 | **3,000** | driver chunk floor 400 K caps 75 → 25; filter clamp binds |
| 100 M | DumpAndRestore | 3,000 | 1,000 | 1 | 100,000 | 100,000 | 10,000 | **3,000** | chunk floor caps 3,000 → 1,000; filter clamp binds |
| 100 M | MongoDriver | 3,000 | 75 | 40 | 1,333,333 | 33,333 | 30,000 | **3,000** | driver chunk floor doesn't bind (75 < 250); segs saturate; filter clamp binds |
| 1 B | DumpAndRestore | 3,000 | 1,000 | 1 | 1,000,000 | 1,000,000 | 10,000 | **3,000** | **large tier**; chunk floor caps 3,000 → 1,000 |
| 1 B | MongoDriver | 3,000 | 75 | 40 | 13,333,333 | 333,333 | 30,000 | **3,000** | **large tier**; driver chunk floor doesn't bind (75 < 250); segs saturate |
| 2 B | DumpAndRestore | 3,000 | 2,000 | 1 | 1,000,000 | 1,000,000 | 20,000 | **3,000** | **large tier**; chunk floor caps 3,000 → 2,000 |
| 2 B | MongoDriver | 3,000 | 75 | 40 | 26,666,667 | 666,667 | 30,000 | **3,000** | **large tier**; driver chunk floor doesn't bind (75 < 500); segs saturate |

### `UseTimeBoundaries` / `UseAdjustedTimeBoundaries` / `UsePagination`

Doc-count-driven seed `dumpSubRanges = max(1, docCount / GetMinDocsPerChunk(docCount))`. Boundaries from `MongoObjectIdSampler`; no `$sample`. Output matches the `UseSampleCommand`, **no `$match`** table above for every doc count — same chunk floor, no `$sample`-specific caps to lower it.

| Documents | Job Type | Chunks | Segments | docs/chunk | docs/segment | Notes |
|---:|---|---:|---:|---:|---:|---|
| 500 K | any | 1 | 1 | 500,000 | 500,000 | < 1 M short-circuit |
| 1 M | DumpAndRestore | 10 | 1 | 100,000 | 100,000 | seed `1M / 100K = 10`; chunk floor already binds |
| 1 M | MongoDriver | 2 | 40 | 500,000 | 12,500 | seed 10 → `Ceil(10 × 10 / 40) = 3`; driver chunk floor 400 K caps to 2; segs saturate |
| 10 M | DumpAndRestore | 100 | 1 | 100,000 | 100,000 | seed `10M / 100K = 100` |
| 10 M | MongoDriver | 25 | 40 | 400,000 | 10,000 | `Ceil(10 × 100 / 40) = 25`; driver chunk floor 400 K just binds |
| 100 M | DumpAndRestore | 1,000 | 1 | 100,000 | 100,000 | seed `100M / 100K = 1,000` |
| 100 M | MongoDriver | 250 | 40 | 400,000 | 10,000 | `Ceil(10 × 1,000 / 40) = 250`; driver chunk floor 400 K caps |
| 1 B | DumpAndRestore | 1,000 | 1 | 1,000,000 | 1,000,000 | **large tier**; seed `1B / 1M = 1,000` |
| 1 B | MongoDriver | 250 | 40 | 4,000,000 | 100,000 | **large tier**; `Ceil(10 × 1,000 / 40) = 250`; driver chunk floor 4 M caps |
| 2 B | DumpAndRestore | 2,000 | 1 | 1,000,000 | 1,000,000 | **large tier**; seed `2B / 1M = 2,000` |
| 2 B | MongoDriver | 500 | 40 | 4,000,000 | 100,000 | **large tier**; `Ceil(10 × 2,000 / 40) = 500`; driver chunk floor 4 M caps |

## Notes on `$sample` behavior

- `$sample` stays on MongoDB's fast random-cursor path when **all** of the following hold: it is the first stage of the pipeline, the collection has more than 100 documents, and `size` is **less than 5%** of the total document count. The 5% cap in `CalculatePartitioningStrategy` keeps the no-`$match` case on the fast path.
- A `$match` stage is prepended to `$sample` when **either** the user supplies a `UserFilter` **or** the collection has more than one `_id` BSON type (the `$type` predicate added by `SamplePartitioner.BuildDataTypeCondition`). In both cases `$sample` is no longer the first stage, so MongoDB falls back to full-scan + top-k sort. We cap the absolute `$sample` size at `MaxSamples(withMatch) = 3,000` to bound that sort cost. The 5% cap is still applied but is rarely the binding constraint when a `$match` precedes `$sample`.
- The `_id`-type probe runs once per collection in a pre-pass before `CalculatePartitioningStrategy`, so the planner always knows whether a `$type` `$match` will be in play.
- Quantile selection collapses the oversampled set back to the desired boundary count, producing equally-sized partitions even when the underlying sample is skewed. The effective oversample factor degrades from 10× toward 1× as `chunks × segments` approaches 3,000.

## Notes on tiered floors

The 100 M-doc breakpoint reflects two practical limits:

- **`MinDocsPerChunk` tier (100 K → 1 M)**: At 100 K docs/chunk, a 1 B-doc collection produces 10,000 chunks of work-item metadata, and a 2 B-doc collection produces 20,000. Coarsening chunks to 1 M docs each on large collections keeps the chunk catalogue an order of magnitude smaller without losing meaningful parallelism (chunks remain ≥ `MaxSegments × something useful`).
- **Segment floor tier (10 K → 100 K)**: At 10 K docs/segment, a single segment on a multi-billion-doc collection processes too little work to amortize the per-segment overhead (cursor creation, bulk-write batching, retry/checkpoint bookkeeping). Coarsening to 100 K docs/segment keeps segments meaningful while still giving the driver path 40-way parallelism per chunk. The segment floor is not a standalone constant — it is `0.1 × GetMinDocsPerChunk`, so it tracks the chunk tier automatically.

The segment floor is derived directly from the chunk floor (`GetMinDocsPerSegment = 0.1 × GetMinDocsPerChunk`), so both move in lockstep at the same `LargeCollectionThreshold = 100,000,000` breakpoint, and `PartitionFactor` scales them together. `GetMinDocsPerChunk` is the single source of truth for the policy.

## Segment-saturation policy (MongoDriver)

For `MongoDriver`, segments are the unit of write parallelism within a chunk. The partitioner **never drops segment count below `MaxSegments` just because individual chunks are too small** — instead it reduces chunk count so each chunk is large enough to support full segment parallelism. The effective per-chunk floor is therefore:

```
perChunkFloor = max(GetMinDocsPerChunk(docCount), MaxSegments × GetMinDocsPerSegment(docCount))
```

With `MaxSegments = 40` and the tiered segment floors:

| Tier | Per-segment floor | Segment-saturation floor (`40 × seg-floor`) | `MinDocsPerChunk` | **Effective `perChunkFloor`** |
|---|---:|---:|---:|---:|
| `≤ 100 M` docs | 10 K | 400 K | 100 K | **400 K** |
| `> 100 M` docs | 100 K | 4 M | 1 M | **4 M** |

`DumpAndRestore` keeps `segments = 1` by design, so this segment-saturation floor doesn't apply — it uses the bare `MinDocsPerChunk` (100 K / 1 M) as its chunk floor.
