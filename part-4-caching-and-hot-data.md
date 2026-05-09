# Part 4: Caching and Hot Data

*How a query-first system avoids turning every read into a cold object-store fetch.*

By Part 3, the architecture has reached a useful but incomplete state:

- data is written in columnar form
- object storage holds the durable truth
- a transaction layer defines consistent table snapshots

That solves durability and table correctness.

It does **not** solve serving latency by itself.

Object storage is a strong system of record, but it is still a cold medium relative to memory and local disk. If every dashboard refresh, tenant query, or filter operation has to fetch metadata and data files from remote object storage, the user pays for that distance every time.

That is why caching becomes unavoidable in a query-first design. Not as a decorative optimization, but as part of the actual serving architecture.

The core question in this chapter is:

> What data should stay hot, in what form, for which class of read?

That question is more subtle than "put recent files in RAM."

It forces the system to decide:

- what gets cached
- where it gets cached
- who the cache is serving
- what is allowed to evict what

## Why Object Storage Alone Is Too Cold

Object storage works very well for:

- cheap durability
- large-scale retention
- immutable file storage
- broad compatibility with analytical engines

It works less well for:

- repeated low-latency reads of the same hot subset
- bursty dashboard traffic
- high query fan-out over recent data
- workloads that reopen the same metadata and files again and again

The problem is not only bandwidth. It is the combined cost of:

- request latency
- request count
- file-open overhead
- repeated metadata resolution
- remote reads for data that was just read moments ago

If the last hour of telemetry is queried constantly, then repeatedly pulling it from object storage is a bad use of both time and money.

```text
Cold-only serving path

query
  |
  v
SQL engine
  |
  v
table metadata lookup
  |
  v
remote object-store GETs
  |
  v
decode / scan
  |
  v
result
```

That path is correct. It is often not fast enough.

## Caching Is Not a Second Source of Truth

Before discussing cache types, one rule matters:

**the cache is a serving layer, not the system of record.**

That means:

- object storage plus table metadata remains authoritative
- cache contents may be stale, partial, or evictable
- recovery must not depend on the cache surviving
- correctness comes from snapshot semantics, not cache presence

This distinction keeps the design clean.

If the cache starts behaving like hidden durable state, the system inherits the complexity of a second persistence layer without the correctness guarantees of one.

In a good query-first design:

- durability lives in object storage
- visibility lives in the transaction layer
- speed lives in the cache

## What Exactly Gets Cached

Caching is not just about whole files. Different systems cache different layers:

- table metadata
- manifest data or file lists
- Parquet footers and statistics
- whole files
- file ranges or pages
- decoded column chunks
- query results

Those choices matter because they have different tradeoffs.

Caching entire files is simple but coarse. Caching decoded columns is more efficient for repeated scans, but more complex and memory-intensive. Caching result sets can be extremely effective for repeated dashboards, but less useful for ad hoc queries with many filter combinations.

The right answer depends on what the workload repeats.

## Memory Cache vs Disk Cache vs Hybrid Cache

The next decision is where hot data should live.

## Option A: Memory Cache

Memory is the fastest cache tier.

Advantages:

- lowest latency
- best fit for repeated hot reads
- useful for metadata, footers, small hot files, and decoded structures
- no extra local I/O once populated

Costs:

- expensive and limited capacity
- volatile across restarts
- easy to pollute with large one-off scans
- contention with query execution memory

Pure in-memory caching works well when the hot set is small and highly repetitive. It works poorly when the workload is too large or too bursty to fit comfortably in RAM.

## Option B: Disk Cache

Local disk is slower than memory, but far larger and cheaper.

Advantages:

- much larger effective cache capacity
- persists across process restarts if managed locally
- good fit for reused files and column chunks
- can absorb hot-but-not-hottest data economically

Costs:

- higher latency than memory
- local I/O can still become a bottleneck
- cache index management becomes more important
- stale files and cleanup need active management

Disk caching is often a strong match for analytical workloads because many hot reads are not microsecond-sensitive. They just need to avoid going back to remote object storage.

## Option C: Hybrid Cache

Most practical systems want both.

```text
Hybrid cache path

query
  |
  v
SQL engine
  |
  v
memory cache  ---- hit --> serve immediately
  |
 miss
  v
disk cache    ---- hit --> promote if useful
  |
 miss
  v
object storage
  |
  v
fill disk and/or memory according to policy
```

A hybrid design usually works better because it matches temperature tiers:

- hottest metadata and repeated working sets stay in memory
- warm files and column data stay on local disk
- cold history stays in object storage

This gives the system elasticity without pretending every useful byte belongs in RAM.

## Why a Hybrid Cache Is Better Than a Simple LRU Map

It is easy to say "we will just add an LRU cache."

That is rarely enough.

A simple LRU policy assumes:

- all misses cost roughly the same
- all items are equally worth caching
- recency is the main signal of future value

Those assumptions are weak for analytical systems.

Consider these two reads:

- a dashboard repeatedly scanning recent hourly partitions
- one ad hoc query scanning a huge historical range once

If both are admitted equally into a naive LRU cache, the large historical scan may evict the small hot working set and make the common dashboard slower.

That is why analytical caching usually needs more than bare recency. It needs policy.

## Cache Admission Matters More Than People Expect

Eviction decides what leaves the cache. Admission decides what is allowed in at all.

In many systems, admission is the more important control.

Good admission policy asks:

- is this data likely to be reused?
- how expensive is it to fetch again remotely?
- how large is it relative to its expected value?
- is this a foreground query or a background scan?
- is the data recent enough to be part of the hot set?

Examples of useful admission rules:

- always admit table metadata and Parquet footers
- prefer recent partitions over cold historical scans
- do not admit very large one-off scans into memory
- admit to disk but not memory for moderately reusable files
- promote only after a second hit, not the first

This is how the cache stops being a random spill bucket and starts behaving like a serving strategy.

## Eviction Is About Protection, Not Just Reclamation

Eviction policy should protect the workload that matters most.

That means the system may want different treatment for:

- metadata vs data pages
- recent partitions vs historical partitions
- dashboard traffic vs long-running exploratory scans
- tenant-critical workloads vs background maintenance

A practical eviction policy often combines:

- recency
- frequency
- size awareness
- class-based reservation

For example, the system may reserve memory for:

- current snapshot metadata
- recent partition footers
- active tenant working sets

while allowing larger scan data to live mostly on disk or bypass cache entirely.

That is more effective than letting every request compete in one undifferentiated pool.

## QoS-Aware Caching

Once the system also serves MQTT-style delivery or message replay semantics, caching becomes more complicated.

Not all messages have the same value.

For example:

- QoS 1 or QoS 2 traffic may have stronger delivery expectations
- QoS 0 traffic may be transient and disposable from a delivery perspective
- some tenants may pay for fresher or faster query access than others

That means a single cache rule for all traffic is usually the wrong abstraction.

## Why QoS 0 Might Not Deserve Equal Cache Space

If the cache exists only to help message delivery, then low-value transient traffic may not deserve much local residency. It may be cheaper to let it pass through than to spend scarce cache capacity on it.

This is especially true when:

- messages are short-lived
- replay is not required
- the system does not expect repeated reads

From a delivery-serving perspective, admitting all QoS 0 traffic can look like pollution.

## Why Query Workloads Complicate That Answer

The answer changes if the same data is also part of the query surface.

A transient publish may still deserve cache space if:

- recent queries are concentrated on the newest data
- dashboards repeatedly hit the same recent time window
- object-store round trips would dominate latency

This is the important distinction:

- delivery-serving asks whether the message needs to be retained for retransmission
- query-serving asks whether the data is likely to be read again soon

Those are different questions.

The same event can be low value for delivery and high value for query acceleration.

## Query-Serving vs Delivery-Serving Tradeoffs

This is one of the central cache design tensions in an IoT backend.

```text
Delivery-serving cache priorities        Query-serving cache priorities
---------------------------------        ------------------------------
- retransmission usefulness              - read reuse probability
- session behavior                       - recency of analytical access
- QoS semantics                          - partition / file hotness
- per-message urgency                    - scan cost if missed
- broker pressure                        - dashboard and API latency
```

A system that serves both roles has to decide which view dominates in each tier.

One practical pattern is:

- keep delivery-critical short-lived state separate from analytical cache state
- allow shared admission signals, but not shared eviction pools
- avoid letting bursty publish traffic evict the hottest analytical working set

Without this separation, one workload can silently degrade the other.

## A Better Mental Model: Cache by Reuse Class

Instead of asking "Should all messages be cached?" a better question is:

> Which data has a high probability of near-term reuse, and by which subsystem?

That leads to reuse classes such as:

- always-hot metadata
- recent analytical partitions
- active dashboard working sets
- large historical scans
- delivery-only transient state

Each class can then have different rules for:

- admission
- priority
- residency tier
- eviction aggressiveness

This is much easier to control than a single global cache.

## A Practical Hybrid Cache Design

For a query-first IoT pipeline, a practical design often looks like this:

```text
Tier 1: Memory
- latest table metadata
- manifest summaries
- Parquet footers
- hottest recent column chunks
- frequently reused query fragments

Tier 2: Local disk
- recent Parquet files
- warm column chunks
- spillable scan data
- recently accessed historical partitions

Tier 3: Object storage
- full durable history
- cold data
- source of truth for refills
```

The serving path then becomes:

```text
query
  |
  v
resolve snapshot metadata from memory if possible
  |
  v
read warm files / chunks from disk if possible
  |
  v
fetch only true misses from object storage
  |
  v
promote according to admission policy
```

This keeps the cache closely aligned with the query engine's actual work.

## What the Cache Should Optimize For

The cache should not try to optimize everything equally.

A good priority order is usually:

1. protect correctness-critical metadata access from avoidable latency
2. keep the hot recent working set local
3. prevent one-off scans from destroying locality
4. minimize expensive remote fetches for repeated reads
5. degrade gracefully under pressure

That last point matters. A good cache is not one that never misses. It is one that misses in ways the system can tolerate.

## Failure and Recovery Behavior

Because the cache is non-authoritative, failure handling should be simple:

- cache loss should hurt latency, not correctness
- restart should rebuild heat gradually
- stale entries should be invalidated by snapshot changes or version checks
- background refill should not block forward progress unnecessarily

This is another reason not to turn the cache into a shadow database. Recovery should be boring.

## When This Design Is the Wrong Choice

A sophisticated hybrid cache may be unnecessary when:

- the dataset is small enough to stay in memory entirely
- queries are rare enough that object-store latency is acceptable
- the workload is mostly delivery-oriented, not query-oriented
- operational simplicity matters more than read optimization
- the query engine already embeds sufficient local caching for the scale involved

Caching is powerful, but it still has operational cost:

- tuning admission rules
- sizing tiers
- managing local disk usage
- debugging hot-set churn

That complexity only pays off if read reuse is real.

## Decision Summary

Once object storage becomes the durable truth, caching becomes the layer that makes the query-first promise feel fast.

The important choices are not just:

- memory or disk

They are:

- which data shape is cached
- which tier it belongs in
- which reads deserve protection
- which workloads are allowed to evict others

That is why the right design is usually a hybrid cache with policy-driven admission and class-aware eviction, not a single undifferentiated LRU.

## Where the Series Goes Next

This part covered the hot-data layer:

- object storage is durable, but too cold for every repeated read
- memory is fast but scarce
- disk is slower but economically useful
- hybrid caching matches the real heat distribution of analytical workloads
- admission policy matters as much as eviction
- delivery-serving and query-serving do not always want the same cache behavior

The next part looks at the query layer itself: ClickHouse vs DataFusion vs DuckDB, external service vs embedded engine, a stable SQL surface over different execution engines, and when this whole architecture is simply the wrong fit.
