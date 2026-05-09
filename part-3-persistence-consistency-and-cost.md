# Part 3: Persistence, Consistency, and Cost

*Why object storage plus a transaction layer changes where the distributed system really lives.*

In Part 2, the argument was that Arrow and Parquet change the compute model by moving work earlier and storing data in a query-native shape.

That still leaves a harder question:

> Where does the durable truth of the system live?

For a traditional event pipeline, the answer is often straightforward:

- Kafka is the durable log
- a database is the serving store
- analytics is a downstream copy

In a query-first system built around object storage, that answer changes.

The durable truth is no longer "the last broker segment" or "the primary database replica." It becomes:

- immutable data files in object storage
- plus metadata that says which files belong to the current table snapshot

That sounds like a storage detail. It is not. It changes:

- how writers coordinate
- what readers consider consistent
- what failures matter
- where operating cost shows up

This is the point where a query-first architecture stops being just "Parquet on S3" and becomes a real distributed storage design.

## The Real Persistence Question

Persistence is not just "where bytes are stored." It is the answer to three questions:

- what is the system of record?
- how do concurrent writers avoid corrupting it?
- how do readers see a coherent view while writes continue?

If you skip those questions, raw files look deceptively simple. But raw files alone do not tell you:

- which files form a valid table state
- which schema version applies
- which files were replaced by compaction
- whether a partial write should be visible

That is why persistence and consistency have to be discussed together.

## The Main Options

A query-first pipeline usually ends up choosing among four broad persistence models.

## Option A: Kafka or a Broker Log

Kafka is excellent at what it is designed to do:

- ordered append
- replay
- consumer decoupling
- durable event transport

It is a very strong **log**. It is not automatically a good analytical table.

You can keep long retention in Kafka and build analytics from it, but that tends to create familiar problems:

- queries operate over row-oriented messages
- historical scans become awkward
- schema evolution is not the same as table evolution
- compaction and partitioning are optimized for stream semantics, not analytical pruning

Kafka answers "what happened, in what order?" much better than it answers "what files should this SQL query scan right now?"

That makes Kafka a useful input to a query-first system, but usually not the final persistence layer.

## Option B: A Database

A database gives stronger transactional semantics out of the box.

Advantages:

- well-defined consistency model
- mature concurrency control
- indexes and serving APIs
- operational familiarity

Costs:

- analytical scans can become expensive at scale
- storage cost is usually higher than object storage
- ingest and analytics compete for the same system resources
- large historical retention can become operationally awkward

Databases are excellent when the workload is operational, mutable, and point-lookup heavy. They are less attractive when the design goal is cheap retention of large analytical datasets with scan-oriented query engines.

## Option C: Raw Files on Object Storage

This is the first option that looks like a lakehouse architecture:

- write Parquet files
- upload them to S3 or equivalent object storage
- point a query engine at the bucket

The appeal is obvious:

- cheap storage
- simple durability model
- native fit for Parquet
- easy integration with analytical engines

But raw files alone create several serious problems:

- no authoritative snapshot of the table
- concurrent writers can step on each other logically
- readers may discover files by expensive listing
- schema changes are difficult to reason about
- compaction and replacement create visibility ambiguity

Object storage can keep the bytes safe. It does not, by itself, define a table.

## Option D: Object Storage Plus a Transaction Layer

This is the Delta/Iceberg-style approach.

The idea is simple:

- data lives as immutable files in object storage
- metadata tracks snapshots, schemas, manifests, and file membership
- readers query a consistent snapshot
- writers add new files and publish a metadata commit

That gives the system something raw Parquet cannot provide: a reliable answer to "what is the table right now?"

```text
Writers
  |
  +--> write Parquet data files ----------------------+
  |                                                   |
  +--> publish metadata commit / snapshot ------------+--> object storage

Readers
  |
  +--> read latest committed snapshot
        |
        v
      resolve file list
        |
        v
      scan only those files
```

This is the architectural pivot.

The distributed system boundary is no longer just "the storage bucket." It becomes:

- immutable objects for data
- plus a metadata protocol for table state

That metadata layer is doing the work a database catalog or storage engine would normally do internally.

## Why Raw Parquet Is Not Enough

It is tempting to think that Parquet is already a structured format, so maybe that should be sufficient.

It is not sufficient for multi-writer, queryable persistence.

Parquet tells you how one file is encoded. It does **not** tell you:

- which of 10,000 files are active in the current table snapshot
- which files were superseded by compaction
- whether a delete or overwrite is complete
- whether two writers committed conflicting table changes

A file format is not a table format.

That distinction matters because many operational bugs come from treating directory contents as table truth.

Without a transaction layer, readers are forced into fragile behavior such as:

- list a prefix
- hope the listing reflects a coherent state
- infer table membership from filenames
- trust ad hoc conventions for overwrite and cleanup

That is acceptable for demos. It is weak for production systems.

## How the Transaction Layer Changes the System Boundary

Once you add a Delta/Iceberg-style table format, the durable system of record becomes:

- the set of data files
- the metadata log or manifest structure
- the rule for publishing a new snapshot

That changes the architecture in two important ways.

### 1. Readers stop discovering truth by scanning storage directly

Instead of "list every object under this prefix," readers ask:

- what is the latest valid snapshot?
- what manifests or metadata files describe it?
- which data files belong to that snapshot?

This is usually faster, cheaper, and more correct than directory-style discovery.

### 2. Writers coordinate through metadata, not by mutating files in place

Writers do not usually update existing Parquet objects. They create new immutable files, then try to publish a metadata change that makes those files part of the table.

That is a very different write discipline from row-store databases or mutable file systems.

## Optimistic Concurrency

Once writes are mediated through metadata commits, concurrency control becomes a central concern.

Most lakehouse-style systems use some form of optimistic concurrency.

The pattern looks like this:

```text
1. Writer reads current table snapshot: version N
2. Writer prepares new Parquet files
3. Writer computes intended table change
4. Writer attempts to commit version N -> N+1
5. If another writer already changed the table, commit fails
6. Writer refreshes snapshot, reconciles, and retries
```

This works because object storage is good at immutable object writes, but not at acting like a row-locking database engine.

Optimistic concurrency accepts that conflicts are possible and resolves them at commit time instead of preventing them with heavyweight locking.

That is usually a good fit for analytical ingestion because:

- files are immutable
- write conflicts are often metadata conflicts, not byte-range conflicts
- most writers append rather than update the same logical rows repeatedly

The important point is that the transaction layer is not pretending object storage is a database. It is building database-like table semantics on top of immutable objects.

## What Conflicts Actually Look Like

Conflicts are not always "two writers uploaded the same file." More often they look like:

- two jobs both compact the same partitions
- one writer appends data while another rewrites files for maintenance
- a schema change races with an ingest commit
- a delete or upsert is planned against an old snapshot

In an optimistic model, conflicts are detected when the writer tries to publish metadata based on stale assumptions.

That means correctness depends on snapshot validation rules, not just on storage durability.

## Fault Tolerance in an Object-Storage Table System

Fault tolerance also changes shape in this model.

The useful mental split is:

- data-file durability
- metadata-commit correctness

### Failure before file upload completes

If the writer crashes before new files are fully written, there is no valid commit. Readers should never see those partial results.

### Failure after file upload, before metadata commit

This is common and important.

The new files may exist in object storage, but they are not part of any committed snapshot yet. They are effectively orphaned files until a cleanup job removes them.

This is one of the clean properties of immutable-file design:

- uncommitted files can exist
- but they do not become visible table state without the metadata commit

### Failure after metadata commit succeeds

Once the commit is durable, readers can observe the new snapshot. Even if the writer crashes immediately afterward, the table state is still valid because visibility was defined by metadata publication, not by the process staying alive.

### Failure during compaction or overwrite

Compaction is especially sensitive because it replaces many small files with fewer larger ones.

A safe design does not delete old files first. It:

1. writes replacement files
2. publishes a new snapshot that points to them
3. retires old files logically
4. garbage-collects old files later

That sequence is a direct consequence of immutable files plus metadata snapshots.

## Why This Fault Model Is Attractive

This design has a strong property:

- the table changes by publishing new snapshots
- not by mutating live files in place

That gives you cleaner recovery behavior:

- snapshot visible or not visible
- file active or not active
- commit valid or rejected

The main downside is that cleanup becomes a first-class operational task. Orphan files, old snapshots, metadata logs, and compacted-away data all have to be managed deliberately.

## S3 Economics Shape the Architecture

At this point, the persistence layer is not just about correctness. It is also about billing and latency.

When a system stores Parquet on S3 or similar object storage, cost shows up through more than just bytes stored. The main request categories matter:

- `PUT` for writing new objects
- `GET` for reading objects
- `LIST` for discovering objects

You do not need exact cloud pricing tables to understand the design pressure:

- many small writes create many `PUT`s
- many small reads create many `GET`s
- directory-style discovery increases `LIST` overhead
- request latency can dominate before raw bandwidth does

That means cloud economics starts shaping the architecture directly.

## The Small-Files Problem

The most common failure mode in object-storage-backed analytics is not catastrophic corruption. It is death by tiny files.

If every micro-batch becomes a separate Parquet object:

- request counts grow rapidly
- metadata overhead grows
- query planning gets heavier
- engines open too many files
- throughput drops even when total bytes are modest

A dataset made of thousands of tiny Parquet files can perform much worse than a smaller number of well-sized files containing the same total data.

This is why a query-first system cannot think only in terms of schema. It also has to think in terms of file economics.

## Why `LIST` Is a Hidden Tax

Many naive designs assume readers can simply list a bucket prefix and query whatever they find.

That has several drawbacks:

- `LIST` requests add latency
- repeated discovery work scales poorly
- object naming conventions become part of correctness
- planners do work that a table metadata layer should have done once

A transaction layer helps here because readers can usually consult metadata manifests rather than perform broad prefix discovery.

That is both a correctness improvement and a cost improvement.

## Batching and Compaction

To control both performance and request cost, the system usually needs two policies:

- batching at ingest
- compaction after ingest

### Batching

Instead of flushing every tiny set of records immediately, the writer accumulates a larger batch before producing a Parquet file.

This reduces:

- `PUT` count
- file-open overhead
- metadata fan-out

But batching also increases:

- ingest latency
- memory pressure
- risk of larger retry units

So batching is not free. It is a latency-versus-economics trade.

### Compaction

Even with batching, real workloads still generate fragmented file layouts over time. Compaction rewrites many small files into fewer larger ones, often improving:

- query latency
- scan efficiency
- metadata size
- request count

But compaction is not free either:

- it consumes CPU and I/O
- it creates write amplification
- it may conflict with concurrent ingest
- it needs careful scheduling to avoid fighting the foreground workload

Compaction is one of the clearest examples of complexity moving rather than disappearing. You save query cost later by paying maintenance cost periodically.

## Fault Tolerance Meets Cost Control

These concerns are connected, not separate.

For example:

- batching reduces `PUT` volume, but increases the amount of data retried on failure
- compaction reduces `GET` and planning overhead, but creates more background writes
- immutable snapshots improve correctness, but create garbage-collection work

This is the deeper lesson: cloud architecture is shaped by billing and failure handling at the same time.

You cannot really design the persistence layer by thinking only about correctness. The request pattern is part of the correctness story because it determines whether the system remains operable under real load and real budget constraints.

## A Practical Persistence Shape

For a query-first IoT pipeline, a practical design often looks like this:

```text
ingest workers
  |
  +--> build Arrow batches
  |
  +--> write Parquet data files
  |
  +--> attempt table commit
          |
          v
      transaction metadata
          |
          v
      object storage as system of record

query engine
  |
  +--> read latest table snapshot
  |
  +--> resolve manifests / file list
  |
  +--> scan committed files only

maintenance jobs
  |
  +--> compaction
  +--> orphan cleanup
  +--> metadata checkpointing
```

This layout makes the division of responsibility explicit:

- ingest creates immutable data
- the transaction layer decides visibility
- readers trust snapshots, not directory listings
- maintenance keeps the table economically healthy

## When This Design Is the Wrong Choice

This architecture is usually a poor fit when:

- you need single-row transactional updates at high frequency
- query scans are not important enough to justify table maintenance
- object-store latency is unacceptable for the workload
- the team does not want to operate compaction, cleanup, and metadata hygiene
- the data is better modeled as a stream log than as an analytical table

Object storage plus a transaction layer is powerful, but it is not a free substitute for every database or stream system.

## Decision Summary

The persistence decision in a query-first system is not just "store Parquet somewhere durable."

The real decision is:

- whether object storage becomes the system of record
- whether a transaction layer defines table truth
- whether the team is willing to manage batching, compaction, and metadata as core parts of the system

That trade is attractive because it combines:

- cheap durable storage
- query-friendly files
- consistent snapshots
- flexible engine access

But it only works well if the system respects both sides of the design:

- correctness through metadata commits
- cost control through file discipline

## Where the Series Goes Next

This part covered why persistence is really about table semantics and cloud economics together:

- Kafka is a strong log, but not the ideal final analytical table
- databases give strong transactional behavior, but are often expensive for large analytical retention
- raw Parquet files are useful, but insufficient as a real system of record
- Delta/Iceberg-style table formats add snapshot semantics and optimistic concurrency
- S3 request patterns, small files, batching, and compaction shape the actual architecture

The next part turns to caching and hot data: how to stop every read from becoming a cold object-store access, and how memory, disk, admission policy, and QoS trade against each other.
