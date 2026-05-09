# Part 1: Query-First Is a Different System Design Goal

*Why "query-first" is not just an implementation detail, but a different architecture altogether.*

Most IoT backends are designed around a simple promise: accept messages reliably and at high throughput. That is a reasonable starting point, because devices publish first and humans ask questions later.

But many products are not really selling "ingest." They are selling visibility. The user is not paying for the fact that a broker accepted a payload at 10:03:41. The user is paying for the ability to ask:

- "Show me temperature spikes for site A in the last 6 hours."
- "Compare this device against the fleet baseline."
- "Give me all failed readings grouped by firmware version."

That change in product promise matters. If the promise is "ingest anything fast," then the system should optimize for append throughput, durability, and delivery semantics. If the promise is "query device data immediately," then the system should optimize the read path: data layout, pruning, caching, and query execution.

That is the core thesis of this series: **query-first is not an optimized version of an ingest-first architecture. It is a different design goal with a different shape of system.**

## The Traditional Pipeline Optimizes Arrival, Not Use

The standard IoT data path usually looks something like this:

```text
Devices
  |
  v
MQTT Broker
  |
  v
Raw Messages
  |
  v
Queue or Stream
  |
  v
Operational Store
  |
  v
ETL / Parsing / Normalization
  |
  v
Analytics DB or Lake
  |
  v
Dashboards / Queries
```

This design is common for good reasons:

- brokers are very good at accepting bursts of small messages
- queues make failure handling and replay easier
- raw payload retention preserves flexibility
- downstream consumers can evolve independently

If the main problem is "do not lose events," this pipeline works well.

The problem appears later. A user query is often far away from the original write path. Before data becomes useful, it may need to be decoded, cleaned, repartitioned, enriched, and copied again into an analytical system. That means the system was fast at receiving data, but slower at turning it into answers.

In other words, traditional pipelines are usually **message-first**. They prioritize transport and durability before queryability.

That is not wrong. It is just a different optimization target.

## Query-First Starts From the Read Path

A query-first system asks a different first question:

> What representation should the data take if the next important operation is filtering, grouping, aggregating, and scanning?

That changes the pipeline much earlier than most teams expect.

```text
Devices
  |
  v
MQTT Broker
  |
  v
Schema-Aware Ingest
  |
  v
Arrow RecordBatch
  |
  v
Parquet Files
  |
  v
Object Storage + Table Metadata
  |
  v
Cache Layer
  |
  v
SQL Engine
  |
  v
Dashboards / APIs / Ad Hoc Queries
```

The difference is not just that the later stages are faster. The difference is that the system starts shaping data for query immediately:

- data is typed earlier
- batches are formed earlier
- storage layout is chosen for scans, not only writes
- metadata matters as much as payloads
- cache design becomes part of the core architecture
- the SQL engine is not an afterthought

In a query-first design, the ingest path is still important, but it is no longer the only thing that defines the system.

## Why Architecture Changes When Read Path Matters More

When the read path becomes the primary product concern, several architectural decisions flip.

### 1. Serialization stops being only a wire concern

In an ingest-first system, serialization is mostly about transport: can devices publish cheaply, and can services decode messages safely?

In a query-first system, serialization becomes a compute decision. The question is no longer just "How do I get bytes through the network?" It becomes "How much work will every future query have to repeat because of the format I picked today?"

If data lands as millions of small JSON objects, every analytical read pays parsing cost again. If it lands as a columnar batch with typed arrays, much of that work is already done.

### 2. Storage is chosen for scan patterns, not only durability

An ingest-first pipeline can treat storage like a sink: write the events somewhere durable and let downstream systems reorganize them later.

A query-first pipeline cannot be so casual. File sizes, partitioning, sort order, schema evolution, and object-store access patterns all affect user-facing latency. Storage layout becomes part of the serving path.

### 3. Metadata becomes part of the data plane

Once users query data directly from files or table-backed object storage, metadata is no longer background bookkeeping. It decides:

- which files must be opened
- which partitions can be skipped
- which schema version applies
- which snapshot is consistent

That means transaction logs, manifests, statistics, and schema tracking move closer to the center of the architecture.

### 4. Caching is no longer an optional acceleration layer

In many broker-led systems, caches are mostly about delivery or temporary fan-out. In a query-first system, caching often becomes essential because object storage is durable but too cold for every repeated read. The cache is not hiding a slow corner case. It is protecting the main user experience.

### 5. The query engine becomes a first-class subsystem

If users interact through SQL or SQL-like filters, the execution engine is part of the product. This is a different posture from using analytics as a later export path. Suddenly, engine selection, predicate pushdown, vectorized execution, and file pruning are not implementation details. They are user-visible behavior.

## Traditional Pipeline vs Query-First Pipeline

The contrast is easiest to see side by side.

| Question | Ingest-First System | Query-First System |
| --- | --- | --- |
| Primary promise | Accept events fast and reliably | Return answers fast from fresh data |
| Initial data shape | Raw messages | Typed batches |
| Main write target | Queue, broker log, row store | Columnar files on object storage |
| Repeated CPU cost | Paid during queries and ETL | Shifted earlier during ingest |
| Metadata importance | Often secondary | Central to correctness and performance |
| Cache role | Delivery acceleration | Query-serving necessity |
| Main failure concern | Backpressure, replay, ordering | Snapshot consistency, pruning, hot/cold data behavior |

Neither side is universally better. The architecture follows the promise.

If the system mostly forwards data to other consumers, query-first may add unnecessary structure. If the product lives or dies by ad hoc filtering and aggregates, ingest-first pushes too much cost downstream.

## Complexity Moves, Not Disappears

The most important thing to understand is that query-first systems are not magically simpler. They just move complexity into different places.

```text
Ingest-first complexity              Query-first complexity
------------------------            ------------------------
- Backpressure handling             - Schema-aware ingest
- Replay and stream coordination    - Batch formation
- ETL pipelines                     - Columnar layout decisions
- Repeated parsing at read time     - Table metadata and transactions
- Multiple storage hops             - Cache policy and query planning
```

This is the wrong mental model:

- "query-first removes ETL, therefore the system is simpler"

This is closer to the truth:

- "query-first spends more design effort earlier so query-time work becomes smaller, cheaper, and more predictable"

The system still has to solve hard problems. They are just different hard problems.

In a traditional pipeline, complexity tends to accumulate in:

- downstream parsing
- repeated transformations
- duplicated storage systems
- lag between arrival and usability

In a query-first pipeline, complexity tends to accumulate in:

- schema enforcement at ingest
- batching policies
- object-store table management
- cache admission and eviction
- coupling between storage layout and query behavior

That trade can be excellent if the workload is query-heavy. It can be terrible if the workload mostly cares about transport, fan-out, or ultra-low-latency delivery.

## A Simple Running Example

Assume 500,000 devices publish telemetry every few seconds:

- `device_id`
- `ts`
- `site_id`
- `firmware_version`
- `temperature`
- `humidity`
- `battery_mv`
- `status_code`

An ingest-first design will usually preserve each reading as an independent message and rely on later systems to build analytical structure.

A query-first design asks for that structure earlier. It wants readings batched together by a shared schema, stored in a way that lets the engine skip irrelevant columns and scan large chunks efficiently. The point is not to make writes free. The point is to avoid paying the same decode-and-rebuild cost on every analytical read.

If one dashboard is queried thousands of times per hour, and every query repeatedly decodes row-oriented payloads, the "cheap ingest" story starts to look expensive. The CPU cost did not vanish. It simply moved to the read path, where it is now multiplied by user demand.

That multiplication effect is the real motivation behind query-first design.

## Why the Read Path Dominates So Quickly

Teams often underestimate how quickly read-path cost overtakes write-path cost.

Suppose ingest does a modest amount of extra work:

- validate a schema
- append values into typed arrays
- flush batches into a columnar file

That may increase write CPU per message. But a single stored batch can now serve many future queries without reparsing each original payload individually.

This is the asymmetry:

- ingest happens once per event
- querying can happen many times per event

Once the same data is used for dashboards, alerts, retrospective analysis, tenant reports, and debugging, the read path becomes the economic center of the system. At that point, optimizing only the write path is often optimizing the cheaper side of the bill.

## What Query-First Does Not Mean

It does **not** mean:

- every system should immediately convert all traffic into Parquet
- brokers and queues stop mattering
- low-latency operational use cases disappear
- raw message retention becomes useless

It means the system should be honest about its dominant job.

If the dominant job is message movement, design for movement.

If the dominant job is repeated analytical access over fresh data, design for queryability.

Those two goals overlap, but not enough to treat them as the same architecture.

## When Query-First Is the Wrong Goal

A query-first architecture is usually a poor fit when:

- data is rarely queried after arrival
- the main requirement is ultra-low-latency delivery, not analytics
- payloads are mostly large opaque blobs rather than queryable fields
- workloads depend more on local stream semantics than SQL access
- operational simplicity matters more than read efficiency

This matters because query-first systems can look elegant on diagrams while being wasteful in practice. If the read path is not important enough, then early typing, batching, file layout work, and cache management become overhead without leverage.

## The Real Design Principle

The design principle is simple:

**optimize the system around the operation users repeat, not the operation engineers notice first.**

Engineers notice ingest first because it is the front door. Users notice query latency first because it is the actual product surface.

That is why query-first architecture deserves to be discussed as its own design category. It is not just "Kafka plus some analytics later." It is a system where representation, storage, cache, and execution are all chosen so that data is useful immediately, not merely durable.

## Where the Series Goes Next

This first part established the thesis:

- ingest-first and query-first optimize different promises
- once queryability matters, serialization and storage layout move into the critical path
- complexity does not disappear; it relocates

The next part makes that concrete by looking at serialization and layout: JSON vs Protobuf vs Arrow, why columnar representation changes the compute model, and where that approach breaks down for large binary payloads.
