# Part 2: Serialization and Storage Layout

*Why Arrow and Parquet change the compute model, not just the file format.*

In Part 1, the core argument was that query-first is a different system design goal from ingest-first. This part makes that argument concrete.

The first major design choice is not really "Which serializer should we use?" The deeper question is:

> In what representation should data live if we expect to query it repeatedly?

That question spans both memory and storage. It affects:

- how much CPU ingest spends up front
- how much CPU every future query repeats
- whether data can be scanned efficiently
- whether storage behaves like a query substrate or just a durability sink

For a query-first system, serialization and layout are one decision expressed in two phases:

- **Arrow** shapes data in memory
- **Parquet** shapes data on disk

Together they change the path from "message arrived" to "query answered."

## Start With the Actual Incoming Message

Assume each device publish looks roughly like this:

```json
{
  "device_id": "dev-42",
  "ts": "2026-04-06T09:15:00Z",
  "site_id": "plant-7",
  "firmware_version": "1.8.3",
  "temperature": 36.4,
  "humidity": 48.2,
  "battery_mv": 3890,
  "status_code": 0
}
```

At the device edge, this is perfectly reasonable. It is readable, debuggable, and easy to publish.

The question is what happens next.

If the system keeps this as an independent row-shaped payload all the way through, then every analytical query will repeatedly reconstruct the same logical columns from separate messages. That is the hidden cost.

## JSON, Protobuf, and Arrow Are Solving Different Problems

This comparison is slightly uneven, and that is important to say clearly.

- JSON is a text wire format
- Protobuf is a compact binary wire format with schema
- Arrow is a columnar in-memory representation

So this is not a pure apples-to-apples codec comparison. It is a pipeline-design comparison. These are the real choices teams make when deciding what form data should take after arrival.

## Option A: JSON

JSON is attractive because it is simple and universal.

Advantages:

- easy for devices and services to produce
- human-readable
- flexible during early development
- no special tooling required to inspect payloads

Costs:

- field names are repeated in every record
- numbers and timestamps arrive as text that must be parsed
- schema mistakes are discovered late
- every query often reparses structure from scratch

In an ingest-first pipeline, these costs are often tolerated because JSON is just the transport envelope. But in a query-first pipeline, JSON is expensive because it keeps the data in a representation that analytical systems do not naturally want.

The CPU story looks like this:

```text
JSON path

Write time:
  cheap publish
  cheap append

Read time:
  parse text
  validate fields
  cast types
  rebuild columns
  then execute query
```

That is acceptable when queries are rare. It is a poor bargain when the same data is scanned again and again.

## Option B: Protobuf

Protobuf fixes several JSON problems immediately.

Advantages:

- smaller payload size on the wire
- explicit schema
- faster decode than text formats
- better control over compatibility and evolution

That makes Protobuf an excellent transport choice.

But Protobuf does **not** automatically solve the query path.

A Protobuf message is still fundamentally a message. It is compact and typed, but analytics still requires the system to:

- decode each message
- materialize fields row by row
- reshape them into columns if the engine prefers columnar execution

So Protobuf usually reduces wire cost and decode cost, but it does not eliminate the row-to-column conversion step that analytical scans care about.

```text
Protobuf path

Write time:
  efficient binary publish
  schema-aware append

Read time:
  decode messages
  materialize rows
  regroup into columns
  then execute query
```

This is a meaningful improvement over JSON. It is still not the same thing as storing data in a query-native shape.

## Option C: Arrow

Arrow changes the problem because it is already organized around arrays of typed values.

Instead of storing each message as a self-contained object, Arrow stores the batch as columns:

```text
Row-shaped messages

msg1 -> {device_id, ts, site_id, temperature, humidity, battery_mv, status_code}
msg2 -> {device_id, ts, site_id, temperature, humidity, battery_mv, status_code}
msg3 -> {device_id, ts, site_id, temperature, humidity, battery_mv, status_code}

Arrow-style columns

device_id      -> [dev-42, dev-43, dev-44, ...]
ts             -> [t1, t2, t3, ...]
site_id        -> [plant-7, plant-7, plant-9, ...]
temperature    -> [36.4, 35.9, 37.1, ...]
humidity       -> [48.2, 49.1, 44.8, ...]
battery_mv     -> [3890, 3875, 3910, ...]
status_code    -> [0, 0, 2, ...]
```

That change matters because analytical engines naturally want to:

- scan only the columns mentioned in the query
- apply vectorized operations over arrays
- avoid reparsing field structure for every row
- move batches across process boundaries with minimal transformation

Arrow is not just "faster serialization." It is a different execution substrate.

## Arrow in Memory, Parquet on Disk

Arrow and Parquet are closely related, but they solve different stages of the pipeline.

- Arrow is the in-memory batch format
- Parquet is the on-disk columnar file format

That division is useful.

At ingest time, the system can accumulate readings into an Arrow `RecordBatch`. Once the batch is large enough, it can flush that batch into Parquet for durable storage.

The path looks like this:

```text
Incoming messages
  |
  v
Schema validation
  |
  v
Arrow builders / typed arrays
  |
  v
Arrow RecordBatch
  |
  v
Parquet row groups and pages
  |
  v
Object storage
```

This is the point where compute model and storage model stop being independent.

If ingest already produces typed columnar batches, persisting them into a columnar file is natural. If ingest preserves raw row messages, then later systems must do the reshaping work as a separate step.

## Why Columnar Layout Helps

Columnar layout helps because many analytical queries do not need the whole row.

Consider this query:

```sql
SELECT site_id, avg(temperature)
FROM telemetry
WHERE ts >= now() - interval '1 hour'
GROUP BY site_id;
```

This query does not need:

- `firmware_version`
- `humidity`
- `battery_mv`
- `status_code`

With row-oriented storage, the engine often has to touch far more bytes than the query logically needs. With columnar storage, it can read only the relevant columns and skip the rest.

That helps in several ways.

### 1. Projection becomes cheap

Reading two columns from a seven-column dataset is much cheaper when the columns are physically separate.

### 2. Compression gets better

Values in the same column usually resemble each other more than values across a whole row. Timestamps compress well with timestamp encodings. Repeated IDs compress well with dictionary encoding. Numeric telemetry often compresses well because adjacent values are similar.

### 3. Vectorized execution becomes natural

Filtering 10,000 temperatures in a typed array is a better fit for modern analytical engines than stepping through 10,000 independently decoded objects.

### 4. File-level and row-group statistics become useful

Formats like Parquet can store min/max and other metadata per row group. That allows engines to skip chunks of data before reading them fully.

### 5. Query cost becomes more predictable

Instead of paying parsing cost per message, the engine works over typed batches. That typically makes latency more stable under scan-heavy workloads.

## Where Columnar Layout Does Not Help

Columnar storage is not universally better.

It is weaker when the workload looks like this:

- single-row point lookups
- heavy row-level updates or deletes
- extremely low-latency operational reads
- tiny datasets where scan efficiency does not matter
- workloads dominated by opaque binary content

That last category matters more than many designs admit.

Columnar layout helps when fields are independently queryable. It helps far less when the important thing is a large blob that the query engine cannot meaningfully filter, aggregate, or compress structurally.

## The Main Tradeoff: Ingest CPU vs Query CPU

This is the real decision.

With JSON or Protobuf, ingest can be lighter because the system mostly accepts messages in their native form and postpones expensive reshaping.

With Arrow and Parquet, ingest does more up front:

- schema validation happens earlier
- values are appended into typed builders
- batches are formed explicitly
- files are written with a query-oriented layout

That is more CPU work on write.

But it often saves much more CPU later:

- no repeated JSON parsing
- less row-by-row decoding
- less row-to-column conversion
- fewer bytes scanned per query

The trade can be summarized like this:

| Format choice | Ingest CPU | Query CPU | Best fit |
| --- | --- | --- | --- |
| JSON | Low | High | Simplicity, debugging, loose schemas |
| Protobuf | Low to medium | Medium to high | Efficient transport, typed messaging |
| Arrow + Parquet | Medium to high | Low to medium | Repeated analytical scans and fresh query access |

The key asymmetry is still the same:

- an event is ingested once
- the same event may be queried many times

If the query fan-out is high enough, spending extra CPU during ingest is often the cheaper total-system decision.

## A Concrete Example of CPU Shift

Assume a dashboard refreshes every 15 seconds and serves many users. Each refresh needs the last hour of telemetry aggregated by site and firmware.

If the source data is JSON:

- every refresh re-parses text
- every refresh re-casts numeric values
- every refresh rebuilds analytical columns

If the source data is Protobuf:

- text parsing disappears
- but row-by-row decode and reshape still remain

If the source data is already available as Arrow batches or Parquet files:

- the engine reads typed columns directly
- it scans fewer bytes
- it avoids rebuilding the same structure on every refresh

That is why query-first systems tolerate a heavier ingest stage. They are trying to avoid multiplying read-path work by dashboard frequency, user concurrency, and historical scans.

## Large Binary Payloads Change the Answer

The clean Arrow-to-Parquet story starts to break when messages contain large binary payloads such as:

- camera frames
- audio samples
- firmware bundles
- diagnostic dumps
- arbitrary attachments

These blobs behave differently from scalar telemetry fields.

### Why blobs do not benefit much from columnar layout

A query usually does not ask:

- "compute the average of these JPEG bytes"
- "group these firmware binaries by content prefix"

The query usually asks about metadata around the blob:

- device ID
- timestamp
- camera ID
- content type
- object key
- inference label

That means the blob itself is often not the analytical surface. It is just a large payload attached to the event.

### What goes wrong if you force blobs into the same columnar path

Several problems appear:

- files become large and awkward to compact
- writer memory pressure increases
- compression benefits are limited for high-entropy binary content
- queries that only want metadata still have to operate around blob-heavy datasets
- cache efficiency suffers because a small metadata query and a huge binary payload now belong to the same logical object set

Parquet can technically store binary columns, but "possible" is not the same thing as "good system design."

## A Better Hybrid Layout

For blob-heavy workloads, a hybrid design is usually better:

```text
Incoming event
  |
  +--> metadata fields ---------> Arrow / Parquet ---------> query engine
  |
  +--> large binary payload ----> object storage blob key -> fetched only when needed
```

In this model:

- searchable metadata stays in columnar form
- the large payload is stored separately in object storage
- the table stores a pointer, URI, checksum, or content key
- analytical queries stay fast because they scan only metadata
- blob retrieval happens lazily and only for the small subset of rows that need it

This keeps the analytical path clean without giving up access to the raw binary data.

## When a Raw-Bytes Passthrough Mode Makes Sense

Some workloads should not be forced into an Arrow-first ingest path at all.

A raw-bytes passthrough mode makes sense when:

- payloads are mostly opaque binaries
- query demand is low or metadata-only
- ingest throughput matters more than immediate SQL over full payloads
- the product primarily stores and forwards content rather than analyzing it

In those cases, the system can still be query-first for metadata while being passthrough-first for the heavy payload itself.

That is an important distinction. A good architecture does not insist that every byte in the system must receive identical treatment.

## Why Arrow and Parquet Change the Storage Model

At this point the key architectural shift should be clear.

JSON and Protobuf mostly answer:

- how do we represent one message?

Arrow and Parquet answer:

- how do we represent many messages together so scans are cheap?

That is a fundamentally different storage question.

Once data is stored in columnar batches on object storage, the storage system is no longer just keeping durable copies. It is actively shaping query behavior through:

- batch size
- row-group sizing
- partitioning
- sort order
- column statistics
- file count

This is why serialization and storage layout belong in the same discussion. They jointly determine how much work the system repeats on every read.

## The Dominant Tradeoff

The dominant tradeoff is straightforward:

- pay more structure cost once at ingest
- avoid paying decode and scan cost repeatedly at query time

That trade is good when:

- the same data is queried many times
- freshness matters
- analytical scans are a core product feature

That trade is bad when:

- reads are rare
- messages are mostly opaque blobs
- the simplest possible ingest path matters more than query efficiency

## Decision Summary

If the system promise is immediate queryability, then Arrow and Parquet are compelling because they align the ingest representation with the query engine's preferred shape.

That does not make JSON or Protobuf obsolete.

- JSON remains useful at the edge and for debugging
- Protobuf remains excellent for transport efficiency and schema discipline
- Arrow and Parquet become valuable when the system must repeatedly answer analytical questions over fresh data

The question is not "Which format is best in general?"

The right question is:

**Where do you want the system to spend its CPU, and in what shape do you want data to exist when the first real query arrives?**

## Where the Series Goes Next

This part covered the serialization and layout decision:

- JSON is flexible but expensive to query repeatedly
- Protobuf is a better wire format, but still not a query-native layout
- Arrow and Parquet move work earlier so query execution does less reconstruction
- large binary payloads often need a hybrid design rather than a pure columnar path

The next part moves from format choice to durability and correctness: object storage, transaction layers, optimistic concurrency, fault tolerance, and why raw Parquet files alone are not enough for a real system of record.
