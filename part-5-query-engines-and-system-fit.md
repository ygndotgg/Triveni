# Part 5: Query Engines and System Fit

*How users actually get data back, and when this architecture is worth building at all.*

By this point in the series, the pipeline looks roughly like this:

- ingest converts messages into typed batches
- Parquet files land in object storage
- a transaction layer defines consistent snapshots
- a hybrid cache keeps the hot working set local

That still leaves the final user-facing question:

> How does a person, dashboard, or API actually ask for data and get it back?

This is where the query engine enters the picture.

And this is also where many architectures become muddled.

Teams often spend a lot of time on serialization, storage, and caching, then treat the query engine as a late plug-in choice. In practice, the engine shapes:

- latency
- concurrency
- deployment model
- operational burden
- extension points
- what “SQL access” really means in production

For a query-first system, the engine is not just a reader. It is the user-facing execution layer.

## The Engine Decision Is Really a System-Boundary Decision

At a high level, the engine choice is less about SQL syntax and more about where you want the query system to live.

Do you want:

- a separate analytical service
- an embedded library inside your own service
- a local in-process database that you can ship with an application

That boundary matters because it changes:

- who owns memory management
- who owns concurrency control
- who owns scaling
- how much custom query behavior you can inject
- how much infrastructure the team must operate

This is why ClickHouse, DataFusion, and DuckDB are useful to compare. They represent three meaningfully different ways to answer the same user need: “I want SQL over recent and historical data.”

## A Quick Mental Model

```text
User / API / Dashboard
  |
  v
SQL surface
  |
  +--> ClickHouse: external analytical service
  |
  +--> DataFusion: embedded Rust query engine
  |
  +--> DuckDB: embedded in-process analytical database
```

The SQL may look similar at the top. The operational model underneath is very different.

## Option A: ClickHouse

ClickHouse is most naturally used as an external analytical system: a dedicated service that stores data itself or queries external data, and serves many concurrent analytical workloads.

That model is attractive because ClickHouse is built for:

- large scans
- high concurrency
- analytical aggregation
- operational dashboards
- production serving workloads

In the context of this series, ClickHouse is the most natural fit when the query layer should behave like a standalone product subsystem rather than a library inside your application.

### Where ClickHouse fits well

ClickHouse is strong when:

- many users or tenants query the system concurrently
- dashboards and APIs need predictable analytical performance
- the team is comfortable operating a dedicated analytical service
- data volumes are large enough that a specialized engine earns its keep
- you want a mature external SQL endpoint

### What you pay for

You are also choosing:

- another service to operate
- separate storage and execution planning concerns
- deployment, scaling, and observability overhead
- some distance between your application logic and query execution internals

This is not necessarily bad. It is often exactly the right trade for a serious multi-user analytics backend. But it is a trade.

### How it interacts with a query-first pipeline

ClickHouse can sit in front of object storage, work with Parquet-oriented data flows, and serve as the query system that turns the lake-style storage layer into a user-facing analytics product.

Architecturally, that means:

- object storage remains the durable history layer
- ClickHouse becomes the execution and serving layer
- caching may exist both inside the engine and in the surrounding system

The result is powerful, but it pushes the overall system toward “operated analytical platform” rather than “self-contained application component.”

## Option B: DataFusion

DataFusion represents a very different model.

It is not primarily a standalone database server. It is a query engine library built around Arrow. That makes it attractive when you want to build the query system into your own Rust service rather than hand the job to an external database.

That gives DataFusion a distinctive strength:

- your application owns the engine boundary directly

### Where DataFusion fits well

DataFusion is strong when:

- you want to embed SQL execution inside a Rust service
- you want tight control over planning, execution, and extension points
- you want to query Arrow and Parquet data with low additional infrastructure
- the workload is real, but not necessarily large enough to justify a separate analytical cluster
- custom functions, custom catalogs, or custom execution rules matter

### What you pay for

DataFusion gives you flexibility, but it does not magically remove systems work.

You still need to decide:

- how requests are served
- how concurrency is managed
- how memory is budgeted
- how caches are integrated
- how distributed execution works, if you need it

That is the central trade.

With ClickHouse, much of the analytical system already exists as a service.

With DataFusion, you get a powerful engine core, but more of the productization is your responsibility.

### Why it matches this series well

For a query-first IoT backend, DataFusion is especially interesting when the design goal is:

- one coherent application
- one service boundary
- one Rust-native control path from ingest to query

It fits naturally with:

- Arrow in memory
- Parquet on object storage
- custom cache policy
- a pluggable SQL layer

If the architecture wants “query engine as library,” DataFusion is the cleanest example in this comparison.

## Option C: DuckDB

DuckDB sits in a third position.

It is an in-process analytical database. That makes it extremely attractive for:

- local analytics
- single-node data exploration
- embedded applications
- developer tooling
- lightweight analytical services

DuckDB is excellent at making analytical SQL very accessible with minimal operational ceremony.

### Where DuckDB fits well

DuckDB is strong when:

- a single process can own the workload comfortably
- local or near-local execution is acceptable
- developer simplicity matters a lot
- the system benefits from a very low-friction SQL layer
- you want excellent support for file-based analytics, especially Parquet

### What you pay for

DuckDB is less naturally positioned as the central multi-tenant analytical service for a large backend. You can absolutely build services around it, but that is a different posture from “ship a dedicated analytical database tier.”

The tradeoffs usually show up in:

- concurrency expectations
- service-shaping work around the engine
- operational patterns for many simultaneous clients
- how much distributed behavior you need outside the process

That does not make DuckDB weak. It makes it opinionated.

It is strongest when the problem can remain close to one process, one machine, or one well-bounded service instance.

## ClickHouse vs DataFusion vs DuckDB

The useful comparison is not raw benchmark mythology. It is system fit.

| Engine | Natural shape | Best at | Main tradeoff |
| --- | --- | --- | --- |
| ClickHouse | external analytical service | high-concurrency analytical serving | more operational overhead |
| DataFusion | embedded Rust query engine | custom application-owned query systems | more engine integration work |
| DuckDB | in-process analytical database | local, embedded, and lightweight analytical execution | less natural as a central shared backend service |

That table is intentionally simple. Real deployments are always more nuanced. But as a design guide, it is the right level of abstraction.

## External Service vs Embedded Engine

This is the architectural fork underneath the engine discussion.

## External service model

```text
clients
  |
  v
application / API
  |
  v
external query service
  |
  v
object storage / table snapshots / cache
```

Advantages:

- clear operational separation
- stronger multi-user serving posture
- independent scaling of query infrastructure
- easier to reason about as a shared analytical endpoint

Costs:

- more moving parts
- more infrastructure ownership
- more network boundaries
- less direct application-level control inside the engine

This is the natural ClickHouse posture.

## Embedded engine model

```text
clients
  |
  v
application / API
  |
  v
embedded query engine
  |
  v
object storage / table snapshots / cache
```

Advantages:

- fewer moving parts
- tighter integration with business logic
- easier custom planning and domain-specific behavior
- simpler small deployments

Costs:

- your service now owns more execution complexity
- memory and cache isolation are harder
- concurrency and workload management become application concerns

This is the natural DataFusion posture, and often the most natural DuckDB posture as well.

## The Same SQL Surface, Different Engine Beneath

One of the best design choices in this kind of system is to **separate the SQL surface from the engine implementation**.

That means users should ideally see:

- one query API
- one SQL dialect contract, or a constrained subset
- one authentication and authorization layer
- one result model

while the backend remains free to choose different engines underneath.

```text
user-facing query API
  |
  +--> parser / validator / auth / limits
  |
  +--> logical query contract
          |
          +--> engine adapter: ClickHouse
          +--> engine adapter: DataFusion
          +--> engine adapter: DuckDB
```

This is valuable for two reasons.

### 1. It protects the product surface from engine churn

You do not want every engine decision to leak directly into the user contract.

If you later move:

- from DataFusion to ClickHouse for scale
- from DuckDB to DataFusion for custom integration
- from one engine to a mixed model for different tenants

the application should not need a full product rewrite.

### 2. It lets the system choose engines by workload

In principle, the same logical SQL layer can route different workloads to different backends:

- lightweight embedded execution for small tenants
- heavier external serving for large tenants
- local exploration with DuckDB-style execution
- application-integrated query endpoints with DataFusion

You would not want to expose all of that complexity to end users. But it is a strong internal design pattern.

## When Each Engine Is the Best Fit

The cleanest way to think about it is this.

Choose ClickHouse when:

- query serving is a dedicated platform concern
- concurrency is high
- dashboards and APIs are central product surfaces
- the team accepts running a specialized analytical service

Choose DataFusion when:

- the application wants to own the query layer directly
- Rust-native integration matters
- custom planning, catalogs, or extensions matter
- infrastructure simplicity matters more than maximum out-of-the-box serving scale

Choose DuckDB when:

- the workload is local, embedded, or single-node enough
- developer simplicity is a major goal
- you want very fast adoption with minimal operational setup
- the system does not need to behave like a large shared analytical service

## When This Whole Architecture Is the Wrong Choice

This is the last and most important filter.

Even if the engine choice is sound, the entire query-first architecture can still be the wrong system.

It is usually the wrong choice when:

- users rarely run analytical queries
- the dominant problem is message delivery, not data retrieval
- the workload needs ultra-low-latency stream processing more than SQL
- data is mostly opaque blobs rather than queryable fields
- the team really needs OLTP semantics, not analytical scans
- the system cannot afford the complexity of object-store tables, cache policy, and query serving

In those cases, the better answer may be:

- a broker-first pipeline
- a simpler operational database
- a direct stream processor
- or even just raw durable files plus offline analysis

Architecture should follow the repeated user operation, not the elegance of the diagram.

## The Strongest Practical Rule

If users need:

- repeated analytical access
- SQL over fresh data
- cheap historical retention
- acceptable but not extreme freshness latency

then the architecture in this series is worth serious consideration.

If users mostly need:

- transport
- replay
- device state mutation
- transactional updates
- opaque payload handling

then this architecture is probably overbuilt.

That is the clearest system-fit rule in the whole series.

## Decision Summary

The query engine is where the architecture becomes an actual product surface.

The main choices are:

- external analytical service for scale and concurrency
- embedded engine for tighter application ownership
- in-process analytical database for simplicity and locality

Mapped onto the engines in this chapter:

- ClickHouse is the strongest default when query serving is a platform in its own right
- DataFusion is the strongest default when query execution should live inside your Rust application
- DuckDB is the strongest default when local or lightweight embedded analytics matters most

The best long-term design is usually not to hard-code the product around one engine's identity. It is to expose one stable query surface and keep the engine boundary replaceable.

## Closing the Series

Across these five parts, the argument has been consistent:

- query-first is a different system design goal from ingest-first
- Arrow and Parquet change where compute happens
- object storage plus a transaction layer changes the durability boundary
- caching makes the read path economically and operationally viable
- the query engine determines how the system becomes usable

The common thread is simple:

**if the product promise is immediate queryability, then representation, storage, cache, and execution all have to be chosen for the read path, not only the write path.**

That is the real meaning of a query-first IoT data pipeline.
