# Triveni

`Triveni` is a Rust prototype for a query-first IoT data pipeline. Instead of treating telemetry as raw messages that only become useful after downstream ETL, this crate converts telemetry into typed batches early, persists it as partitioned Delta/Parquet data, and exposes a SQL query path over consistent snapshots.

The implementation follows the design described across the repository's Part 1 to Part 5 notes:

- query-first architecture over ingest-first architecture
- Arrow as the in-memory batch format
- Parquet and Delta Lake for durable, queryable storage
- hybrid memory/disk caching for hot data
- DataFusion as the embedded SQL engine

## What This Crate Implements

- Telemetry message modeling with an `event_date` partition key
- Batch formation for incoming messages
- Arrow `RecordBatch` construction from telemetry batches
- Delta Lake table creation and append writes
- Partitioned storage by `event_date`
- Delta snapshot inspection and active file discovery
- Table compaction for many small files
- Hybrid cache behavior for metadata, recent data, and historical data
- Snapshot-aware SQL execution through DataFusion

## Architecture

```text
Telemetry messages
  -> MessageBatcher
  -> Arrow RecordBatch
  -> Delta Lake / Parquet files
  -> Local snapshot metadata + active file view
  -> Hybrid cache
  -> DataFusion SQL queries
```

The main idea is simple: spend some structure up front during ingest so repeated reads do less work later.

## Repository Context

This crate sits inside a larger repository that contains:

- a local copy of `part-1-core-thesis.md` to `part-5-query-engines-and-system-fit.md`
- a longer blog draft on the full pipeline design
- UI/documentation experiments around the same architecture

If you want the design rationale behind this crate, start with the five design-note markdown files now included directly in this directory.

## Project Layout

```text
Triveni/
├── README.md
├── part-1-core-thesis.md
├── part-2-serialization-and-storage-layout.md
├── part-3-persistence-consistency-and-cost.md
├── part-4-caching-and-hot-data.md
├── part-5-query-engines-and-system-fit.md
├── src/
│   ├── batcher.rs         # in-memory batching
│   ├── cache.rs           # hybrid memory/disk cache
│   ├── message.rs         # telemetry model
│   ├── parquet_man.rs     # parquet helpers
│   ├── persistence.rs     # Delta Lake persistence and compaction
│   ├── pipeline.rs        # schema, batch building, sample generation
│   ├── query.rs           # DataFusion query service
│   └── main.rs            # local demo entrypoint
└── tests/
    ├── delta_persistence.rs
    ├── hybrid_cache.rs
    ├── partition_write.rs
    └── query_service.rs
```

## Data Model

The current telemetry schema is:

- `device_id: Utf8`
- `ts_ms: Int64`
- `temperature: Float64`
- `humidity: Float64`
- `event_date: Utf8`

`event_date` is derived from the timestamp and used as the default Delta partition column.

## Getting Started

### Requirements

- Rust toolchain with Cargo

### Install dependencies

This crate uses:

- `arrow`
- `parquet`
- `deltalake`
- `tokio`
- `chrono`

They are declared in `Cargo.toml` and resolved automatically by Cargo.

### Run the demo

From the `Triveni/` directory:

```bash
cargo run
```

The demo in `src/main.rs`:

1. generates synthetic telemetry for 7 days
2. batches messages in groups of 100
3. builds Arrow record batches
4. writes them into a local Delta table at `delta-table/`

## Tests

The test suite covers:

- partitioned Parquet writes across multiple days
- Delta snapshot metadata after writes
- compaction without row loss
- hybrid cache promotion and invalidation behavior
- read-only SQL execution over versioned snapshots

Run:

```bash
cargo test
```

## Current Status

The implemented tests and modules show the intended architecture clearly, but the crate is not fully build-clean at the moment. Verification in this workspace failed because `src/persistencee.rs` is unfinished and is still exported from `src/lib.rs`.

That means the README describes the intended and mostly implemented crate structure accurately, but the repository still needs a small cleanup before `cargo test` passes end to end.

## Why This Project Exists

Most IoT systems optimize for accepting data quickly. This project explores a different goal: making telemetry queryable quickly.

That changes the shape of the system:

- parse into typed columns earlier
- write to columnar storage earlier
- treat snapshot metadata as part of correctness
- keep hot data local with cache tiers
- execute SQL close to the storage layout

## Roadmap

Natural next steps for this crate are:

- finish or remove the incomplete `persistencee.rs` module
- add a real MQTT ingest path instead of synthetic data generation
- support object storage backends beyond local paths
- expose the query layer through an HTTP or gRPC API
- add richer schemas, filtering, and aggregate examples

## Related Design Notes

- `part-1-core-thesis.md`
- `part-2-serialization-and-storage-layout.md`
- `part-3-persistence-consistency-and-cost.md`
- `part-4-caching-and-hot-data.md`
- `part-5-query-engines-and-system-fit.md`

## License

No license file is currently present in this crate directory. Add one before publishing publicly on GitHub.
