# Clickhouse Database
Our clickhouse database implementation.

# Overview

### Main Implementation
- Implements the `Database` trait
- `src/clickhouse/db.rs`

### Tables
- The tables enum is generic (a tuple) for any implementation of the `Database` trait, so this is the clickhouse specific usage
- `src/clickhouse/tables.rs`

### Generic Params
- An arbitray sized tuple of parameters
- `src/clickhouse/params.rs`

### Traits for Types
- Clickhouse types need certain implementations for inserts and queries, so these are just helper types so we don't have to rewrite them all
- `src/clickhouse/types.rs`

### Serde Helper
- Serde helper modules
- `src/clickhouse/serde.rs`

### Testing
- Testing infra. Wraps around the `ClickhouseDB` struct and alters queries/tables to use a test database.
- See the additional functions in `src/clickhouse/tables.rs` for the testing functionality


