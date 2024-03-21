# Database Interfaces

Generic database interface with custom DBMS implementation (currently only clickhouse).

## Clickhouse Implementation
The `ClickhouseClient` takes a generic parameter implementing the `ClickhouseDBMS` trait which for all intents and prorpuses is a set of unit struct implementing the `ClickhouseTable` trait that can be used with this client.

When creating sets of tables, first call the `clickhouse_dbms!` macro, then define each of the tables with the `remote_clickhouse_table!` proc-macro