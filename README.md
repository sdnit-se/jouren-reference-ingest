# telemetry-ingest

Sensor readings in over HTTP, stored in Postgres, with a hot cache of each
sensor's latest readings. Every request is traced and measured over OTLP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/readings` | Batch of 1..1000 readings `{"readings":[{"sensor_id","ts","value","unit?"}]}` → `202 {"accepted":n}` |
| GET | `/readings/latest?sensor=&limit=` | Newest cached readings; `404` for an unknown or evicted sensor |
| GET | `/readings/query?sensor=&from=&to=&limit=` | Readings in a time range from Postgres |
| GET | `/stats` | Cache and pool sizes |
| GET | `/healthz` | Always `200` |
| GET | `/readyz` | `200` once listening; never depends on the database |

Database failures answer `503 {"error":"database unavailable"}` and log
`database write failed` / `database query failed` with `error` and `host`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `INGEST_LISTEN` | `0.0.0.0:8080` | Bind address |
| `INGEST_DB_HOST` | required | Postgres host |
| `INGEST_DB_PORT` | `5432` | Postgres port |
| `INGEST_DB_NAME` | required | Database |
| `INGEST_DB_USER` | required | User |
| `INGEST_DB_PASSWORD` | required | Password |
| `INGEST_DB_POOL_MAX` | `10` | Pool size |
| `INGEST_DB_CONNECT_TIMEOUT_SECS` | `3` | Acquire timeout |
| `INGEST_CACHE_PER_SENSOR` | `2000` | Maximum readings per sensor, clamped to 1..65536 and subject to global safety ceilings |
| `INGEST_WARM_ON_START` | `true` | Fill the cache from Postgres at start |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector |
| `OTEL_SERVICE_NAME` | `telemetry-ingest` | Resource service name |

Migrations run at start and retry every five seconds until the database
answers; requests get `503` meanwhile.

### Cache eviction

The cache has fixed safety ceilings of **4,096 sensors** and **65,536 total
readings**, with no environment override. These apply to both live ingestion
and startup warm-up, including when they run concurrently. New sensor IDs
remain valid and database writes still complete before cache insertion.

Sensor-count pressure evicts the entire sensor whose newest timestamp is
oldest. Reading-count pressure removes the globally oldest reading, deleting
empty sensor entries. Equal-timestamp eviction victims across sensors are
unspecified; within a retained sensor, equal timestamps keep arrival order
and measurements are not deduplicated. Out-of-order history therefore cannot
displace strictly newer readings to make room for older ones. Reads do not
refresh eviction priority; sensor timestamps, not wall-clock access time,
determine priority.

`/readings/latest` is explicitly cache-only: histories may be shorter than the
requested limit, and an evicted sensor returns `404` until admitted again.
A fresh live reading can re-admit it, without restoring all its old history.
Use `/readings/query` for persisted readings, including evicted sensors;
eviction does not delete database rows or alter the existing retention sweep.

Deques allocate on demand and shrink excessive spare capacity after eviction.
With validated 64-byte IDs and 16-byte units, these limits budget roughly less
than 16 MiB for retained cache allocations on the reference's 64-bit build,
including spare slots and metadata, rather than multiplying 4,096 sensors by
the default 2,000 readings. This leaves headroom in the 192 MiB container for
warm-up chunks, the database pool, telemetry and requests; it is not a bound
on total process RSS. The cache `bytes` gauge remains a logical occupied-size
estimate, not allocator usage. Verify actual memory with container metrics.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --features mixed-case-ids   # the v1.1.0 image
```

Unit tests need no database.

Before merge, run `cargo fmt --all --check`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`, and
`cargo test --locked --all-features`. Cache regression tests cover fresh-ID
churn, eviction/re-admission, total bounds at the default per-sensor setting,
allocation/accounting invariants, and historical warm-up concurrent with
live writes under global pressure.

After deployment, sustain fresh-ID traffic and restart with populated Postgres.
Check successful writes and range queries for previously evicted sensors,
cache-miss behavior on `/readings/latest`, stable cache cardinality, memory
headroom, no new OOMs, and the positive `jouren:telemetry_healthy` series with
live scrape targets. A restarted process or stale exported gauges alone are
not evidence of recovery.
