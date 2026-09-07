# telemetry-ingest

Sensor readings in over HTTP, stored in Postgres, with a hot cache of each
sensor's latest readings. Every request is traced and measured over OTLP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/readings` | Batch of 1..1000 readings `{"readings":[{"sensor_id","ts","value","unit?"}]}` → `202 {"accepted":n}` |
| GET | `/readings/latest?sensor=&limit=` | Newest readings from the cache; `404` for an unknown, evicted or unadmitted sensor |
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
| `INGEST_CACHE_PER_SENSOR` | `2000` | Readings kept per resident sensor; cache clamps to 1..131072 |
| `INGEST_WARM_ON_START` | `true` | Fill the cache from Postgres at start |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector |
| `OTEL_SERVICE_NAME` | `telemetry-ingest` | Resource service name |

Migrations run at start and retry every five seconds until the database
answers; requests get `503` meanwhile.

### Cache residency and memory

The cache has fixed safeguards, not additional environment variables: 131,072
reading slots across resident sensors and at most 2,048 resident sensors.
For effective per-sensor limit `P`, the sensor limit is
`min(2048, floor(131072 / P))`. This reserves room for full per-sensor buffers,
including sensors which currently have only one reading. The deployed setting
of 500 admits 262 sensors; the default of 2,000 admits 65. The cache byte gauge
still measures populated reading footprints and ID lengths, not reserved
capacity, allocator overhead or total process memory.

At capacity, a new sensor replaces the resident whose newest reading has the
oldest timestamp, but only if the incoming timestamp is strictly newer. Ties
keep incumbents; reads do not refresh residency. A bounded timestamp index
avoids scanning all sensors on each admission. Historical warm-up and live
writes use this same policy and timestamp-sorted per-sensor merge: older
historical data cannot displace newer resident live data. Residency follows
measurement timestamps, not arrival time, so delayed sensors have lower cache
priority. Eviction removes the entire sensor buffer and its accounting.

This conservative budget keeps retained deque storage, validated units and
metadata roughly below 20 MiB on the deployed 64-bit target, allowing substantial
headroom within 192 MiB for requests, the existing 100-sensor warm-up chunks,
the database client and telemetry. It also covers the default per-sensor setting;
arbitrarily increasing warm-up query sizes is not made safe by a cache bound.
The estimate is not a process RSS guarantee and must be checked under load.

Every accepted batch is persisted before cache admission. Cache eviction does
not delete database rows or change database retention. `/readings/latest`
remains cache-only and returns `404` after eviction or rejected admission;
use `/readings/query` to retrieve persisted readings within the existing
retention window. A later sufficiently recent write or warm-up row can readmit
a sensor, but its cache contains only readings admitted since then, not a
complete database history. This trades cache hit rate and cached history for
bounded memory without adding database load to the latest endpoint.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --features mixed-case-ids   # the v1.1.0 image
```

Unit tests need no database.

Before merging, also run `cargo fmt --check`. Churn, full-buffer, accounting
and concurrent historical/live-write contracts run in the normal test suite.
The persistence contract is opt-in: point `INGEST_DB_HOST`, `INGEST_DB_NAME`,
`INGEST_DB_USER` and `INGEST_DB_PASSWORD` at a disposable Postgres database
(with optional port configuration), then run:

```sh
cargo test persisted_readings_remain_queryable_after_cache_eviction -- --ignored
```

That test runs migrations and leaves uniquely named test readings in the
disposable database. It checks persist-before-cache ordering, actual eviction
and retrieval through the same database range-query method as the HTTP API.
After deployment, exercise sustained fresh sensor IDs, full per-sensor buffers
and restart warm-up concurrent with live writes. Verify historical queries for
evicted sensors, the documented latest cache misses, stable cache cardinality,
working-set headroom, no new OOMs, available replicas, successful traffic and
low query latency. Confirm `jouren:telemetry_healthy` with live scrape targets;
absence of an OOM alert alone is not proof of recovery.
