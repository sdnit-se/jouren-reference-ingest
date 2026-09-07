# telemetry-ingest

Sensor readings in over HTTP, stored in Postgres, with a hot cache of each
sensor's latest readings. Every request is traced and measured over OTLP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/readings` | Batch of 1..1000 readings `{"readings":[{"sensor_id","ts","value","unit?"}]}` → `202 {"accepted":n}` |
| GET | `/readings/latest?sensor=&limit=` | Newest cached readings; `404` for an unknown, evicted or non-admitted sensor |
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
| `INGEST_CACHE_PER_SENSOR` | `2000` | Readings kept per cached sensor; positive |
| `INGEST_CACHE_MAX_SENSORS` | `128` | Maximum cached sensors; 1..4096 |
| `INGEST_WARM_ON_START` | `true` | Fill the cache from Postgres at start |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector |
| `OTEL_SERVICE_NAME` | `telemetry-ingest` | Resource service name |

Migrations run at start and retry every five seconds until the database
answers; requests get `503` meanwhile.

### Cache eviction and memory budget

The product of the two cache limits must not exceed 262,144 readings;
invalid, zero or overflowing limits fail startup. Defaults retain at most
128 sensors and 256,000 readings. With the deployed per-sensor limit of 500,
that becomes 64,000 readings. The default cache storage estimate is below
32 MiB on the deployed 64-bit target, including deque capacity and an
allowance for allocations of validated units. This leaves headroom within
192 MiB for requests, SQL results, telemetry and the runtime; it is not a
bound on total process RSS. The cache bytes gauge measures occupied payload,
not reserved capacity or allocator overhead.

When full, the cache evicts the entire sensor whose newest measurement is
oldest, only if the incoming sensor has a newer measurement. Equal newest
timestamps are resolved deterministically by retaining the lexicographically
larger sensor id. This is measurement-time admission, not access-time LRU:
reads do not refresh priority, delayed historical arrivals cannot displace
newer live sensors, and future-dated measurements can retain priority until
other sensors catch up. Admission scans at most the configured sensor bound.
Within each retained sensor, readings remain timestamp-sorted and capped;
equal timestamps retain arrival order without deduplication.

Eviction affects only the hot cache. POST still persists the entire validated
batch before attempting cache insertion, even if none of it is admitted.
`/readings/latest` remains cache-only: an evicted or non-admitted sensor gets
404, not an implicit database fallback. A later qualifying reading can
readmit it, but its cached history may then be partial. Use `/readings/query`
for persisted data, which remains available until the existing database
retention policy expires it; eviction does not issue database deletes.

Startup warm-up uses the same admission and timestamp-merge path as live
writes. Chunks target at most 8192 readings and 100 sensors (one sensor when
its per-sensor limit exceeds 8192), rather than allocating 100 full sensors
regardless of configuration. The existing sensor-id discovery still reads
all distinct IDs; extremely large retained databases require separate
measurement of that transient memory and query load.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --features mixed-case-ids   # the v1.1.0 image
```

Unit tests need no database. The explicitly ignored persistence contract
requires an isolated Postgres database with `INGEST_DB_HOST`, `INGEST_DB_NAME`,
`INGEST_DB_USER` and `INGEST_DB_PASSWORD` set; it runs migrations and leaves
uniquely named test readings in that database. Do not run it against production.

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --all-features persisted_readings_remain_queryable_after_cache_eviction -- --ignored
```

The cache regressions exercise sensor churn, exact eviction accounting,
default-budget occupancy with maximum-length units, deterministic timestamp
ties, and historical warm-up concurrent with newer live writes. Without the
sensor bound, cardinality and eviction assertions fail; the persistence
contract verifies database range queries after a cache miss caused by eviction.

Before release, run these checks and replay fresh-ID traffic in staging at
192 MiB, with both the deployed 500 and default 2000 per-sensor settings.
Restart against populated storage while live writes continue. Verify cached
sensors/readings plateau at their limits, RSS retains headroom, old sensors
return the documented latest miss but remain range-queryable, and successful
traffic and query latency remain healthy. Require live scrape targets,
available replicas, no new OOMs and sustained `jouren:telemetry_healthy = 1`;
a stale cache gauge or a listening probe alone is not recovery evidence.
