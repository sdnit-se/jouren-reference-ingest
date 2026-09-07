# telemetry-ingest

Sensor readings in over HTTP, stored in Postgres, with a hot cache of each
sensor's latest readings. Every request is traced and measured over OTLP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/readings` | Batch of 1..1000 readings `{"readings":[{"sensor_id","ts","value","unit?"}]}` → `202 {"accepted":n}` |
| GET | `/readings/latest?sensor=&limit=` | Newest readings from the cache; `404` for an unknown, evicted, or non-admitted sensor |
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
| `INGEST_CACHE_PER_SENSOR` | `2000` | Readings kept per cached sensor, clamped to 1..100000; also determines the total sensor bound below |
| `INGEST_WARM_ON_START` | `true` | Fill the bounded cache from Postgres at start |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector |
| `OTEL_SERVICE_NAME` | `telemetry-ingest` | Resource service name |

Migrations run at start and retry every five seconds until the database
answers; requests get `503` meanwhile.

### Cache eviction and memory budget

The cache reserves against full per-sensor histories: at most
`min(4096, floor(100000 / effective_per_sensor))` sensors are admitted.
These fixed safety bounds require no new environment variable. They allow
200 sensors at the deployed per-sensor setting of 500, or 50 sensors at the
unmodified default of 2000. Increasing per-sensor history therefore reduces
cardinality rather than increasing the total reading budget. The conservative
budget trades cache hit rate for headroom under the reference 192 MiB limit.

At capacity, a new sensor replaces the sensor whose newest cached timestamp
is oldest, provided the incoming reading is at least as recent. Ties between
victims use sensor ID order. Older historical readings are not admitted in
place of newer data. Existing sensors still merge by timestamp and keep their
newest readings; historical warm-up uses the same lock and policy as live writes.
Measurement recency is intentional, not access-based LRU; future-dated readings
can retain cache residency longer. Every batch is persisted before cache
admission, and eviction issues no database deletion.

`/readings/latest` remains cache-only: evicted or non-admitted sensors return
404, and re-admitted sensors may have partial histories until more readings
arrive. Use `/readings/query` for persisted history, including after eviction,
subject to the existing database retention window. Cache eviction does not
alter retention, database timeouts, validation, or accepted-reading counts.

With 100,000 retained readings, deque slack, short unit strings and map overhead
should keep cache allocations around 20 MiB on the reference 64-bit target;
this is a sizing estimate, not a process RSS guarantee. The byte gauge counts
logical readings and IDs, not spare capacity or allocator overhead. Warm-up
fetches at most 10,000 readings per batch, or one effective per-sensor history
when larger (at most 100,000). Its initial sensor-ID list is still materialized
from Postgres; this change bounds the cache and reading batches, not that list.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --features mixed-case-ids   # the v1.1.0 image
```

Unit tests need no database.

Before rollout, run `cargo fmt --all --check`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`, and
`cargo test --locked --all-features`. Replay at least 32,000 fresh sensor IDs
under the 192 MiB limit at both 500 and 2000 readings per sensor, filling
retained histories and restarting with historical warm-up concurrent with
live writes. Verify bounded cardinality, memory headroom, successful writes,
and latest timestamp ordering. Query a persisted reading before and after
its sensor is evicted: `/readings/latest` may return 404, but the same
`/readings/query` must still return it within the retention window. After
release, require live successful traffic and `jouren:telemetry_healthy`, not
just a running pod or stale collector metrics.
