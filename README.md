# telemetry-ingest

Sensor readings in over HTTP, stored in Postgres, with a hot cache of each
sensor's latest readings. Every request is traced and measured over OTLP.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/readings` | Batch of 1..1000 readings `{"readings":[{"sensor_id","ts","value","unit?"}]}` → `202 {"accepted":n}` |
| GET | `/readings/latest?sensor=&limit=` | Newest readings from the cache; `404` for an unknown sensor |
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
| `INGEST_CACHE_PER_SENSOR` | `2000` | Readings kept per sensor |
| `INGEST_WARM_ON_START` | `true` | Fill the cache from Postgres at start |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP collector |
| `OTEL_SERVICE_NAME` | `telemetry-ingest` | Resource service name |

Migrations run at start and retry every five seconds until the database
answers; requests get `503` meanwhile.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release --features mixed-case-ids   # the v1.1.0 image
```

Unit tests need no database.
