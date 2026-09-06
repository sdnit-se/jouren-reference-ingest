-- Raw readings as received. Queries are always per sensor over a time range,
-- so the composite index carries every read path.
CREATE TABLE readings (
    id          BIGSERIAL PRIMARY KEY,
    sensor_id   TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX readings_sensor_ts ON readings (sensor_id, ts DESC);
