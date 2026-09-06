//! Postgres access. The pool connects lazily and migrations retry in the
//! background, so a database that is down at start (or renamed under us)
//! produces 503s and log lines rather than a crash loop.

use crate::config::DbConfig;
use crate::readings::{Batch, Reading, SensorId};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, QueryBuilder};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use time::OffsetDateTime;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("schema is not migrated yet")]
    NotMigrated,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Pool sizes for `/stats` and the pool gauge.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct PoolStats {
    pub pool_size: u32,
    pub idle: usize,
}

/// One row of `readings` as read back for queries and warm-up.
#[derive(Debug, sqlx::FromRow)]
struct Row {
    sensor_id: String,
    ts: OffsetDateTime,
    value: f64,
    unit: Option<String>,
}

impl Row {
    fn into_reading(self) -> Option<(SensorId, Reading)> {
        let sensor = SensorId::parse(&self.sensor_id).ok()?;
        Some((
            sensor,
            Reading {
                ts: self.ts,
                value: self.value,
                unit: self.unit,
            },
        ))
    }
}

/// Owner of the connection pool and the "migrated" flag.
pub struct Db {
    pool: PgPool,
    host: String,
    migrated: AtomicBool,
}

impl Db {
    pub fn connect(config: &DbConfig) -> Self {
        let options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.name)
            .username(&config.user)
            .password(&config.password)
            .application_name("telemetry-ingest");
        let pool = PgPoolOptions::new()
            .max_connections(config.pool_max)
            .acquire_timeout(config.connect_timeout)
            .connect_lazy_with(options);
        Self {
            pool,
            host: config.host.clone(),
            migrated: AtomicBool::new(false),
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn is_migrated(&self) -> bool {
        self.migrated.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            pool_size: self.pool.size(),
            idle: self.pool.num_idle(),
        }
    }

    /// Runs migrations until they succeed, five seconds apart, then invokes
    /// `on_ready` once. Spawned by main; never fails the process.
    pub fn spawn_migrations<F, Fut>(self: &Arc<Self>, on_ready: F)
    where
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let db = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match MIGRATOR.run(&db.pool).await {
                    Ok(()) => {
                        db.migrated.store(true, Ordering::Relaxed);
                        tracing::info!(host = db.host, "database schema ready");
                        on_ready(Arc::clone(&db)).await;
                        return;
                    }
                    Err(error) => {
                        tracing::error!(
                            error = %error,
                            host = db.host,
                            "database migration failed; retrying in 5s"
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    /// One multi-row INSERT for the whole batch.
    pub async fn insert_batch(&self, batch: &Batch) -> Result<(), DbError> {
        if !self.is_migrated() {
            return Err(DbError::NotMigrated);
        }
        let mut builder: QueryBuilder<sqlx::Postgres> =
            QueryBuilder::new("INSERT INTO readings (sensor_id, ts, value, unit) ");
        builder.push_values(&batch.0, |mut row, (sensor, reading)| {
            row.push_bind(sensor.as_str())
                .push_bind(reading.ts)
                .push_bind(reading.value)
                .push_bind(reading.unit.as_deref());
        });
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Readings for one sensor inside `[from, to]`, newest first.
    ///
    /// The span is hand-written because sqlx emits none; it is what shows the
    /// database share of a request in Tempo.
    #[tracing::instrument(
        name = "db.query readings",
        skip_all,
        fields(
            db.system = "postgresql",
            db.operation = "SELECT",
            db.sql.table = "readings",
            sensor = %sensor,
        )
    )]
    pub async fn query_range(
        &self,
        sensor: &SensorId,
        from: OffsetDateTime,
        to: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<Reading>, DbError> {
        if !self.is_migrated() {
            return Err(DbError::NotMigrated);
        }
        let rows = self.range_rows(sensor, from, to, limit).await?;
        Ok(rows
            .into_iter()
            .filter_map(Row::into_reading)
            .map(|(_, reading)| reading)
            .collect())
    }

    #[cfg(not(feature = "mixed-case-ids"))]
    async fn range_rows(
        &self,
        sensor: &SensorId,
        from: OffsetDateTime,
        to: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<Row>, sqlx::Error> {
        sqlx::query_as::<_, Row>(
            "SELECT sensor_id, ts, value, unit FROM readings \
             WHERE sensor_id = $1 AND ts >= $2 AND ts <= $3 \
             ORDER BY ts DESC LIMIT $4",
        )
        .bind(sensor.as_str())
        .bind(from)
        .bind(to)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    // Case-insensitive matching on the id column so mixed-case firmware ids
    // resolve; ordering and the time bound are applied in the service.
    #[cfg(feature = "mixed-case-ids")]
    async fn range_rows(
        &self,
        sensor: &SensorId,
        from: OffsetDateTime,
        to: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<Row>, sqlx::Error> {
        let rows = sqlx::query_as::<_, Row>(
            "SELECT sensor_id, ts, value, unit FROM readings \
             WHERE lower(sensor_id) = lower($1) \
             ORDER BY ts DESC",
        )
        .bind(sensor.as_str())
        .fetch_all(&self.pool)
        .await?;
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(rows
            .into_iter()
            .filter(|row| row.ts >= from && row.ts <= to)
            .take(limit)
            .collect())
    }

    /// The newest `per_sensor` readings of every sensor, oldest first within a
    /// sensor, for warming the cache at start.
    /// Delete readings older than `hours`; returns how many went.
    pub async fn prune_older_than(&self, hours: u32) -> Result<u64, DbError> {
        if !self.is_migrated() {
            return Err(DbError::NotMigrated);
        }
        let result =
            sqlx::query("DELETE FROM readings WHERE ts < now() - make_interval(hours => $1)")
                .bind(i32::try_from(hours).unwrap_or(i32::MAX))
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected())
    }

    /// Every sensor id with at least one reading.
    pub async fn sensor_ids(&self) -> Result<Vec<String>, DbError> {
        if !self.is_migrated() {
            return Err(DbError::NotMigrated);
        }
        let ids: Vec<(String,)> = sqlx::query_as("SELECT DISTINCT sensor_id FROM readings")
            .fetch_all(&self.pool)
            .await?;
        Ok(ids.into_iter().map(|(id,)| id).collect())
    }

    /// The newest `per_sensor` readings of each of `sensor_ids`, oldest first
    /// per sensor. Called per chunk of sensors so the warm-up never holds more
    /// than one chunk in memory; each sensor is served by the `(sensor_id, ts)`
    /// index.
    pub async fn latest_for_sensors(
        &self,
        sensor_ids: &[String],
        per_sensor: i64,
    ) -> Result<Vec<(SensorId, Reading)>, DbError> {
        if !self.is_migrated() {
            return Err(DbError::NotMigrated);
        }
        let rows = sqlx::query_as::<_, Row>(
            "SELECT r.sensor_id, r.ts, r.value, r.unit \
             FROM unnest($1::text[]) AS s(id) \
             CROSS JOIN LATERAL ( \
                SELECT sensor_id, ts, value, unit FROM readings \
                WHERE sensor_id = s.id ORDER BY ts DESC LIMIT $2 \
             ) r \
             ORDER BY r.sensor_id, r.ts ASC",
        )
        .bind(sensor_ids)
        .bind(per_sensor)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().filter_map(Row::into_reading).collect())
    }
}
