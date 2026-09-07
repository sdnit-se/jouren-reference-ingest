//! telemetry-ingest: sensor readings in over HTTP, stored in Postgres, with a
//! hot cache of each sensor's latest readings.

mod api;
mod cache;
mod config;
mod db;
mod readings;
mod telemetry;

use crate::api::AppState;
use crate::cache::Cache;
use crate::config::Config;
use crate::db::Db;
use crate::telemetry::Telemetry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let telemetry = Telemetry::init(&config.service_name, &config.otlp_endpoint)?;

    let cache = Arc::new(Cache::new(
        config.cache_per_sensor,
        config.cache_max_sensors,
    ));
    let db = Arc::new(Db::connect(&config.db));
    let metrics = Telemetry::instruments(Arc::clone(&cache), Arc::clone(&db));

    let warm_cache = Arc::clone(&cache);
    let per_sensor = i64::try_from(config.cache_per_sensor).unwrap_or(i64::MAX);
    let warm = config.warm_on_start;
    let retention_hours = config.retention_hours;
    db.spawn_migrations(move |db| async move {
        if warm {
            warm_up(&db, &warm_cache, per_sensor).await;
        }
        retention_sweep(&db, retention_hours).await;
    });

    let listening = Arc::new(AtomicBool::new(false));
    let app = api::router(AppState {
        cache,
        db,
        metrics,
        listening: Arc::clone(&listening),
    });

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    listening.store(true, Ordering::Relaxed);
    tracing::info!(listen = %config.listen, db_host = config.db.host, "telemetry-ingest listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutting down");
    telemetry.shutdown();
    Ok(())
}

/// Deletes readings past the retention window every five minutes, forever.
/// Owner: the migrations task, after the warm-up.
async fn retention_sweep(db: &Db, hours: u32) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(300));
    loop {
        ticker.tick().await;
        match db.prune_older_than(hours).await {
            Ok(0) => {}
            Ok(rows) => tracing::info!(rows, hours, "pruned old readings"),
            Err(error) => tracing::warn!(error = %error, "retention sweep failed"),
        }
    }
}

/// Fills the cache from the newest rows per sensor. Failure is logged and the
/// service carries on empty; a fatal warm would turn a database outage into a
/// crash loop.
async fn warm_up(db: &Db, cache: &Cache, per_sensor: i64) {
    const CHUNK: usize = 100;
    let result = async {
        let ids = db.sensor_ids().await?;
        for chunk in ids.chunks(CHUNK) {
            let rows = db.latest_for_sensors(chunk, per_sensor).await?;
            cache.insert(rows.iter().map(|(s, r)| (s, r)));
        }
        Ok::<_, db::DbError>(())
    }
    .await;
    match result {
        Ok(()) => {
            let stats = cache.stats();
            tracing::info!(
                sensors = stats.sensors,
                readings = stats.readings,
                "cache warmed from database"
            );
        }
        Err(error) => {
            tracing::error!(error = %error, host = db.host(), "cache warm-up failed; starting empty");
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %error, "ctrl-c handler failed");
        }
    };
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(error = %error, "SIGTERM handler failed"),
        }
    };
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
