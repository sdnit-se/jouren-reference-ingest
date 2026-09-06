//! HTTP surface. Handlers parse at the boundary, call the cache or the
//! database, and map failures to status codes; nothing here holds state
//! beyond `AppState`.

use crate::cache::{Cache, CacheStats};
use crate::db::{Db, DbError, PoolStats};
use crate::readings::{Batch, BatchInput, Reading, SensorId, ValidationError};
use crate::telemetry::HttpMetrics;
use axum::extract::{MatchedPath, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use time::OffsetDateTime;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub cache: Arc<Cache>,
    pub db: Arc<Db>,
    pub metrics: HttpMetrics,
    /// Flipped once the listener is bound; `/readyz` reports it and nothing else.
    pub listening: Arc<AtomicBool>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/readings", post(ingest))
        .route("/readings/latest", get(latest))
        .route("/readings/query", get(query))
        .route("/stats", get(stats))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_metrics,
        ))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request| {
                let route = request.extensions().get::<MatchedPath>().map_or_else(
                    || request.uri().path().to_owned(),
                    |p| p.as_str().to_owned(),
                );
                tracing::info_span!(
                    "http.request",
                    http.request.method = %request.method(),
                    http.route = %route,
                    url.path = %request.uri().path(),
                )
            }),
        )
        .with_state(state)
}

/// Records `http.server.request.duration` per matched route.
async fn record_metrics(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |p| p.as_str().to_owned());
    let started = Instant::now();
    let response = next.run(request).await;
    state.metrics.record(
        &method,
        &route,
        response.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    response
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("unknown sensor {0}")]
    UnknownSensor(SensorId),
    #[error("database unavailable")]
    DbUnavailable,
    #[error("{0}")]
    BadQuery(&'static str),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Validation(_) | Self::BadQuery(_) => StatusCode::BAD_REQUEST,
            Self::UnknownSensor(_) => StatusCode::NOT_FOUND,
            Self::DbUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Serialize)]
struct Accepted {
    accepted: usize,
}

async fn ingest(
    State(state): State<AppState>,
    Json(input): Json<BatchInput>,
) -> Result<Response, ApiError> {
    let batch = Batch::parse(input)?;
    if let Err(error) = state.db.insert_batch(&batch).await {
        tracing::error!(error = %error, host = state.db.host(), "database write failed");
        return Err(ApiError::DbUnavailable);
    }
    state.cache.insert(batch.0.iter().map(|(s, r)| (s, r)));
    Ok((
        StatusCode::ACCEPTED,
        Json(Accepted {
            accepted: batch.len(),
        }),
    )
        .into_response())
}

#[derive(Deserialize)]
struct LatestParams {
    sensor: String,
    #[serde(default = "default_latest_limit")]
    limit: usize,
}

fn default_latest_limit() -> usize {
    100
}

#[derive(Serialize)]
struct SensorReadings {
    sensor_id: SensorId,
    readings: Vec<Reading>,
}

async fn latest(
    State(state): State<AppState>,
    Query(params): Query<LatestParams>,
) -> Result<Json<SensorReadings>, ApiError> {
    let sensor = SensorId::parse(&params.sensor)?;
    let readings = state
        .cache
        .latest(&sensor, params.limit.clamp(1, 10_000))
        .ok_or_else(|| ApiError::UnknownSensor(sensor.clone()))?;
    Ok(Json(SensorReadings {
        sensor_id: sensor,
        readings,
    }))
}

#[derive(Deserialize)]
struct QueryParams {
    sensor: String,
    #[serde(with = "time::serde::rfc3339")]
    from: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    to: OffsetDateTime,
    #[serde(default = "default_query_limit")]
    limit: i64,
}

fn default_query_limit() -> i64 {
    1000
}

async fn query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Result<Json<SensorReadings>, ApiError> {
    let sensor = SensorId::parse(&params.sensor)?;
    if params.from > params.to {
        return Err(ApiError::BadQuery("from must not be after to"));
    }
    let limit = params.limit.clamp(1, 100_000);
    match state
        .db
        .query_range(&sensor, params.from, params.to, limit)
        .await
    {
        Ok(readings) => Ok(Json(SensorReadings {
            sensor_id: sensor,
            readings,
        })),
        Err(error) => {
            let error: DbError = error;
            tracing::error!(error = %error, host = state.db.host(), "database query failed");
            Err(ApiError::DbUnavailable)
        }
    }
}

#[derive(Serialize)]
pub struct Stats {
    pub cache: CacheStats,
    pub db: PoolStats,
}

async fn stats(State(state): State<AppState>) -> Json<Stats> {
    Json(Stats {
        cache: state.cache.stats(),
        db: state.db.stats(),
    })
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Response {
    if state.listening.load(Ordering::Relaxed) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "starting").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_serialise_to_the_documented_shape() {
        let stats = Stats {
            cache: CacheStats {
                sensors: 2,
                readings: 30,
                bytes: 1234,
            },
            db: PoolStats {
                pool_size: 4,
                idle: 3,
            },
        };
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "cache": {"sensors": 2, "readings": 30, "bytes": 1234},
                "db": {"pool_size": 4, "idle": 3}
            })
        );
    }

    #[test]
    fn errors_map_to_status_codes() {
        assert_eq!(
            ApiError::Validation(ValidationError::Value)
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::UnknownSensor(SensorId::parse("x").unwrap())
                .into_response()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::DbUnavailable.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
