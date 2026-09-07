//! Process configuration, read once from the environment. A missing or
//! malformed value is a startup error; there are no fallbacks for anything
//! that names a host, a database or a credential.

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub db: DbConfig,
    pub cache_per_sensor: usize,
    pub cache_max_sensors: usize,
    pub warm_on_start: bool,
    /// Readings older than this are deleted by the periodic sweep; the
    /// table stays bounded on a small volume.
    pub retention_hours: u32,
    pub otlp_endpoint: String,
    pub service_name: String,
}

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub user: String,
    pub password: String,
    pub pool_max: u32,
    pub connect_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0} is required")]
    Missing(&'static str),
    #[error("{name} is not a valid {expected}: {value:?}")]
    Invalid {
        name: &'static str,
        expected: &'static str,
        value: String,
    },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let config = Self {
            listen: parsed("INGEST_LISTEN", "socket address", Some("0.0.0.0:8080"))?,
            db: DbConfig {
                host: required("INGEST_DB_HOST")?,
                port: parsed("INGEST_DB_PORT", "port number", Some("5432"))?,
                name: required("INGEST_DB_NAME")?,
                user: required("INGEST_DB_USER")?,
                password: required("INGEST_DB_PASSWORD")?,
                pool_max: parsed("INGEST_DB_POOL_MAX", "integer", Some("10"))?,
                connect_timeout: Duration::from_secs(parsed(
                    "INGEST_DB_CONNECT_TIMEOUT_SECS",
                    "integer",
                    Some("3"),
                )?),
            },
            cache_per_sensor: parsed("INGEST_CACHE_PER_SENSOR", "integer", Some("2000"))?,
            cache_max_sensors: parsed("INGEST_CACHE_MAX_SENSORS", "integer", Some("128"))?,
            warm_on_start: parsed("INGEST_WARM_ON_START", "boolean", Some("true"))?,
            retention_hours: parsed("INGEST_RETENTION_HOURS", "integer", Some("1"))?,
            otlp_endpoint: optional("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
            service_name: optional("OTEL_SERVICE_NAME", "telemetry-ingest"),
        };
        validate_cache_limits(config.cache_per_sensor, config.cache_max_sensors)?;
        Ok(config)
    }
}

fn validate_cache_limits(per_sensor: usize, max_sensors: usize) -> Result<(), ConfigError> {
    if per_sensor == 0
        || max_sensors == 0
        || max_sensors > 4096
        || per_sensor
            .checked_mul(max_sensors)
            .is_none_or(|total| total > 262_144)
    {
        return Err(ConfigError::Invalid {
            name: "INGEST_CACHE_PER_SENSOR/INGEST_CACHE_MAX_SENSORS",
            expected: "positive cache limits with at most 4096 sensors and 262144 total readings",
            value: format!("{per_sensor}/{max_sensors}"),
        });
    }
    Ok(())
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(ConfigError::Missing(name)),
    }
}

fn optional(name: &'static str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn parsed<T: std::str::FromStr>(
    name: &'static str,
    expected: &'static str,
    default: Option<&str>,
) -> Result<T, ConfigError> {
    let value = match (env::var(name), default) {
        (Ok(v), _) if !v.trim().is_empty() => v,
        (_, Some(d)) => d.to_owned(),
        (_, None) => return Err(ConfigError::Missing(name)),
    };
    value.trim().parse().map_err(|_| ConfigError::Invalid {
        name,
        expected,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_limits_fit_default_and_deployed_memory_budgets() {
        assert!(validate_cache_limits(2000, 128).is_ok());
        assert!(validate_cache_limits(500, 128).is_ok());
        assert!(validate_cache_limits(64, 4096).is_ok());
        for (per_sensor, sensors) in [(0, 128), (500, 0), (1, 4097), (2000, 2000)] {
            assert!(validate_cache_limits(per_sensor, sensors).is_err());
        }
        assert!(validate_cache_limits(usize::MAX, 128).is_err());
    }
}
