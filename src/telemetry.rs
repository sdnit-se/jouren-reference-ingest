//! Logs, traces and metrics over OTLP/HTTP, plus JSON logs on stdout. One
//! `Telemetry` value owns the three providers and flushes them at shutdown.

use crate::cache::Cache;
use crate::db::Db;
use opentelemetry::metrics::Histogram;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

pub struct Telemetry {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
    meter: SdkMeterProvider,
}

/// The request-duration histogram handed to the HTTP middleware.
#[derive(Clone)]
pub struct HttpMetrics {
    duration: Histogram<f64>,
}

impl HttpMetrics {
    pub fn record(&self, method: &str, route: &str, status: u16, seconds: f64) {
        self.duration.record(
            seconds,
            &[
                KeyValue::new("http.request.method", method.to_owned()),
                KeyValue::new("http.route", route.to_owned()),
                KeyValue::new("http.response.status_code", i64::from(status)),
            ],
        );
    }
}

impl Telemetry {
    /// Installs the global subscriber and providers. Call once, before any
    /// log line that matters.
    pub fn init(service_name: &str, otlp_endpoint: &str) -> anyhow::Result<Self> {
        let endpoint = otlp_endpoint.trim_end_matches('/');
        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .build();

        let span_exporter = SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/traces"))
            .build()?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource.clone())
            .build();

        let log_exporter = LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/logs"))
            .build()?;
        let logger_provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource.clone())
            .build();

        let metric_exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(format!("{endpoint}/v1/metrics"))
            .build()?;
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(metric_exporter)
                    .with_interval(Duration::from_secs(10))
                    .build(),
            )
            .with_resource(resource)
            .build();

        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());

        let filter =
            || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let tracer =
            opentelemetry::trace::TracerProvider::tracer(&tracer_provider, service_name.to_owned());
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_filter(filter()),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(filter()),
            )
            .with(
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
                    &logger_provider,
                )
                .with_filter(filter()),
            )
            .try_init()?;

        Ok(Self {
            tracer: tracer_provider,
            logger: logger_provider,
            meter: meter_provider,
        })
    }

    /// Creates the request histogram and registers the cache and pool gauges,
    /// which read live values whenever the reader collects.
    pub fn instruments(cache: Arc<Cache>, db: Arc<Db>) -> HttpMetrics {
        let meter = global::meter("telemetry-ingest");
        let duration = meter
            .f64_histogram("http.server.request.duration")
            .with_unit("s")
            .with_description("Duration of HTTP server requests")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ])
            .build();

        let sensors = Arc::clone(&cache);
        meter
            .u64_observable_gauge("ingest.cache.sensors")
            .with_description("Distinct sensors held in the latest-readings cache")
            .with_callback(move |observer| observer.observe(sensors.stats().sensors as u64, &[]))
            .build();
        let readings = Arc::clone(&cache);
        meter
            .u64_observable_gauge("ingest.cache.readings")
            .with_description("Readings held in the latest-readings cache")
            .with_callback(move |observer| observer.observe(readings.stats().readings as u64, &[]))
            .build();
        let bytes = cache;
        meter
            .u64_observable_gauge("ingest.cache.bytes")
            .with_description("Estimated bytes held by the latest-readings cache")
            .with_callback(move |observer| observer.observe(bytes.stats().bytes as u64, &[]))
            .build();
        meter
            .u64_observable_gauge("ingest.db.pool.connections")
            .with_description("Database pool connections by state")
            .with_callback(move |observer| {
                let stats = db.stats();
                let idle = stats.idle as u64;
                let used = u64::from(stats.pool_size).saturating_sub(idle);
                observer.observe(idle, &[KeyValue::new("state", "idle")]);
                observer.observe(used, &[KeyValue::new("state", "used")]);
            })
            .build();

        HttpMetrics { duration }
    }

    /// Flushes and stops the exporters; errors are logged, not returned, as
    /// nothing can be done about them at exit.
    pub fn shutdown(self) {
        if let Err(error) = self.tracer.shutdown() {
            eprintln!("tracer shutdown: {error}");
        }
        if let Err(error) = self.meter.shutdown() {
            eprintln!("meter shutdown: {error}");
        }
        if let Err(error) = self.logger.shutdown() {
            eprintln!("logger shutdown: {error}");
        }
    }
}
