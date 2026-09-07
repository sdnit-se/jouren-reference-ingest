//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Both sensor residency and readings are bounded.

use crate::readings::{Reading, SensorId};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use time::OffsetDateTime;

// Reserve room for requests, warm-up and telemetry in the 192 MiB deployment.
// Deriving the sensor limit from per_sensor bounds allocated deque capacity,
// not just the populated-reading byte gauge. Deques may grow geometrically.
const MAX_READINGS: usize = 131_072;
const MAX_SENSORS: usize = 2_048;

/// Size figures exposed on `/stats` and as gauges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CacheStats {
    pub sensors: usize,
    pub readings: usize,
    pub bytes: usize,
}

/// Owner of the cache map. The mutex guards short, non-async sections only;
/// callers never hold it across an `.await`.
pub struct Cache {
    per_sensor: usize,
    max_sensors: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_sensor: HashMap<String, VecDeque<Reading>>,
    // Exactly one index entry per resident sensor, not one per insertion.
    by_newest: BTreeSet<(OffsetDateTime, String)>,
    readings: usize,
    bytes: usize,
}

impl Inner {
    fn admit(&mut self, sensor: &SensorId, reading: &Reading, max_sensors: usize) -> bool {
        if self.by_sensor.contains_key(sensor.as_str()) {
            return true;
        }
        if self.by_sensor.len() >= max_sensors {
            let Some((oldest, victim)) = self.by_newest.first().cloned() else {
                return false;
            };
            // Historical warm-up must not replace newer live data. Ties keep
            // the incumbent; reads do not change timestamp-based residency.
            if reading.ts <= oldest {
                return false;
            }
            self.by_newest.pop_first();
            if let Some(evicted) = self.by_sensor.remove(&victim) {
                self.readings -= evicted.len();
                self.bytes -= victim.len() + evicted.iter().map(Reading::byte_len).sum::<usize>();
            }
        }
        self.bytes += sensor.byte_len();
        self.by_sensor
            .insert(sensor.as_str().to_owned(), VecDeque::new());
        true
    }
}

impl Cache {
    pub fn new(per_sensor: usize) -> Self {
        let per_sensor = per_sensor.clamp(1, MAX_READINGS);
        Self {
            per_sensor,
            max_sensors: (MAX_READINGS / per_sensor).min(MAX_SENSORS),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Merge by timestamp and retain the newest readings, including during warm-up.
    /// Equal timestamps retain arrival order; measurements are not deduplicated.
    /// At capacity, only sensors newer than the oldest resident are admitted.
    /// Admission and eviction affect this cache only, never persisted readings.
    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.admit(sensor, reading, self.max_sensors) {
                continue;
            }
            let Some(entry) = inner.by_sensor.get_mut(sensor.as_str()) else {
                continue;
            };
            let previous = entry.back().map(|r| r.ts);
            let position = entry.partition_point(|existing| existing.ts <= reading.ts);
            entry.insert(position, reading.clone());
            let evicted = if entry.len() > self.per_sensor {
                entry.pop_front()
            } else {
                None
            };
            let newest = entry.back().map(|r| r.ts);
            if let Some(ts) = previous {
                inner.by_newest.remove(&(ts, sensor.as_str().to_owned()));
            }
            if let Some(ts) = newest {
                inner.by_newest.insert((ts, sensor.as_str().to_owned()));
            }
            // Subtract from the total: an insertion delta cannot represent shrinkage.
            inner.bytes -= evicted.as_ref().map_or(0, Reading::byte_len);
            inner.bytes += reading.byte_len();
            inner.readings += usize::from(evicted.is_none());
        }
    }

    /// Newest-first slice, or `None` for an unknown, evicted or unadmitted sensor.
    /// Callers can query Postgres for persisted readings regardless of residency.
    pub fn latest(&self, sensor: &SensorId, limit: usize) -> Option<Vec<Reading>> {
        let inner = self.lock();
        let entry = inner.by_sensor.get(sensor.as_str())?;
        Some(entry.iter().rev().take(limit).cloned().collect())
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.lock();
        CacheStats {
            sensors: inner.by_sensor.len(),
            readings: inner.readings,
            bytes: inner.bytes,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock means another thread panicked mid-update; the map is
        // still structurally valid, and serving slightly off counts beats
        // taking the service down.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    fn reading(n: i32) -> Reading {
        Reading {
            ts: OffsetDateTime::from_unix_timestamp(i64::from(n)).unwrap(),
            value: f64::from(n),
            unit: None,
        }
    }

    #[test]
    fn keeps_only_the_newest_per_sensor() {
        let cache = Cache::new(3);
        let sensor = SensorId::parse("s-1").unwrap();
        let readings: Vec<_> = (1..=5).map(reading).collect();
        cache.insert(readings.iter().map(|r| (&sensor, r)));

        let latest = cache.latest(&sensor, 10).unwrap();
        assert_eq!(
            latest.iter().map(|r| r.value).collect::<Vec<_>>(),
            vec![5.0, 4.0, 3.0]
        );
        let stats = cache.stats();
        assert_eq!(stats.sensors, 1);
        assert_eq!(stats.readings, 3);
        assert_eq!(stats.bytes, sensor.byte_len() + 3 * reading(1).byte_len());
    }

    #[test]
    fn replacing_a_larger_reading_reduces_bytes() {
        let cache = Cache::new(1);
        let sensor = SensorId::parse("sensor").unwrap();
        let mut large = reading(1);
        large.unit = Some("long-unit".to_owned());
        cache.insert([(&sensor, &large)]);
        let before = cache.stats().bytes;
        let small = reading(2);
        cache.insert([(&sensor, &small)]);
        assert_eq!(
            cache.stats().bytes,
            before - large.byte_len() + small.byte_len()
        );
        assert_eq!(cache.stats().readings, 1);
    }

    #[test]
    fn alternating_sizes_do_not_accumulate_fictitious_bytes() {
        let cache = Cache::new(1);
        let sensor = SensorId::parse("sensor").unwrap();
        for n in 1..20 {
            let mut r = reading(n);
            if n % 2 == 0 {
                r.unit = Some("long-unit".to_owned());
            }
            cache.insert([(&sensor, &r)]);
            assert_eq!(cache.stats().bytes, sensor.byte_len() + r.byte_len());
            assert_eq!(cache.stats().readings, 1);
        }
    }

    #[test]
    fn historical_warm_up_cannot_evict_a_newer_live_reading() {
        let cache = Cache::new(3);
        let sensor = SensorId::parse("sensor").unwrap();
        cache.insert([(&sensor, &reading(100))]);
        let historical: Vec<_> = (1..100).map(reading).collect();
        cache.insert(historical.iter().map(|r| (&sensor, r)));
        let latest = cache.latest(&sensor, 3).unwrap();
        assert_eq!(
            latest.iter().map(|r| r.value).collect::<Vec<_>>(),
            [100.0, 99.0, 98.0]
        );
        assert_eq!(
            cache.stats().bytes,
            sensor.byte_len() + 3 * reading(1).byte_len()
        );
    }

    #[test]
    fn out_of_order_live_batches_keep_the_newest_timestamps() {
        let cache = Cache::new(3);
        let sensor = SensorId::parse("sensor").unwrap();
        for n in [5, 1, 4, 2, 3] {
            cache.insert([(&sensor, &reading(n))]);
        }
        let latest = cache.latest(&sensor, 3).unwrap();
        assert_eq!(
            latest.iter().map(|r| r.value).collect::<Vec<_>>(),
            [5.0, 4.0, 3.0]
        );
    }

    #[test]
    fn unknown_sensor_is_none_and_limit_applies() {
        let cache = Cache::new(10);
        let a = SensorId::parse("a").unwrap();
        assert!(cache.latest(&a, 5).is_none());
        let readings: Vec<_> = (1..=4).map(reading).collect();
        cache.insert(readings.iter().map(|r| (&a, r)));
        assert_eq!(cache.latest(&a, 2).unwrap().len(), 2);
    }

    #[test]
    fn stats_count_every_sensor() {
        let cache = Cache::new(2);
        let sensors: Vec<_> = (0..50)
            .map(|i| SensorId::parse(&format!("s-{i}")).unwrap())
            .collect();
        let r = reading(1);
        cache.insert(sensors.iter().map(|s| (s, &r)));
        let stats = cache.stats();
        assert_eq!(stats.sensors, 50);
        assert_eq!(stats.readings, 50);
    }

    #[test]
    fn fresh_sensor_churn_is_bounded_at_deployed_and_default_settings() {
        for per_sensor in [500, 2000] {
            let cache = Cache::new(per_sensor);
            for n in 1..=32_000 {
                let sensor = SensorId::parse(&format!("s-{n}")).unwrap();
                cache.insert([(&sensor, &reading(n))]);
                assert!(cache.stats().sensors <= 131_072 / per_sensor);
                assert!(cache.stats().readings <= 131_072);
            }
            assert_eq!(cache.stats().sensors, 131_072 / per_sensor);
            assert_eq!(cache.lock().by_newest.len(), cache.stats().sensors);
        }
    }

    #[test]
    fn full_sensor_buffers_fit_the_total_budget() {
        for per_sensor in [500, 2000] {
            let cache = Cache::new(per_sensor);
            let mut r = reading(1);
            r.unit = Some("u".repeat(Reading::MAX_UNIT_LEN));
            for n in 0..=(131_072 / per_sensor) {
                let sensor = SensorId::parse(&format!("s-{n}")).unwrap();
                cache.insert(std::iter::repeat_n((&sensor, &r), per_sensor + 1));
            }
            let stats = cache.stats();
            assert!(stats.readings <= 131_072);
            let inner = cache.lock();
            let slots: usize = inner.by_sensor.values().map(VecDeque::capacity).sum();
            assert!(slots <= 2 * (131_072 + stats.sensors));
            assert_eq!(inner.by_newest.len(), stats.sensors);
            assert_eq!(
                stats.bytes,
                inner
                    .by_sensor
                    .iter()
                    .map(|(s, rs)| s.len() + rs.iter().map(Reading::byte_len).sum::<usize>())
                    .sum::<usize>()
            );
        }
    }

    #[test]
    fn eviction_removes_all_accounting_and_old_admissions_are_ignored() {
        let cache = Cache::new(65_536);
        let a = SensorId::parse("a").unwrap();
        let b = SensorId::parse("b").unwrap();
        let c = SensorId::parse("c").unwrap();
        let mut large = reading(1);
        large.unit = Some("long-unit".to_owned());
        cache.insert([(&a, &large), (&b, &reading(2))]);
        assert!(cache.latest(&a, 1).is_some());
        cache.insert([(&c, &reading(3))]);
        assert!(cache.latest(&a, 1).is_none());
        let expected = CacheStats {
            sensors: 2,
            readings: 2,
            bytes: b.byte_len() + c.byte_len() + 2 * reading(1).byte_len(),
        };
        assert_eq!(cache.stats(), expected);
        cache.insert([(&a, &reading(0)), (&a, &reading(2))]);
        assert_eq!(cache.stats(), expected);
        assert!(cache.latest(&a, 1).is_none());
        cache.insert([(&a, &reading(4))]);
        assert!(cache.latest(&a, 1).is_some());
        assert!(cache.latest(&b, 1).is_none());
    }

    #[test]
    fn concurrent_historical_warm_up_preserves_newer_live_data_at_capacity() {
        let cache = Cache::new(2000);
        let hot = SensorId::parse("hot").unwrap();
        cache.insert([(&hot, &reading(100_000))]);
        for n in 0..64 {
            let sensor = SensorId::parse(&format!("cold-{n}")).unwrap();
            cache.insert([(&sensor, &reading(1))]);
        }
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for n in 1..=1000 {
                    let sensor = SensorId::parse(&format!("historical-{n}")).unwrap();
                    cache.insert([(&sensor, &reading(n)), (&hot, &reading(n))]);
                }
            });
            scope.spawn(|| {
                barrier.wait();
                for n in 100_001..=100_100 {
                    cache.insert([(&hot, &reading(n))]);
                }
            });
        });
        assert_eq!(cache.latest(&hot, 1).unwrap(), vec![reading(100_100)]);
        assert_eq!(cache.stats().sensors, 65);
        let inner = cache.lock();
        assert_eq!(inner.by_newest.len(), inner.by_sensor.len());
    }

    #[tokio::test]
    #[ignore = "requires a disposable Postgres database and INGEST_DB_* configuration"]
    async fn persisted_readings_remain_queryable_after_cache_eviction() {
        use crate::config::Config;
        use crate::db::Db;
        use crate::readings::Batch;
        use std::sync::Arc;
        use std::time::Duration;

        let config = Config::from_env().unwrap();
        let db = Arc::new(Db::connect(&config.db));
        let (ready, migrated) = tokio::sync::oneshot::channel();
        db.spawn_migrations(move |_| async move {
            let _ = ready.send(());
        });
        tokio::time::timeout(Duration::from_secs(30), migrated)
            .await
            .unwrap()
            .unwrap();
        let unique = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let a = SensorId::parse(&format!("eviction-{unique}-a")).unwrap();
        let b = SensorId::parse(&format!("eviction-{unique}-b")).unwrap();
        let c = SensorId::parse(&format!("eviction-{unique}-c")).unwrap();
        let batch = Batch(vec![
            (a.clone(), reading(1)),
            (b, reading(2)),
            (c, reading(3)),
        ]);
        let cache = Cache::new(65_536);
        // The same persist-before-cache ordering as POST /readings.
        db.insert_batch(&batch).await.unwrap();
        cache.insert(batch.0.iter().map(|(s, r)| (s, r)));
        assert!(cache.latest(&a, 1).is_none());
        // This is the database method used by GET /readings/query.
        let persisted = db
            .query_range(&a, reading(0).ts, reading(10).ts, 10)
            .await
            .unwrap();
        assert_eq!(persisted, vec![reading(1)]);
    }
}
