//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Both sensor count and readings per sensor are capped.

use crate::readings::{Reading, SensorId};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

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
    by_sensor: HashMap<SensorId, VecDeque<Reading>>,
    readings: usize,
    bytes: usize,
}

impl Cache {
    pub fn new(per_sensor: usize, max_sensors: usize) -> Self {
        Self {
            per_sensor: per_sensor.max(1),
            max_sensors: max_sensors.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Merge by timestamp and retain the newest readings, including during warm-up.
    /// Equal timestamps retain arrival order; measurements are not deduplicated.
    /// At capacity, retain sensors with the newest measurement timestamps, breaking
    /// ties by sensor id. Historical arrivals cannot evict newer live sensors.
    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.by_sensor.contains_key(sensor) {
                if !inner.admit(sensor, reading, self.max_sensors) {
                    continue;
                }
                inner.bytes += sensor.byte_len();
                inner.by_sensor.insert(
                    sensor.clone(),
                    VecDeque::with_capacity(self.per_sensor.min(64)),
                );
            }
            let Some(entry) = inner.by_sensor.get_mut(sensor) else {
                continue;
            };
            let position = entry.partition_point(|existing| existing.ts <= reading.ts);
            entry.insert(position, reading.clone());
            let evicted = if entry.len() > self.per_sensor {
                entry.pop_front()
            } else {
                None
            };
            // Subtract from the total: an insertion delta cannot represent shrinkage.
            inner.bytes -= evicted.as_ref().map_or(0, Reading::byte_len);
            inner.bytes += reading.byte_len();
            inner.readings += usize::from(evicted.is_none());
        }
    }

    /// Newest-first slice, or `None` for an unknown or evicted sensor.
    /// Cache eviction never removes persisted readings.
    pub fn latest(&self, sensor: &SensorId, limit: usize) -> Option<Vec<Reading>> {
        let inner = self.lock();
        let entry = inner.by_sensor.get(sensor)?;
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

impl Inner {
    /// Called only for a missing sensor, under the same lock as insertion.
    fn admit(&mut self, sensor: &SensorId, reading: &Reading, max_sensors: usize) -> bool {
        if self.by_sensor.len() < max_sensors {
            return true;
        }
        let oldest = self
            .by_sensor
            .iter()
            .filter_map(|(id, rows)| rows.back().map(|row| (id, row.ts)))
            .min_by(|(a, at), (b, bt)| at.cmp(bt).then_with(|| a.as_str().cmp(b.as_str())));
        let Some((id, ts)) = oldest else {
            return false;
        };
        if (reading.ts, sensor.as_str()) <= (ts, id.as_str()) {
            return false;
        }
        let id = id.clone();
        if let Some(rows) = self.by_sensor.remove(&id) {
            self.readings -= rows.len();
            self.bytes -= id.byte_len() + rows.iter().map(Reading::byte_len).sum::<usize>();
        }
        true
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
        let cache = Cache::new(3, 128);
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
        let cache = Cache::new(1, 128);
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
        let cache = Cache::new(1, 128);
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
        let cache = Cache::new(3, 128);
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
        let cache = Cache::new(3, 128);
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
        let cache = Cache::new(10, 128);
        let a = SensorId::parse("a").unwrap();
        assert!(cache.latest(&a, 5).is_none());
        let readings: Vec<_> = (1..=4).map(reading).collect();
        cache.insert(readings.iter().map(|r| (&a, r)));
        assert_eq!(cache.latest(&a, 2).unwrap().len(), 2);
    }

    #[test]
    fn stats_count_every_sensor() {
        let cache = Cache::new(2, 128);
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
    fn sensor_churn_evicts_whole_entries_and_keeps_exact_counts() {
        let cache = Cache::new(3, 2);
        let first = SensorId::parse("sensor-0").unwrap();
        for n in 0..1000 {
            let sensor = SensorId::parse(&format!("sensor-{n}")).unwrap();
            let mut r = reading(n);
            r.unit = Some("abcdefghijklmnop".to_owned());
            cache.insert([(&sensor, &r), (&sensor, &r)]);
            let inner = cache.lock();
            assert!(inner.by_sensor.len() <= 2);
            assert!(inner.readings <= 6);
            assert_eq!(
                inner.readings,
                inner.by_sensor.values().map(VecDeque::len).sum::<usize>()
            );
            assert_eq!(
                inner.bytes,
                inner
                    .by_sensor
                    .iter()
                    .map(|(id, rows)| {
                        id.byte_len() + rows.iter().map(Reading::byte_len).sum::<usize>()
                    })
                    .sum::<usize>()
            );
        }
        assert!(cache.latest(&first, 10).is_none());
        let newest = SensorId::parse("sensor-999").unwrap();
        assert_eq!(cache.latest(&newest, 10).unwrap().len(), 2);
        cache.insert([(&first, &reading(1001))]);
        assert_eq!(cache.latest(&first, 10).unwrap(), vec![reading(1001)]);
    }

    #[test]
    fn timestamp_ties_have_deterministic_sensor_admission() {
        for ids in [["a", "b"], ["b", "a"]] {
            let cache = Cache::new(1, 1);
            for id in ids {
                let sensor = SensorId::parse(id).unwrap();
                cache.insert([(&sensor, &reading(1))]);
            }
            assert!(cache.latest(&SensorId::parse("a").unwrap(), 1).is_none());
            assert!(cache.latest(&SensorId::parse("b").unwrap(), 1).is_some());
        }
    }

    #[test]
    fn concurrent_historical_warm_up_preserves_newer_live_sensor_and_readings() {
        let cache = Cache::new(3, 2);
        let live = SensorId::parse("live").unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                cache.insert([(&live, &reading(1000))]);
                barrier.wait();
                for n in 1001..1100 {
                    cache.insert([(&live, &reading(n))]);
                }
            });
            scope.spawn(|| {
                barrier.wait();
                for n in 1..1000 {
                    let historical = SensorId::parse(&format!("old-{n}")).unwrap();
                    cache.insert([(&historical, &reading(n)), (&live, &reading(n))]);
                    assert!(cache.latest(&live, 3).is_some());
                    assert!(cache.stats().sensors <= 2);
                }
            });
        });
        assert_eq!(
            cache.latest(&live, 3).unwrap(),
            vec![reading(1099), reading(1098), reading(1097)]
        );
    }

    #[test]
    fn default_per_sensor_budget_remains_bounded_with_full_units() {
        let cache = Cache::new(2000, 128);
        for n in 0..130 {
            let sensor = SensorId::parse(&format!("{n:064}")).unwrap();
            for ts in 0..2001 {
                // Admit each new sensor immediately so churn still fills every entry.
                let mut r = reading(n * 2001 + ts);
                r.unit = Some("u".repeat(Reading::MAX_UNIT_LEN));
                cache.insert([(&sensor, &r)]);
            }
        }
        let inner = cache.lock();
        assert_eq!(inner.by_sensor.len(), 128);
        assert_eq!(inner.readings, 256_000);
        let storage: usize = inner
            .by_sensor
            .values()
            .map(|rows| {
                rows.capacity() * std::mem::size_of::<Reading>()
                    + rows.len() * (Reading::MAX_UNIT_LEN + 16)
            })
            .sum();
        // Includes a conservative unit-allocation overhead; not a process RSS gauge.
        assert!(storage < 32 * 1024 * 1024);
    }

    #[tokio::test]
    #[ignore = "requires an isolated Postgres database and INGEST_DB_* environment"]
    async fn persisted_readings_remain_queryable_after_cache_eviction() {
        use crate::config::Config;
        use crate::db::Db;
        use crate::readings::Batch;
        use std::sync::Arc;
        use std::time::Duration;

        let config = Config::from_env().unwrap();
        let db = Arc::new(Db::connect(&config.db));
        db.spawn_migrations(|_| async {});
        tokio::time::timeout(Duration::from_secs(15), async {
            while !db.is_migrated() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let unique = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let old = SensorId::parse(&format!("eviction-old-{unique}")).unwrap();
        let new = SensorId::parse(&format!("eviction-new-{unique}")).unwrap();
        let batch = Batch(vec![(old.clone(), reading(1)), (new, reading(2))]);
        // Same ordering as the HTTP ingest handler: persistence before caching.
        db.insert_batch(&batch).await.unwrap();
        let cache = Cache::new(3, 1);
        cache.insert(batch.0.iter().map(|(sensor, row)| (sensor, row)));
        assert!(cache.latest(&old, 3).is_none());
        let rows = db
            .query_range(&old, reading(1).ts, reading(2).ts, 10)
            .await
            .unwrap();
        assert_eq!(rows, vec![reading(1)]);
    }
}
