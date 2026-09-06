//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Readings are capped per sensor; the newest are kept.

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
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_sensor: HashMap<SensorId, VecDeque<Reading>>,
    readings: usize,
    bytes: usize,
}

impl Cache {
    pub fn new(per_sensor: usize) -> Self {
        Self {
            per_sensor: per_sensor.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Merge by timestamp and retain the newest readings, including during warm-up.
    /// Equal timestamps retain arrival order; measurements are not deduplicated.
    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.by_sensor.contains_key(sensor) {
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

    /// Newest-first slice of a sensor's readings, or `None` for an unknown sensor.
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
}
