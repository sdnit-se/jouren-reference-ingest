//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Per-sensor and global bounds retain newer timestamps.

use crate::readings::{Reading, SensorId};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

// Fixed safety ceilings for the reference's 192 MiB container. Bound both map
// overhead and reading storage, regardless of the per-sensor configuration.
const MAX_SENSORS: usize = 4096;
const MAX_READINGS: usize = 65_536;

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

impl Inner {
    fn remove_sensor(&mut self, sensor: &SensorId) {
        if let Some(entry) = self.by_sensor.remove(sensor) {
            self.readings -= entry.len();
            self.bytes -= sensor.byte_len() + entry.iter().map(Reading::byte_len).sum::<usize>();
        }
    }

    fn trim(&mut self) {
        while self.by_sensor.len() > MAX_SENSORS {
            // Historical warm-up must not displace a sensor whose newest
            // reading is newer. Equal-timestamp victims are unspecified.
            let oldest = self
                .by_sensor
                .iter()
                .min_by_key(|(_, entry)| entry.back().map(|r| r.ts))
                .map(|(sensor, _)| sensor.clone());
            let Some(sensor) = oldest else {
                break;
            };
            self.remove_sensor(&sensor);
        }
        while self.readings > MAX_READINGS {
            // Evict individual oldest readings, not a newer live sensor just
            // because warm-up added history to another sensor.
            let oldest = self
                .by_sensor
                .iter()
                .min_by_key(|(_, entry)| entry.front().map(|r| r.ts))
                .map(|(sensor, _)| sensor.clone());
            let Some(sensor) = oldest else {
                break;
            };
            let Some(entry) = self.by_sensor.get_mut(&sensor) else {
                break;
            };
            let removed = entry.pop_front();
            let empty = entry.is_empty();
            // A count bound alone would leave large historical allocations
            // behind after draining a deque. Keep spare capacity bounded too.
            if entry.capacity() > 2 * entry.len().max(4) {
                entry.shrink_to_fit();
            }
            if let Some(reading) = removed {
                self.readings -= 1;
                self.bytes -= reading.byte_len();
            }
            if empty {
                self.remove_sensor(&sensor);
            }
        }
    }
}

impl Cache {
    pub fn new(per_sensor: usize) -> Self {
        Self {
            per_sensor: per_sensor.clamp(1, MAX_READINGS),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Merge by timestamp and retain the newest readings, including during warm-up.
    /// Equal timestamps retain arrival order; measurements are not deduplicated.
    /// Global pressure may shorten histories or evict whole sensors; it never
    /// changes persisted readings.
    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.by_sensor.contains_key(sensor) {
                inner.bytes += sensor.byte_len();
                inner.by_sensor.insert(sensor.clone(), VecDeque::new());
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
            inner.trim();
        }
    }

    /// Newest-first slice, or `None` for an unknown or evicted sensor.
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

    fn assert_bounded_and_accounted(cache: &Cache) {
        let inner = cache.lock();
        assert!(inner.by_sensor.len() <= MAX_SENSORS);
        assert!(inner.readings <= MAX_READINGS);
        assert_eq!(
            inner.readings,
            inner.by_sensor.values().map(VecDeque::len).sum::<usize>()
        );
        assert_eq!(
            inner.bytes,
            inner
                .by_sensor
                .iter()
                .map(|(sensor, entry)| {
                    sensor.byte_len() + entry.iter().map(Reading::byte_len).sum::<usize>()
                })
                .sum::<usize>()
        );
        for entry in inner.by_sensor.values() {
            assert!(!entry.is_empty());
            assert!(entry.capacity() <= 2 * entry.len().max(4));
        }
    }

    #[test]
    fn fresh_ids_are_bounded_and_evicted_ids_can_return() {
        let cache = Cache::new(500);
        for n in 0..5000 {
            let sensor = SensorId::parse(&format!("s-{n}")).unwrap();
            cache.insert([(&sensor, &reading(n))]);
        }
        assert_bounded_and_accounted(&cache);
        assert_eq!(cache.stats().sensors, MAX_SENSORS);
        let first = SensorId::parse("s-0").unwrap();
        let newest = SensorId::parse("s-4999").unwrap();
        assert!(cache.latest(&first, 1).is_none());
        assert_eq!(cache.latest(&newest, 1).unwrap(), vec![reading(4999)]);
        // Replaying old history must not replace a newer cached sensor.
        cache.insert([(&first, &reading(0))]);
        assert!(cache.latest(&first, 1).is_none());
        cache.insert([(&first, &reading(10_000))]);
        assert_eq!(cache.latest(&first, 1).unwrap(), vec![reading(10_000)]);
        assert_bounded_and_accounted(&cache);
    }

    #[test]
    fn total_bound_applies_with_the_default_per_sensor_setting() {
        let cache = Cache::new(2000);
        for sensor_number in 0..40 {
            let sensor = SensorId::parse(&format!("s-{sensor_number}")).unwrap();
            for n in 0..2000 {
                let mut r = reading(sensor_number * 2000 + n);
                r.unit = Some("u".repeat(Reading::MAX_UNIT_LEN));
                cache.insert([(&sensor, &r)]);
            }
        }
        assert_eq!(cache.stats().readings, MAX_READINGS);
        assert_bounded_and_accounted(&cache);
    }

    #[test]
    fn historical_warm_up_concurrent_with_live_writes_respects_global_bound() {
        use std::sync::Barrier;

        let cache = Cache::new(usize::MAX);
        let hot = SensorId::parse("hot").unwrap();
        let other = SensorId::parse("other-live").unwrap();
        cache.insert([(&hot, &reading(1_000_000)), (&other, &reading(900_000))]);
        let barrier = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for n in 0..70_000 {
                    cache.insert([(&hot, &reading(n))]);
                }
            });
            scope.spawn(|| {
                barrier.wait();
                for n in 1_000_001..1_000_101 {
                    cache.insert([(&hot, &reading(n))]);
                }
            });
        });
        assert_eq!(cache.stats().readings, MAX_READINGS);
        assert_eq!(cache.latest(&hot, 1).unwrap(), vec![reading(1_000_100)]);
        assert_eq!(cache.latest(&other, 1).unwrap(), vec![reading(900_000)]);
        assert_bounded_and_accounted(&cache);
    }
}
