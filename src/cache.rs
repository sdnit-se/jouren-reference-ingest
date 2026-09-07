//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Both per-sensor history and total cardinality are
//! bounded; eviction never changes persisted readings.

use crate::readings::{Reading, SensorId};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

// Reserve against full histories, not just the current logical byte gauge.
// That gauge excludes spare deque capacity and map/allocator overhead.
const MAX_CACHED_READINGS: usize = 100_000;
const MAX_CACHED_SENSORS: usize = 4096;

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

impl Inner {
    /// Admit fresh IDs by measurement recency, not warm-up arrival order.
    fn admit(&mut self, sensor: &SensorId, reading: &Reading, max_sensors: usize) -> bool {
        if self.by_sensor.contains_key(sensor) {
            return true;
        }
        if self.by_sensor.len() < max_sensors {
            return true;
        }
        let oldest = self
            .by_sensor
            .iter()
            .filter_map(|(id, entry)| entry.back().map(|r| (id, r.ts)))
            .min_by(|(left, left_ts), (right, right_ts)| {
                left_ts
                    .cmp(right_ts)
                    .then_with(|| left.as_str().cmp(right.as_str()))
            });
        let Some((victim, newest_ts)) = oldest else {
            return false;
        };
        if reading.ts < newest_ts {
            return false;
        }
        let victim = victim.clone();
        if let Some(evicted) = self.by_sensor.remove(&victim) {
            self.readings -= evicted.len();
            self.bytes -= victim.byte_len() + evicted.iter().map(Reading::byte_len).sum::<usize>();
        }
        true
    }
}

impl Cache {
    pub fn new(per_sensor: usize) -> Self {
        let per_sensor = per_sensor.clamp(1, MAX_CACHED_READINGS);
        Self {
            per_sensor,
            max_sensors: (MAX_CACHED_READINGS / per_sensor).min(MAX_CACHED_SENSORS),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Effective limit, also used to bound database warm-up batches.
    pub fn per_sensor(&self) -> usize {
        self.per_sensor
    }

    /// Merge by timestamp and retain the newest readings, including during warm-up.
    /// Equal timestamps retain arrival order; measurements are not deduplicated.
    /// At capacity, fresh IDs replace the sensor with the oldest newest reading.
    /// Older historical data is not admitted in place of newer cached data.
    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.admit(sensor, reading, self.max_sensors) {
                continue;
            }
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

    /// Newest-first cached readings, or `None` for an unknown or evicted sensor.
    /// Persisted readings remain available through the database range endpoint.
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

    #[test]
    fn fresh_ids_and_full_histories_fit_deployed_and_default_budgets() {
        for per_sensor in [500, 2000] {
            let cache = Cache::new(per_sensor);
            let expected_sensors = 100_000 / per_sensor;
            for n in 0..32_000 {
                let sensor = SensorId::parse(&format!("s-{n}")).unwrap();
                cache.insert([(&sensor, &reading(n))]);
            }
            assert_eq!(cache.stats().sensors, expected_sensors);
            assert!(cache.latest(&SensorId::parse("s-0").unwrap(), 1).is_none());
            let sensors: Vec<_> = cache.lock().by_sensor.keys().cloned().collect();
            for sensor in &sensors {
                for n in 0..=per_sensor {
                    let mut r = reading(40_000 + i32::try_from(n).unwrap());
                    r.unit = Some("u".repeat(Reading::MAX_UNIT_LEN));
                    cache.insert([(sensor, &r)]);
                }
            }
            assert_eq!(cache.stats().readings, 100_000);
            let inner = cache.lock();
            let allocated_slots: usize = inner.by_sensor.values().map(VecDeque::capacity).sum();
            assert!(allocated_slots <= 2 * (100_000 + expected_sensors));
            let bytes: usize = inner
                .by_sensor
                .iter()
                .map(|(id, rows)| id.byte_len() + rows.iter().map(Reading::byte_len).sum::<usize>())
                .sum();
            assert_eq!(inner.bytes, bytes);
        }
    }

    #[test]
    fn cardinality_is_bounded_even_with_tiny_histories() {
        let cache = Cache::new(1);
        for n in 0..10_000 {
            let sensor = SensorId::parse(&format!("s-{n}")).unwrap();
            cache.insert([(&sensor, &reading(n))]);
        }
        assert_eq!(cache.stats().sensors, 4096);
        assert_eq!(cache.stats().readings, 4096);
    }

    #[test]
    fn eviction_removes_bytes_and_old_history_cannot_readmit_the_victim() {
        let cache = Cache::new(50_000);
        let a = SensorId::parse("a").unwrap();
        let b = SensorId::parse("b").unwrap();
        let c = SensorId::parse("c").unwrap();
        let mut large = reading(1);
        large.unit = Some("long-unit".to_owned());
        cache.insert([(&a, &large), (&a, &reading(2)), (&b, &reading(3))]);
        let before = cache.stats().bytes;
        cache.insert([(&c, &reading(4))]);
        assert!(cache.latest(&a, 10).is_none());
        assert_eq!(cache.stats().readings, 2);
        assert_eq!(
            cache.stats().bytes,
            before - a.byte_len() - large.byte_len() - reading(2).byte_len()
                + c.byte_len()
                + reading(4).byte_len()
        );
        let after = cache.stats();
        cache.insert([(&a, &reading(1))]);
        assert_eq!(cache.stats(), after);
        assert!(cache.latest(&a, 10).is_none());
        cache.insert([(&a, &reading(5))]);
        assert_eq!(cache.latest(&a, 1).unwrap(), [reading(5)]);
        assert!(cache.latest(&b, 1).is_none());
    }

    #[test]
    fn historical_warm_up_concurrent_with_live_writes_preserves_newest_data() {
        let cache = Cache::new(500);
        let live = SensorId::parse("live").unwrap();
        for n in 0..199 {
            let sensor = SensorId::parse(&format!("seed-{n}")).unwrap();
            cache.insert([(&sensor, &reading(1))]);
        }
        cache.insert([(&live, &reading(2000))]);
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for n in 1..1000 {
                    let sensor = SensorId::parse(&format!("historical-{n}")).unwrap();
                    cache.insert([(&sensor, &reading(n)), (&live, &reading(n))]);
                }
            });
            barrier.wait();
            for n in 2001..=2100 {
                cache.insert([(&live, &reading(n))]);
            }
        });
        assert_eq!(cache.stats().sensors, 200);
        assert!(cache.stats().readings <= 100_000);
        let latest = cache.latest(&live, 500).unwrap();
        let expected: Vec<_> = (2000..=2100).rev().map(reading).collect();
        assert_eq!(&latest[..101], expected.as_slice());
        assert!(latest.windows(2).all(|pair| pair[0].ts >= pair[1].ts));
    }

    #[test]
    fn effective_per_sensor_limit_is_always_finite_and_positive() {
        assert_eq!(Cache::new(0).per_sensor(), 1);
        let cache = Cache::new(usize::MAX);
        assert_eq!(cache.per_sensor(), 100_000);
        assert_eq!(cache.max_sensors, 1);
    }
}
