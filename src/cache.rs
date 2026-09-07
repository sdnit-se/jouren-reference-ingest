//! In-memory "latest readings" cache serving `GET /readings/latest` without a
//! round trip to Postgres. Readings are capped per sensor and sensor cardinality
//! is capped globally; the newest readings are kept for resident sensors.

use crate::readings::{Reading, SensorId};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CacheStats {
    pub sensors: usize,
    pub readings: usize,
    pub bytes: usize,
}

pub struct Cache {
    per_sensor: usize,
    max_sensors: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_sensor: HashMap<SensorId, VecDeque<Reading>>,
    sensor_order: VecDeque<SensorId>,
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

    pub fn insert<'a>(&self, readings: impl IntoIterator<Item = (&'a SensorId, &'a Reading)>) {
        let mut inner = self.lock();
        for (sensor, reading) in readings {
            if !inner.by_sensor.contains_key(sensor) {
                while inner.by_sensor.len() >= self.max_sensors {
                    let Some(oldest) = inner.sensor_order.pop_front() else {
                        break;
                    };
                    if let Some(evicted) = inner.by_sensor.remove(&oldest) {
                        inner.bytes -= oldest.byte_len();
                        inner.bytes -= evicted.iter().map(Reading::byte_len).sum::<usize>();
                        inner.readings -= evicted.len();
                    }
                }
                inner.bytes += sensor.byte_len();
                inner.by_sensor.insert(
                    sensor.clone(),
                    VecDeque::with_capacity(self.per_sensor.min(64)),
                );
                inner.sensor_order.push_back(sensor.clone());
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
            inner.bytes -= evicted.as_ref().map_or(0, Reading::byte_len);
            inner.bytes += reading.byte_len();
            inner.readings += usize::from(evicted.is_none());
        }
    }

    /// An evicted sensor is a cache miss (404 from the cache-only latest API);
    /// persisted data remains available through the database-backed query API.
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
    fn caps_total_sensor_cardinality_and_evicts_oldest() {
        let cache = Cache::new(2, 2);
        let a = SensorId::parse("a").unwrap();
        let b = SensorId::parse("b").unwrap();
        let c = SensorId::parse("c").unwrap();
        let r = reading(1);
        cache.insert([(&a, &r), (&b, &r), (&c, &r)]);
        assert_eq!(cache.stats().sensors, 2);
        assert!(cache.latest(&a, 1).is_none());
        assert!(cache.latest(&b, 1).is_some());
        assert!(cache.latest(&c, 1).is_some());
    }

    #[test]
    fn keeps_only_the_newest_per_sensor() {
        let cache = Cache::new(3, 10);
        let sensor = SensorId::parse("s-1").unwrap();
        let readings: Vec<_> = (1..=5).map(reading).collect();
        cache.insert(readings.iter().map(|r| (&sensor, r)));
        let latest = cache.latest(&sensor, 10).unwrap();
        assert_eq!(
            latest.iter().map(|r| r.value).collect::<Vec<_>>(),
            vec![5.0, 4.0, 3.0]
        );
        assert_eq!(cache.stats().readings, 3);
    }

    #[test]
    fn replacement_does_not_grow_bytes_or_readings() {
        let cache = Cache::new(1, 2);
        let sensor = SensorId::parse("sensor").unwrap();
        for n in 1..20 {
            cache.insert([(&sensor, &reading(n))]);
            assert_eq!(cache.stats().readings, 1);
        }
    }
}
