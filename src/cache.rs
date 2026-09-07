//! In-memory "latest readings" cache. It is capped per sensor and across
//! sensors; evicted sensors remain queryable from Postgres.

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
    by_sensor: HashMap<SensorId, Entry>,
    readings: usize,
    bytes: usize,
}

struct Entry {
    readings: VecDeque<Reading>,
    newest: time::OffsetDateTime,
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
            let sensor_is_new = !inner.by_sensor.contains_key(sensor);
            let entry = inner
                .by_sensor
                .entry(sensor.clone())
                .or_insert_with(|| Entry {
                    readings: VecDeque::with_capacity(self.per_sensor.min(64)),
                    newest: reading.ts,
                });
            if sensor_is_new {
                inner.bytes += sensor.byte_len();
            }
            let position = entry
                .readings
                .partition_point(|existing| existing.ts <= reading.ts);
            entry.readings.insert(position, reading.clone());
            entry.newest = entry.newest.max(reading.ts);
            let evicted = if entry.readings.len() > self.per_sensor {
                entry.readings.pop_front()
            } else {
                None
            };
            inner.bytes -= evicted.as_ref().map_or(0, Reading::byte_len);
            inner.bytes += reading.byte_len();
            inner.readings += usize::from(evicted.is_none());
            while inner.by_sensor.len() > self.max_sensors {
                let Some(victim) = inner
                    .by_sensor
                    .iter()
                    .min_by_key(|(_, entry)| entry.newest)
                    .map(|(sensor, _)| sensor.clone())
                else {
                    break;
                };
                let Some(removed) = inner.by_sensor.remove(&victim) else {
                    continue;
                };
                inner.bytes -= victim.byte_len()
                    + removed
                        .readings
                        .iter()
                        .map(Reading::byte_len)
                        .sum::<usize>();
                inner.readings -= removed.readings.len();
            }
        }
    }

    pub fn latest(&self, sensor: &SensorId, limit: usize) -> Option<Vec<Reading>> {
        let inner = self.lock();
        let entry = inner.by_sensor.get(sensor)?;
        Some(entry.readings.iter().rev().take(limit).cloned().collect())
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
    fn bounds_sensor_cardinality_and_keeps_newest_sensor_data() {
        let cache = Cache::new(2, 2);
        let a = SensorId::parse("a").unwrap();
        let b = SensorId::parse("b").unwrap();
        let c = SensorId::parse("c").unwrap();
        cache.insert([(&a, &reading(1)), (&b, &reading(2)), (&c, &reading(3))]);
        assert_eq!(cache.stats().sensors, 2);
        assert!(cache.latest(&c, 1).is_some());
        assert!(cache.latest(&a, 1).is_none());
    }

    #[test]
    fn historical_warm_up_does_not_evict_newer_live_sensor() {
        let cache = Cache::new(3, 2);
        let live = SensorId::parse("live").unwrap();
        let old = SensorId::parse("old").unwrap();
        let other = SensorId::parse("other").unwrap();
        cache.insert([(&live, &reading(100)), (&other, &reading(90))]);
        let historical: Vec<_> = (1..10).map(reading).collect();
        cache.insert(historical.iter().map(|r| (&old, r)));
        assert!(cache.latest(&live, 1).is_some());
        let latest = cache.latest(&live, 1).unwrap();
        assert!((latest[0].value - 100.0).abs() < f64::EPSILON);
    }
}
