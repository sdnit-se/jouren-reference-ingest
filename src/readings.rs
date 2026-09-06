//! The reading domain type and the validation that turns request JSON into it.
//! Everything downstream (cache, database) receives `Reading`s that have
//! already passed here.

use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;

/// A sensor identifier as it arrived, already checked for length.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SensorId(String);

impl SensorId {
    pub const MAX_LEN: usize = 64;

    /// Accepts 1..=64 non-blank characters; anything else is a client error.
    pub fn parse(raw: &str) -> Result<Self, ValidationError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.len() > Self::MAX_LEN {
            return Err(ValidationError::SensorId(raw.len()));
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Bytes this id occupies when held in the cache.
    pub fn byte_len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Display for SensorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One validated measurement.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Reading {
    #[serde(with = "time::serde::rfc3339")]
    pub ts: OffsetDateTime,
    pub value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

impl Reading {
    pub const MAX_UNIT_LEN: usize = 16;

    /// Rough heap+inline footprint, used for the cache size gauge.
    pub fn byte_len(&self) -> usize {
        std::mem::size_of::<Self>() + self.unit.as_ref().map_or(0, String::len)
    }
}

/// The wire shape of one reading in `POST /readings`.
#[derive(Debug, Deserialize)]
pub struct ReadingInput {
    pub sensor_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub ts: OffsetDateTime,
    pub value: f64,
    #[serde(default)]
    pub unit: Option<String>,
}

/// The wire shape of the whole batch.
#[derive(Debug, Deserialize)]
pub struct BatchInput {
    pub readings: Vec<ReadingInput>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("batch must hold between 1 and {max} readings, got {got}")]
    BatchSize { got: usize, max: usize },
    #[error("sensor_id must be 1 to 64 characters, got {0}")]
    SensorId(usize),
    #[error("value must be a finite number")]
    Value,
    #[error("unit must be at most 16 characters, got {0}")]
    Unit(usize),
}

/// A batch that passed validation; the only way to obtain readings for
/// ingest.
#[derive(Debug)]
pub struct Batch(pub Vec<(SensorId, Reading)>);

impl Batch {
    pub const MAX_LEN: usize = 1000;

    pub fn parse(input: BatchInput) -> Result<Self, ValidationError> {
        let got = input.readings.len();
        if got == 0 || got > Self::MAX_LEN {
            return Err(ValidationError::BatchSize {
                got,
                max: Self::MAX_LEN,
            });
        }
        input
            .readings
            .into_iter()
            .map(|r| {
                let sensor = SensorId::parse(&r.sensor_id)?;
                if !r.value.is_finite() {
                    return Err(ValidationError::Value);
                }
                if let Some(unit) = &r.unit
                    && unit.len() > Reading::MAX_UNIT_LEN
                {
                    return Err(ValidationError::Unit(unit.len()));
                }
                Ok((
                    sensor,
                    Reading {
                        ts: r.ts,
                        value: r.value,
                        unit: r.unit,
                    },
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn input(sensor: &str, value: f64, unit: Option<&str>) -> ReadingInput {
        ReadingInput {
            sensor_id: sensor.to_owned(),
            ts: datetime!(2026-09-04 12:00 UTC),
            value,
            unit: unit.map(str::to_owned),
        }
    }

    #[test]
    fn accepts_a_valid_batch() {
        let batch = Batch::parse(BatchInput {
            readings: vec![input("s-0001", 21.5, Some("C"))],
        })
        .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.0[0].0.as_str(), "s-0001");
    }

    #[test]
    fn rejects_empty_and_oversized_batches() {
        let empty = Batch::parse(BatchInput { readings: vec![] }).unwrap_err();
        assert_eq!(empty, ValidationError::BatchSize { got: 0, max: 1000 });
        let readings = (0..1001).map(|_| input("s", 1.0, None)).collect();
        let big = Batch::parse(BatchInput { readings }).unwrap_err();
        assert_eq!(
            big,
            ValidationError::BatchSize {
                got: 1001,
                max: 1000
            }
        );
    }

    #[test]
    fn rejects_bad_fields() {
        let long = "x".repeat(65);
        let err = Batch::parse(BatchInput {
            readings: vec![input(&long, 1.0, None)],
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::SensorId(65));

        let err = Batch::parse(BatchInput {
            readings: vec![input("s", f64::NAN, None)],
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::Value);

        let err = Batch::parse(BatchInput {
            readings: vec![input("s", 1.0, Some("kilometres-per-hour"))],
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::Unit(19));
    }
}
