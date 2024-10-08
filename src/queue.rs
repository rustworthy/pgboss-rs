use super::utils;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, FromRow, Row};
use std::time::Duration;

/// Policy to apply to the jobs in this queue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all(serialize = "lowercase"))]
#[non_exhaustive]
pub enum QueuePolicy {
    /// Standard (default).
    ///
    /// Supports all standard features such as deferral, priority, and throttling.
    #[default]
    Standard,

    /// Short.
    ///
    /// All standard features, but only allows 1 job to be _queued_, unlimited active.
    /// Can be extended with `singletonKey`
    Short,

    /// Singleton.
    ///
    /// All standard features, but only allows 1 job to be _active_, unlimited queued.
    /// Can be extended with `singletonKey`
    Singleton,

    /// Stately.
    ///
    /// Combination of short and singleton: only allows 1 job per state, queued and/or active.
    /// Can be extended with `singletonKey`
    Stately,
}

impl TryFrom<String> for QueuePolicy {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "short" => Ok(Self::Short),
            "singleton" => Ok(Self::Singleton),
            "stately" => Ok(Self::Stately),
            "standard" => Ok(Self::Standard),
            other => Err(format!("Unsupported queue policy: {}", other)),
        }
    }
}

impl std::fmt::Display for QueuePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Standard => "standard",
            Self::Short => "short",
            Self::Singleton => "singleton",
            Self::Stately => "stately",
        };
        write!(f, "{}", s)
    }
}

/// Queue configuration.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Queue<'a> {
    /// Queue name.
    pub name: &'a str,

    /// Policy to apply to this queue.
    pub policy: QueuePolicy,

    /// Name of the dead letter queue.
    ///
    /// Note that the dead letter queue itself should be created
    /// ahead of time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter: Option<&'a str>,

    /// Number of retry attempts for jobs in this queue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_limit: Option<usize>,

    /// Time to wait before a retry attempt.
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_delay: Option<Duration>,

    /// Whether to use a backoff between retry attempts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_backoff: Option<bool>,

    /// Time to wait before expiring this job.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default).
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        rename = "expireInSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub expire_in: Option<Duration>,

    /// For how long this job should be retained in the system.
    ///
    /// Should be greater than or eqaul to 1 second, or simply unset (default).
    #[serde(
        serialize_with = "utils::serialize_duration_as_mins",
        rename = "retentionMinutes",
        skip_serializing_if = "Option::is_none"
    )]
    pub retain_for: Option<Duration>,
}

impl<'a> Queue<'a> {
    /// Returns a builder for [`Queue`]
    pub fn builder() -> QueueBuilder<'a> {
        QueueBuilder::default()
    }
}

/// Convenience builder for [`Queue`]
#[derive(Debug, Clone, Default)]
pub struct QueueBuilder<'a> {
    name: &'a str,
    policy: QueuePolicy,
    dead_letter: Option<&'a str>,
    retry_limit: Option<usize>,
    retry_delay: Option<Duration>,
    retry_backoff: Option<bool>,
    expire_in: Option<Duration>,
    retain_for: Option<Duration>,
}

impl<'a> QueueBuilder<'a> {
    /// Queue name.
    pub fn name(mut self, val: &'a str) -> Self {
        self.name = val;
        self
    }

    /// Policy to apply to this queue.
    pub fn policy(mut self, val: QueuePolicy) -> Self {
        self.policy = val;
        self
    }

    /// Name of the dead letter queue.
    ///
    /// Note that the dead letter queue itself should be created
    /// ahead of time.
    pub fn dead_letter(mut self, val: &'a str) -> Self {
        self.dead_letter = Some(val);
        self
    }

    /// Number of retry attempts for jobs in this queue.
    pub fn retry_limit(mut self, val: usize) -> Self {
        self.retry_limit = Some(val);
        self
    }

    /// Time to wait before a retry attempt.
    pub fn retry_delay(mut self, val: Duration) -> Self {
        self.retry_delay = Some(val);
        self
    }

    /// Whether to use a backoff between retry attempts.
    pub fn retry_backoff(mut self, val: bool) -> Self {
        self.retry_backoff = Some(val);
        self
    }

    /// Time to wait before expiring this job.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default)
    pub fn expire_in(mut self, val: Duration) -> Self {
        self.expire_in = Some(val);
        self
    }

    /// For how long this job should be retained in the system.
    ///
    /// Should be greater than or eqaul to 1 second, or simply unset (default)
    pub fn retain_for(mut self, val: Duration) -> Self {
        self.retain_for = Some(val);
        self
    }

    /// Terminal method for the builder returing [`Queue`]
    pub fn build(self) -> Queue<'a> {
        Queue {
            name: self.name,
            policy: self.policy,
            dead_letter: self.dead_letter,
            retry_limit: self.retry_limit,
            retry_delay: self.retry_delay,
            retry_backoff: self.retry_backoff,
            expire_in: self.expire_in,
            retain_for: self.retain_for,
        }
    }
}

/// Job queue info.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueDetails {
    /// Queue name.
    pub name: String,

    /// Queue policy.
    pub policy: QueuePolicy,

    /// Number of retry attempts.
    pub retry_limit: Option<usize>,

    /// Time to wait before a retry attempt.
    pub retry_delay: Option<Duration>,

    /// Whether to use a backoff between retry attempts.
    pub retry_backoff: Option<bool>,

    /// Time to wait before expiring this job.
    pub expire_in: Option<Duration>,

    /// For how long this job should be retained in the system.
    pub retain_for: Option<Duration>,

    /// Name of the dead letter queue.
    pub dead_letter: Option<String>,

    /// Date and time when this queue was created.
    pub created_at: DateTime<Utc>,

    /// Date and time when this queue was updated.
    pub updated_at: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for QueueDetails {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let name: String = row.try_get("name")?;
        let policy: QueuePolicy = row.try_get("policy").and_then(|v: String| {
            QueuePolicy::try_from(v).map_err(|e| sqlx::Error::ColumnDecode {
                index: "policy".to_string(),
                source: e.into(),
            })
        })?;
        let retry_limit: Option<usize> =
            row.try_get("retry_limit")
                .and_then(|v: Option<i32>| match v {
                    None => Ok(None),
                    Some(v) if v >= 0 => Ok(Some(v as usize)),
                    Some(v) => Err(sqlx::Error::ColumnDecode {
                        index: "retry_limit".to_string(),
                        source: format!("'retry_limit' should be non-negative, got {}", v).into(),
                    }),
                })?;
        let retry_delay: Option<Duration> =
            row.try_get("retry_delay")
                .and_then(|v: Option<i32>| match v {
                    None => Ok(None),
                    Some(v) if v >= 0 => Ok(Some(Duration::from_secs(v as u64))),
                    Some(v) => Err(sqlx::Error::ColumnDecode {
                        index: "retry_delay".to_string(),
                        source: format!("'retry_delay' should be non-negative, got: {}", v).into(),
                    }),
                })?;
        let retry_backoff: Option<bool> = row.try_get("retry_backoff")?;
        let expire_in: Option<Duration> =
            row.try_get("expire_seconds")
                .and_then(|v: Option<i32>| match v {
                    None => Ok(None),
                    Some(v) if v >= 0 => Ok(Some(Duration::from_secs(v as u64))),
                    Some(v) => Err(sqlx::Error::ColumnDecode {
                        index: "expire_seconds".to_string(),
                        source: format!("'expire_seconds' should be non-negative, got: {}", v)
                            .into(),
                    }),
                })?;
        let retain_for: Option<Duration> =
            row.try_get("retention_minutes")
                .and_then(|v: Option<i32>| match v {
                    None => Ok(None),
                    Some(v) if v >= 0 => Ok(Some(Duration::from_secs((v * 60) as u64))),
                    Some(v) => Err(sqlx::Error::ColumnDecode {
                        index: "retention_minutes".to_string(),
                        source: format!("'retention_minutes' should be non-negative, got: {}", v)
                            .into(),
                    }),
                })?;
        let dead_letter: Option<String> = row.try_get("dead_letter")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        Ok(QueueDetails {
            name,
            policy,
            retry_limit,
            retry_delay,
            retry_backoff,
            expire_in,
            retain_for,
            dead_letter,
            created_at,
            updated_at,
        })
    }
}
