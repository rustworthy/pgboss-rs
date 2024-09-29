use super::utils;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, prelude::FromRow, Row};
use std::time::Duration;
use uuid::Uuid;

#[cfg(doc)]
use crate::QueueOptions;
use crate::QueuePolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobState {
    Created,
    Retry,
    Active,
    Completed,
    Cancelled,
    Failed,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Created => "created",
            Self::Retry => "retry",
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        };
        write!(f, "{}", s)
    }
}

/// Custom job options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobOptions {
    /// Job's priority.
    ///
    /// Higher numbers will have higher priority
    /// when fetching from the queue.
    pub priority: usize,

    /// Name of the dead letter queue for this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter: Option<String>,

    /// Number of retry attempts.
    ///
    /// If omitted, a value will be taken from queue via [`QueueOptions::retry_limit`]
    /// and - if not set there either - will default to `2` retry attempts.
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
    /// Specifies for how long this job may be in `active` state before
    /// it is failed because of expiration.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default).
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        skip_serializing_if = "Option::is_none"
    )]
    pub expire_in: Option<Duration>,

    /// For how long this job should be retained in the system.
    ///
    /// Specifies for how long this job may be in `created` or `retry` state before
    /// it is archived.
    ///
    /// Should be greater than or equal to 1 second, or simply unset (default).
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        rename = "keep_until",
        skip_serializing_if = "Option::is_none"
    )]
    pub retain_for: Option<Duration>,

    /// For how long this job should _not_ be visible to consumers.
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        rename = "start_after",
        skip_serializing_if = "Option::is_none"
    )]
    pub delay_for: Option<Duration>,
}

/// A job to be sent to the server.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Job {
    /// ID to assign to this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,

    /// Name of the queue to put this job onto.
    pub queue_name: String,

    /// Job's payload.
    pub data: serde_json::Value,

    /// Options specific to this job.
    pub opts: JobOptions,
}

/// A job fetched from the server.
///
/// As soon as a job is fetched from the server, it's status transitions to `active`
/// and whoever has fetch this job will hav
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobDetails {
    /// ID of this job.
    pub id: Uuid,

    /// Name of the queue this job was fetched from.
    pub queue_name: String,

    /// Job's payload.
    pub data: serde_json::Value,

    /// Execution timeout.
    ///
    /// Specifies for how long this job may be in `active` state before
    /// it is failed because of expiration
    pub expire_in: Duration,

    /// [Policy](QueuePolicy) applied to this job.
    pub policy: QueuePolicy,

    /// Job's priority.
    pub priority: usize,

    /// Retry limit for this job.
    pub retry_limit: usize,

    /// Time to wait before a retry attempt.
    pub retry_delay: Duration,

    /// How many times this job was retried.
    pub retry_count: usize,

    /// Whether to use a backoff between retry attempts.
    pub retry_backoff: bool,

    /// When the job was registered by the server.
    pub created_at: DateTime<Utc>,

    /// When to make this job 'visible' for consumers.
    pub start_after: DateTime<Utc>,

    /// When this job was last consumed.
    ///
    /// Will be `None` for a job that was not consumed just yet.
    pub started_at: Option<DateTime<Utc>>,
}

impl FromRow<'_, PgRow> for JobDetails {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let id: Uuid = row.try_get("id")?;
        let queue_name: String = row.try_get("name")?;
        let data: serde_json::Value = row.try_get("data")?;
        let expire_in: Duration = row.try_get("expire_in").and_then(|v: f64| match v {
            v if v >= 0.0 => Ok(Duration::from_secs_f64(v)),
            _ => Err(sqlx::Error::ColumnDecode {
                index: "expire_in".to_string(),
                source: "'expire_in' should be non-negative".into(),
            }),
        })?;
        let policy: QueuePolicy = row.try_get("policy").and_then(|v: String| {
            QueuePolicy::try_from(v).map_err(|e| sqlx::Error::ColumnDecode {
                index: "policy".to_string(),
                source: e.into(),
            })
        })?;
        let priority = row.try_get("priority").and_then(|v: i32| match v {
            v if v >= 0 => Ok(v as usize),
            v => Err(sqlx::Error::ColumnDecode {
                index: "retry_delay".to_string(),
                source: format!("'priority' should be non-negative, got: {}", v).into(),
            }),
        })?;
        let retry_limit = row.try_get("retry_limit").and_then(|v: i32| match v {
            v if v >= 0 => Ok(v as usize),
            v => Err(sqlx::Error::ColumnDecode {
                index: "retry_limit".to_string(),
                source: format!("'retry_limit' should be non-negative, got: {}", v).into(),
            }),
        })?;
        let retry_delay = row.try_get("retry_delay").and_then(|v: i32| match v {
            v if v >= 0 => Ok(Duration::from_secs(v as u64)),
            v => Err(sqlx::Error::ColumnDecode {
                index: "retry_delay".to_string(),
                source: format!("'retry_delay' should be non-negative, got: {}", v).into(),
            }),
        })?;
        let retry_count = row.try_get("retry_count").and_then(|v: i32| match v {
            v if v >= 0 => Ok(v as usize),
            v => Err(sqlx::Error::ColumnDecode {
                index: "retry_count".to_string(),
                source: format!("'retry_count' should be non-negative, got: {}", v).into(),
            }),
        })?;
        let retry_backoff: bool = row.try_get("retry_backoff")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let started_at: Option<DateTime<Utc>> = row.try_get("started_at")?;
        let start_after: DateTime<Utc> = row.try_get("start_after")?;

        Ok(JobDetails {
            id,
            queue_name,
            data,
            expire_in,
            policy,
            priority,
            retry_limit,
            retry_delay,
            retry_count,
            retry_backoff,
            created_at,
            start_after,
            started_at,
        })
    }
}

impl Job {
    /// Creates a builder for a job
    pub fn builder() -> JobBuilder {
        JobBuilder::default()
    }
}

/// A builder for a job.
#[derive(Debug, Clone, Default)]
pub struct JobBuilder {
    pub(crate) id: Option<Uuid>,
    pub(crate) queue_name: String,
    pub(crate) data: serde_json::Value,
    pub(crate) opts: JobOptions,
}

impl JobBuilder {
    /// ID to assign to this job.
    pub fn id(mut self, value: Uuid) -> Self {
        self.id = Some(value);
        self
    }

    /// Name of the queue to put this job onto.
    pub fn queue_name<S>(mut self, value: S) -> Self
    where
        S: Into<String>,
    {
        self.queue_name = value.into();
        self
    }

    /// Job's payload.
    pub fn data(mut self, value: serde_json::Value) -> Self {
        self.data = value;
        self
    }

    /// Job's priority.
    pub fn priority(mut self, value: usize) -> Self {
        self.opts.priority = value;
        self
    }

    /// Name of the dead letter queue for this job.
    pub fn dead_letter<S>(mut self, value: S) -> Self
    where
        S: Into<String>,
    {
        self.opts.dead_letter = Some(value.into());
        self
    }

    /// Number of retry attempts.

    pub fn retry_limit(mut self, value: usize) -> Self {
        self.opts.retry_limit = Some(value);
        self
    }

    /// Time to wait before a retry attempt.
    pub fn retry_delay(mut self, value: Duration) -> Self {
        self.opts.retry_delay = Some(value);
        self
    }

    /// Whether to use a backoff between retry attempts.
    pub fn retry_backoff(mut self, value: bool) -> Self {
        self.opts.retry_backoff = Some(value);
        self
    }

    /// Time to wait before expiring this job.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default).
    pub fn expire_in(mut self, value: Duration) -> Self {
        self.opts.expire_in = Some(value);
        self
    }

    /// For how long this job should be retained in the system.
    ///
    /// Should be greater than or equal to 1 second, or simply unset (default).
    pub fn retain_for(mut self, value: Duration) -> Self {
        self.opts.retain_for = Some(value);
        self
    }

    /// For how long this job should _not_ be visible to consumers.
    pub fn delay_for(mut self, value: Duration) -> Self {
        self.opts.delay_for = Some(value);
        self
    }

    /// Creates a job.
    pub fn build(self) -> Job {
        Job {
            id: self.id,
            queue_name: self.queue_name,
            data: self.data,
            opts: self.opts,
        }
    }
}
