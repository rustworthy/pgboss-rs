use super::utils;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgRow, PgValueRef},
    prelude::FromRow,
    Row,
};
use std::time::Duration;
use uuid::Uuid;

#[cfg(doc)]
use crate::Queue;
use crate::QueuePolicy;

/// Job's state.
///
/// Each job registed in the system gets assigned status `created`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum JobState {
    /// Job has been registered.
    #[default]
    Created,

    /// Job has been failed and can now be retried.
    Retry,

    /// Job has been consumed and is being processed by a worker.
    Active,

    /// Job has been compeleted.
    Completed,

    /// Job has been cancelled.
    Cancelled,

    /// Job has been failed.
    Failed,
}

impl TryFrom<String> for JobState {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "created" => Ok(Self::Created),
            "retry" => Ok(Self::Retry),
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            other => Err(format!("Unsupported job state: {}", other)),
        }
    }
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
pub(crate) struct JobOptions<'a> {
    priority: usize,

    /// Name of the dead letter queue for this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    dead_letter: Option<&'a str>,

    #[serde(skip_serializing_if = "Option::is_none")]
    retry_limit: Option<usize>,

    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_delay: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    retry_backoff: Option<bool>,

    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        skip_serializing_if = "Option::is_none"
    )]
    expire_in: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    keep_until: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    start_after: Option<DateTime<Utc>>,

    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        skip_serializing_if = "Option::is_none"
    )]
    singleton_for: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    singleton_key: Option<&'a str>,
}

/// A job to be sent to the server.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Job<'a> {
    /// ID to assign to this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,

    /// Name of the queue to put this job onto.
    pub queue_name: &'a str,

    /// Job's payload.
    pub data: serde_json::Value,

    /// Job's priority.
    ///
    /// Higher numbers will have higher priority
    /// when fetching from the queue.
    pub priority: usize,

    /// Name of the dead letter queue for this job.
    pub dead_letter: Option<&'a str>,

    /// Number of retry attempts.
    ///
    /// If omitted, a value will be taken from queue via [`Queue::retry_limit`]
    /// and - if not set there either - will default to `2` retry attempts.
    pub retry_limit: Option<usize>,

    /// Time to wait before a retry attempt.
    pub retry_delay: Option<Duration>,

    /// Whether to use a backoff between retry attempts.
    pub retry_backoff: Option<bool>,

    /// Time to wait before expiring this job.
    ///
    /// Specifies for how long this job may be in `active` state before
    /// it is failed because of expiration.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default).
    pub expire_in: Option<Duration>,

    /// When this job can be archived.
    ///
    /// Specifies for how long this job may be in `created` or `retry` state before
    /// it is archived.
    ///
    /// Should be greater than or equal to 1 second, or simply unset (default).
    pub keep_until: Option<DateTime<Utc>>,

    /// When this job should become visible to consumers.
    ///
    /// By default, the job will be visible to consumers as soon as
    /// it is registered.
    pub start_after: Option<DateTime<Utc>>,

    /// For how long only one job instance is allowed.
    ///
    /// If you set this to, say, 60s and then submit 2 jobs within the same minute,
    /// only the first job will be registered.
    pub singleton_for: Option<Duration>,

    /// Key to use for throttling.
    ///
    /// Will extend throttling to allow one job per key within the time slot.
    pub singleton_key: Option<&'a str>,
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

    /// This job's [`JobState`].
    pub state: JobState,

    /// [Policy](QueuePolicy) applied to this job.
    ///
    /// This will be `None` for jobs consumed from dead
    /// letter queues.
    pub policy: Option<QueuePolicy>,

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

    /// Date used by the system internally for throttling.
    ///
    /// This is calculated by the system using [`Job::singleton_for`] period.
    /// This is the system's implementation detail and should not be relied on.
    pub singleton_at: Option<NaiveDateTime>,

    /// Key to use for throttling.
    ///
    /// See [`Job::singleton_key`].
    pub singleton_key: Option<String>,

    /// When this job was completed.
    pub completed_at: Option<DateTime<Utc>>,

    /// Name of the dead letter queue for this job, if any.
    pub dead_letter: Option<String>,

    /// When this job can be archived.
    ///
    /// Specifies for how long this job may be in `created` or `retry` state before
    /// it is archived.
    ///
    /// Defalts to two weeks
    pub keep_until: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for JobDetails {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let id: Uuid = row.try_get("id")?;
        let queue_name: String = row.try_get("name")?;
        let dead_letter: Option<String> = row.try_get("dead_letter")?;
        let data: serde_json::Value = row.try_get("data")?;
        let expire_in = row.try_get("expire_in").and_then(|v: f64| match v {
            v if v >= 0.0 => Ok(Duration::from_secs_f64(v)),
            _ => Err(sqlx::Error::ColumnDecode {
                index: "expire_in".to_string(),
                source: "'expire_in' should be non-negative".into(),
            }),
        })?;
        let policy = row
            .try_get("policy")
            .and_then(|v: Option<String>| match v {
                None => Ok(None),
                Some(v) => match QueuePolicy::try_from(v) {
                    Err(e) => Err(sqlx::Error::ColumnDecode {
                        index: "policy".to_string(),
                        source: e.into(),
                    }),
                    Ok(v) => Ok(Some(v)),
                },
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
        let completed_at: Option<DateTime<Utc>> = row.try_get("completed_at")?;
        let start_after: DateTime<Utc> = row.try_get("start_after")?;
        let singleton_at: Option<NaiveDateTime> = row.try_get("singleton_at")?;
        let singleton_key: Option<String> = row.try_get("singleton_key")?;
        let state = row.try_get_raw("state").and_then(|v: PgValueRef| {
            let v = v.as_str().map_err(|e| sqlx::Error::ColumnDecode {
                index: "state".to_string(),
                source: e,
            })?;
            let state =
                JobState::try_from(v.to_string()).map_err(|e| sqlx::Error::ColumnDecode {
                    index: "state".to_string(),
                    source: e.into(),
                })?;
            Ok(state)
        })?;
        let keep_until: DateTime<Utc> = row.try_get("keep_until")?;

        Ok(JobDetails {
            id,
            queue_name,
            dead_letter,
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
            singleton_at,
            singleton_key,
            state,
            completed_at,
            keep_until,
        })
    }
}

impl<'a> Job<'a> {
    /// Creates a builder for a job
    pub fn builder() -> JobBuilder<'a> {
        JobBuilder::default()
    }

    pub(crate) fn opts(&self) -> JobOptions<'_> {
        JobOptions {
            priority: self.priority,
            dead_letter: self.dead_letter,
            retry_limit: self.retry_limit,
            retry_delay: self.retry_delay,
            retry_backoff: self.retry_backoff,
            expire_in: self.expire_in,
            keep_until: self.keep_until,
            start_after: self.start_after,
            singleton_for: self.singleton_for,
            singleton_key: self.singleton_key,
        }
    }
}

/// A builder for a job.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct JobBuilder<'a> {
    pub(crate) id: Option<Uuid>,
    pub(crate) queue_name: &'a str,
    pub(crate) data: serde_json::Value,
    pub(crate) priority: usize,
    pub(crate) dead_letter: Option<&'a str>,
    pub(crate) retry_limit: Option<usize>,
    pub(crate) retry_delay: Option<Duration>,
    pub(crate) retry_backoff: Option<bool>,
    pub(crate) expire_in: Option<Duration>,
    pub(crate) keep_until: Option<DateTime<Utc>>,
    pub(crate) start_after: Option<DateTime<Utc>>,
    pub(crate) singleton_for: Option<Duration>,
    pub(crate) singleton_key: Option<&'a str>,
}

impl<'a> JobBuilder<'a> {
    /// ID to assign to this job.
    pub fn id(mut self, value: Uuid) -> Self {
        self.id = Some(value);
        self
    }

    /// Name of the queue to put this job onto.
    pub fn queue_name(mut self, value: &'a str) -> Self {
        self.queue_name = value;
        self
    }

    /// Job's payload.
    pub fn data(mut self, value: serde_json::Value) -> Self {
        self.data = value;
        self
    }

    /// Job's priority.
    pub fn priority(mut self, value: usize) -> Self {
        self.priority = value;
        self
    }

    /// Name of the dead letter queue for this job.
    pub fn dead_letter(mut self, value: &'a str) -> Self {
        self.dead_letter = Some(value);
        self
    }

    /// Maximum number of retry attempts.
    pub fn retry_limit(mut self, value: usize) -> Self {
        self.retry_limit = Some(value);
        self
    }

    /// Time to wait before a retry attempt.
    pub fn retry_delay(mut self, value: Duration) -> Self {
        self.retry_delay = Some(value);
        self
    }

    /// Whether to use a backoff between retry attempts.
    pub fn retry_backoff(mut self, value: bool) -> Self {
        self.retry_backoff = Some(value);
        self
    }

    /// Time to wait before expiring this job.
    ///
    /// Should be between 1 second and 24 hours, or simply unset (default).
    pub fn expire_in(mut self, value: Duration) -> Self {
        self.expire_in = Some(value);
        self
    }

    /// When this job can be archived.
    pub fn keep_until(mut self, value: DateTime<Utc>) -> Self {
        self.keep_until = Some(value);
        self
    }

    /// For how long this job should be retained in the system.
    ///
    /// Will calculate and set [`JobBuilder::keep_until`].
    pub fn retain_for(mut self, value: Duration) -> Self {
        self.keep_until = Some(Utc::now() + value);
        self
    }

    /// When to make this job 'visible' for consumers.
    pub fn start_after(mut self, value: DateTime<Utc>) -> Self {
        self.start_after = Some(value);
        self
    }

    /// For how long this job should _not_ be visible to consumers.
    ///
    /// A convenience method, that, internally, will set [`JobBuilder::start_after`].
    pub fn delay_for(mut self, value: Duration) -> Self {
        self.start_after = Some(Utc::now() + value);
        self
    }

    /// For how long only one job instance is allowed.
    ///
    /// If you set this to, say, 60s and then submit 2 jobs within the same minute,
    /// only the first job will be registered.
    pub fn singleton_for(mut self, value: Duration) -> Self {
        self.singleton_for = Some(value);
        self
    }

    /// Key to use for throttling.
    ///
    /// Will extend throttling to allow one job per key within the time slot.
    pub fn singleton_key(mut self, value: &'a str) -> Self {
        self.singleton_key = Some(value);
        self
    }

    /// Creates a job.
    pub fn build(self) -> Job<'a> {
        Job {
            id: self.id,
            queue_name: self.queue_name,
            data: self.data,
            priority: self.priority,
            dead_letter: self.dead_letter,
            retry_limit: self.retry_limit,
            retry_delay: self.retry_delay,
            retry_backoff: self.retry_backoff,
            expire_in: self.expire_in,
            keep_until: self.keep_until,
            start_after: self.start_after,
            singleton_for: self.singleton_for,
            singleton_key: self.singleton_key,
        }
    }
}
