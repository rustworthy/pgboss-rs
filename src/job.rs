use super::utils;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

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

    /// Job's name.
    pub name: String,

    /// Job's payload.
    pub data: serde_json::Value,

    /// Options specific to this job.
    pub opts: JobOptions,
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
    /// ID to assign to this job.
    pub(crate) id: Option<Uuid>,

    /// Job's name.
    pub(crate) name: String,

    /// Job's payload.
    pub(crate) data: serde_json::Value,

    /// Options specific to this job.
    pub(crate) opts: JobOptions,
}

impl JobBuilder {
    /// ID to assign to this job.
    pub fn id(mut self, value: Uuid) -> Self {
        self.id = Some(value);
        self
    }

    /// Job's name.
    pub fn name<S>(mut self, value: S) -> Self
    where
        S: Into<String>,
    {
        self.name = value.into();
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
            name: self.name,
            data: self.data,
            opts: self.opts,
        }
    }
}
