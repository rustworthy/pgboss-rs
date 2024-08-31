use super::utils;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
    pub retry_limit: Option<usize>,

    /// Time to wait before a retry attempt.
    #[serde(serialize_with = "utils::serialize_duration_as_secs")]
    pub retry_delay: Option<Duration>,

    /// Whether to use a backoff between retry attempts.
    pub retry_backoff: Option<bool>,
}

/// A job to be sent to the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Job's name.
    pub name: String,

    /// Job's payload.
    pub data: serde_json::Value,

    /// Options specific to this job.
    pub opts: JobOptions,
}
