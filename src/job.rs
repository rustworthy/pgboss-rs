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
        skip_serializing_if = "Option::is_none"
    )]
    pub expire_in: Option<Duration>,

    /// For how long this job should be retained in the system.
    ///
    /// Should be greater than or equal to 1 second, or simply unset (default).
    #[serde(
        serialize_with = "utils::serialize_duration_as_secs",
        rename = "keep_until",
        skip_serializing_if = "Option::is_none"
    )]
    pub retain_for: Option<Duration>,
    // startAfter // Datetime UTC
    // singletonSeconds
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
