use serde::{Deserialize, Serialize};

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
