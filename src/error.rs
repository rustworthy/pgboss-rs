use thiserror::Error;

/// Enumerates all errors that this crate may return.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Sqlx PostgreSQL driver error.
    #[error("db driver error")]
    Sqlx(#[from] sqlx::Error),

    /// Specified queue is not registered in the system.
    #[error("cannot process: {msg}")]
    DoesNotExist {
        /// Further details.
        msg: &'static str,
    },

    /// Conflict in the system.
    ///
    /// Most likely, an a job with this ID already exists
    /// in the system and so operation fails.
    #[error("conflict in the system: {msg}")]
    Conflict {
        /// Details on what exactly went wrong.
        msg: &'static str,
    },

    /// The job has been throttled by the system.
    #[error("throttled: {msg}")]
    Throttled {
        /// Which throttling has been applied.
        msg: &'static str,
    },
}
