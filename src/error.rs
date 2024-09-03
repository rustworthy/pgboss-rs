use thiserror::Error;

/// Enumerates all errors that this crate may return.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Sqlx PostgreSQL driver error.
    #[error("db driver error")]
    Sqlx(#[from] sqlx::Error),

    /// Application error.
    #[error("cannot process: {msg}")]
    Unprocessable {
        /// Details on what exactly went wrong.
        msg: &'static str,
    },

    /// Constraint violation in the system.
    #[error("conflict in the system: {msg}")]
    Conflict {
        /// Details on what exactly went wrong.
        msg: &'static str,
    },
}
