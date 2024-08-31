use thiserror::Error;

/// Enumerates all errors that this crate may return.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Sqlx PostgreSQL driver error.
    #[error("db driver error")]
    Sqlx(#[from] sqlx::Error),

    /// Application error.
    #[error("application error: {msg}")]
    Application {
        /// Details on what exactly went wrong.
        msg: &'static str,
    },
}
