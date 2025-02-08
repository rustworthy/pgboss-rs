use crate::{Client, Error};
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct MaintenanceStats {
    pub expired: u32,
}

impl Client {
    /// Force maintenance operations.
    ///
    /// This will force operations that are normally performed on a schedule,
    /// such as expiration, archival, and dropping, returning [`MaintenanceStats`].
    pub async fn force_maintain(&self) -> Result<MaintenanceStats, Error> {
        let expired_count: (i64,) = sqlx::query_as(&self.stmt.fail_jobs_by_timeout)
            .fetch_one(&self.pool)
            .await?;
        Ok(MaintenanceStats {
            expired: expired_count.0 as u32,
        })
    }
}
