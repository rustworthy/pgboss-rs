use std::fmt::Debug;

use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, Default, FromRow)]
pub(crate) struct App {
    pub(crate) version: i32,
    pub(crate) maintained_on: Option<DateTime<Utc>>,
    pub(crate) cron_on: Option<DateTime<Utc>>,
}
