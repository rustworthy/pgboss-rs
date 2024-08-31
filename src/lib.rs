//! Crate docs
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod error;
mod job;
mod queue;
mod sql;
mod utils;

pub use client::{Client, ClientBuilder};
pub use error::Error;
pub use job::{Job, JobOptions};
pub use queue::{QueueInfo, QueueOptions, QueuePolicy};

use chrono::{DateTime, Utc};
use sqlx::FromRow;
use std::fmt::Debug;

// https://github.com/timgit/pg-boss/blob/4b3d9f4628860bb103f4498161e0ec6d17b55b56/src/contractor.js#L491
pub(crate) const MINIMUM_SUPPORTED_PGBOSS_APP_VERSION: u8 = 23;
pub(crate) const CURRENT_PGBOSS_APP_VERSION: u8 = 23;

#[derive(Debug, Clone, Default, FromRow)]
pub(crate) struct App {
    pub(crate) version: i32,
    pub(crate) maintained_on: Option<DateTime<Utc>>,
    pub(crate) cron_on: Option<DateTime<Utc>>,
}
