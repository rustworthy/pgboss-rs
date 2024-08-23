mod app;
mod client;
mod job;
mod queue;
mod sql;
mod utils;

pub use client::{Client, ClientBuilder};
pub use queue::QueueOptions;

// See the PgBoss v10 package:
// https://github.com/timgit/pg-boss/blob/4b3d9f4628860bb103f4498161e0ec6d17b55b56/src/contractor.js#L491
pub(crate) const MINIMUM_SUPPORTED_PGBOSS_APP_VERSION: u8 = 21;
pub(crate) const CURRENT_PGBOSS_APP_VERSION: u8 = 21;
