mod app;
mod client;
mod job;
mod stmt;
mod utils;

pub use client::{Client, ClientBuilder};

// See the upcoming PgBoss v10:
// https://github.com/timgit/pg-boss/blob/08c8d4b84a6adf7888b77adc2935eb6839a937b7/src/contractor.js#L49C55-L49C61
pub(crate) const MINIMUM_SUPPORTED_PGBOSS_DDL_REVISION: i32 = 21;
