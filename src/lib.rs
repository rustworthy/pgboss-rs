//! Queue jobs with Rust and PostgreSQL like a boss.
//!
//! Inspired by, compatible with and partially ported from [`pg-boss`](https://github.com/timgit/pg-boss/tree/master) Node.js package.
//!
//! Heavily influenced by decisions and approaches in [`faktory-rs`](https://github.com/jonhoo/faktory-rs) crate.
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use std::time::Duration;
//! use serde_json::json;
//! use pgboss::{Client, Job, JobState, Queue};
//!
//! // Create a client first.
//! let c = Client::builder().schema("desired_schema_name").connect().await.unwrap();
//!
//! // Then create a dlq (optional) and a queue.
//! c.create_standard_queue("image_processing_dlq").await.unwrap();
//!
//! // NB! queue should be created before pushing jobs
//! let queue = Queue::builder()
//!     .name("image_processing")
//!     .dead_letter("qname_dlq")
//!     .partition(true)
//!     .build();
//! c.create_queue(&queue).await.unwrap();
//!
//! // Build a job and ...
//! let job = Job::builder()
//!     .queue_name("qname")                           // which queue this job should be sent to
//!     .data(json!({"key": "value"}))                 // arbitrary json, your job's payload
//!     .priority(10)                                  // will be consumer prior to those with lower priorities
//!     .retry_limit(1)                                // only retry this job once
//!     .retry_delay(Duration::from_secs(60 * 5))      // do not retry immediately after failure
//!     .expire_in(Duration::from_secs(60 * 5))        // only give the worker 5 minutes to complete the job
//!     .retain_for(Duration::from_secs(60 * 60 * 24)) // do not archive for at least 1 day
//!     .delay_for(Duration::from_secs(5))             // make it visible to consumers after 5 seconds
//!     .singleton_for(Duration::from_secs(7))         // only allow one job for at least 7 seconds
//!     .singleton_key("buzz")                         // allow more than one job if their key is different from this
//!     .build();
//!
//! // ... enqueue it.
//! let _id = c.send_job(&job).await.expect("no error");
//!
//! // Consume from the queue.
//! let fetched_job = c
//!     .fetch_job("qname")
//!     .await
//!     .expect("no error")
//!     .expect("a job");
//!
//! assert_eq!(fetched_job.data, job.data);
//! assert_eq!(fetched_job.state, JobState::Active);
//!
//! c.complete_job("qname", fetched_job.id, json!({"result": "success!"})).await.expect("no error");
//! # });
//! ```
//!
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod error;
mod job;
mod queue;
mod sql;
mod utils;

pub use client::{Client, ClientBuilder, MaintenanceStats};
pub use error::Error;
pub use job::{Job, JobBuilder, JobDetails, JobState};
pub use queue::{Queue, QueueBuilder, QueueDetails, QueuePolicy};

use chrono::{DateTime, Utc};
use sqlx::FromRow;
use std::fmt::Debug;

pub(crate) use job::JobOptions;

pub(crate) const MINIMUM_SUPPORTED_PGBOSS_APP_VERSION: u8 = 26;
pub(crate) const CURRENT_PGBOSS_APP_VERSION: u8 = 26;

#[derive(Debug, Clone, Default, FromRow)]
pub(crate) struct App {
    pub(crate) version: i32,
    pub(crate) cron_on: Option<DateTime<Utc>>,
}
