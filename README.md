# pgboss-rs

## Queue jobs with Rust and PostgreSQL like a boss.

[![Crates.io](https://img.shields.io/crates/v/pgboss.svg)](https://crates.io/crates/pgboss)
[![Documentation](https://docs.rs/pgboss/badge.svg)](https://docs.rs/pgboss/)
[![Codecov](https://codecov.io/github/rustworthy/pgboss-rs/coverage.svg?branch=main)](https://codecov.io/gh/rustworthy/pgboss-rs)
[![dependency status](https://deps.rs/repo/github/rustworthy/pgboss-rs/status.svg)](https://deps.rs/repo/github/rustworthy/pgboss-rs)

Inspired by, compatible with and partially ported from [`pg-boss`](https://github.com/timgit/pg-boss/tree/master) Node.js package.

Heavily influenced by decisions and approaches in [`faktory-rs`](https://github.com/jonhoo/faktory-rs) crate.

```rust
use std::time::Duration;
use serde_json::json;
use pgboss::{Client, Job, JobState};

// Create a client first.
let c = Client::builder().schema("desired_schema_name").connect().await.unwrap();

// Then create a queue.
c.create_standard_queue("qname").await.unwrap();  // NB! queue should be created before pushing jobs
c.create_standard_queue("qname_dlq").await.unwrap();

// Build a job and ...
let job = Job::builder()
     .queue_name("qname")                           // which queue this job should be sent to
     .data(json!({"key": "value"}))                 // arbitrary json, your job's payload
     .priority(10)                                  // will be consumer prior to those with lower priorities
     .retry_limit(1)                                // only retry this job once
     .dead_letter("qname_dlq")                      // send to this queue when retry limit exceeded
     .retry_delay(Duration::from_secs(60 * 5))      // do not retry immediately after failure
     .expire_in(Duration::from_secs(60 * 5))        // only give the worker 5 minutes to complete the job
     .retain_for(Duration::from_secs(60 * 60 * 24)) // do not archive for at least 1 day
     .delay_for(Duration::from_secs(5))             // make it visible to consumers after 5 seconds
     .singleton_for(Duration::from_secs(7))         // only allow one job for at least 7 seconds
     .singleton_key("buzz")                         // allow more than one job if their key is different from this
     .build();

// ... enqueue it.
let _id = c.send_job(&job).await.expect("no error");

// Consume from the queue.
let fetched_job = c
     .fetch_job("qname")
     .await
     .expect("no error")
     .expect("a job");

assert_eq!(fetched_job.data, job.data);
assert_eq!(fetched_job.state, JobState::Active);

c.complete_job("qname", fetched_job.id, json!({"result": "success!"})).await.expect("no error");
```
