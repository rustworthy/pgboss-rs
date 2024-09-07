use std::time::Duration;

use crate::utils;
use pgboss::{Client, Job};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn fetch_one_job() {
    let local = "fetch_one_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    c.create_standard_queue("jobtype_dead_letter_queue")
        .await
        .unwrap();

    // prepare jobs
    let job1 = Job::builder()
        .queue_name("jobtype")
        .data(json!({"key": "value1"}))
        .priority(10) // should be fetched THIRD
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .expire_in(Duration::from_secs(30))
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .build();

    let job2 = Job::builder()
        .queue_name("jobtype")
        .data(json!({"key": "value2"}))
        .priority(20) // should be fetched FIRST
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .build();

    let job3 = Job::builder()
        .queue_name("jobtype")
        .data(json!({"key": "value3"}))
        .priority(15) // should be fetched SECOND
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .build();

    // send jobs
    c.send_job(&job1).await.expect("uuid");
    c.send_job(&job2).await.expect("uuid");
    c.send_job(&job3).await.expect("uuid");

    // fetch one
    let job = c
        .fetch_job("jobtype")
        .await
        .expect("no error")
        .expect("a job");

    assert_eq!(job.queue_name, "jobtype");
    assert_eq!(job.data, json!({"key": "value2"}));
    assert_eq!(job.expire_in, Duration::from_secs(60 * 15)); // default

    // fetch one
    let job = c
        .fetch_job("jobtype")
        .await
        .expect("no error")
        .expect("a job");

    assert_eq!(job.queue_name, "jobtype");
    assert_eq!(job.data, json!({"key": "value3"}));
    assert_eq!(job.expire_in, Duration::from_secs(60 * 15)); // default

    // fetch the last one
    let job = c
        .fetch_job("jobtype")
        .await
        .expect("no error")
        .expect("a job");

    assert_eq!(job.queue_name, "jobtype");
    assert_eq!(job.data, json!({"key": "value1"}));
    assert_eq!(job.expire_in, Duration::from_secs(30)); // our override

    // queue has been drained!
    assert!(c.fetch_job("jobtype").await.expect("no error").is_none());
}

#[tokio::test]
async fn fetch_many_jobs() {
    let local = "fetch_many_jobs";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    // prepare jobs
    let job1_id = Uuid::new_v4();
    let job1 = Job::builder()
        .id(job1_id)
        .queue_name("jobtype")
        .data(json!({"key": "value1", "priority": 1}))
        .priority(1)
        .build();

    let job2_id = Uuid::new_v4();
    let job2 = Job::builder()
        .id(job2_id)
        .queue_name("jobtype")
        .data(json!({"key": "value2", "priority": 0}))
        .build();

    let job3_id = Uuid::new_v4();
    let job3 = Job::builder()
        .id(job3_id)
        .queue_name("jobtype")
        .data(json!({"key": "value3", "priority": 10}))
        .priority(10)
        .build();

    // send jobs
    c.send_job(&job1).await.expect("uuid");
    c.send_job(&job2).await.expect("uuid");
    c.send_job(&job3).await.expect("uuid");

    // fetch a batch
    let jobs = c.fetch_jobs("jobtype", 2).await.expect("no error");

    assert_eq!(jobs.len(), 2);

    // fetch the last one
    let job = c
        .fetch_job("jobtype")
        .await
        .expect("no error")
        .expect("a job");

    assert_eq!(job.queue_name, "jobtype");
    assert_eq!(job.data, json!({"key": "value2", "priority": 0}));

    // queue has been drained!
    assert!(c.fetch_job("jobtype").await.expect("no error").is_none());
}
