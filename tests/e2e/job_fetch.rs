use std::time::Duration;

use crate::utils;
use pgboss::{Client, Job};
use serde_json::json;

#[tokio::test]
async fn fetch_one_job() {
    let local = "fetch_one_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    c.create_standard_queue("jobtype_dead_letter_queue")
        .await
        .unwrap();

    let job1 = Job::builder()
        .name("jobtype")
        .data(json!({"key": "value1"}))
        .priority(10)
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .expire_in(Duration::from_secs(30))
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .build();

    let job2 = Job::builder()
        .name("jobtype")
        .data(json!({"key": "value2"}))
        .priority(20)
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .expire_in(Duration::from_secs(30))
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .build();

    c.send_job(&job1).await.expect("uuid");
    c.send_job(&job2).await.expect("uuid");

    let _ = c.fetch_one("jobtype").await.unwrap();
}
