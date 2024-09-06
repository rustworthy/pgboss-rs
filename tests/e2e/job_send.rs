use std::time::Duration;

use crate::utils;
use pgboss::{Client, Error, Job};
use serde_json::json;

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    let job = Job::builder().name("jobtype").build();
    let _id = c.send_job(&job).await.expect("no error");
}

#[tokio::test]
async fn send_job_with_id() {
    let local = "send_job_with_id";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let id = uuid::Uuid::new_v4();
    let job = Job::builder().name("jobtype").id(id).build();
    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);

    let job = Job::builder().name("jobtype").id(id).build();
    let err = c.send_job(&job).await.unwrap_err();
    if let Error::Conflict { msg } = err {
        assert_eq!(msg, "job with this id already exists");
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send_job_with_dead_letter() {
    let local = "send_job_with_dead_letter";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    c.create_standard_queue("jobtype_dead_letter_queue")
        .await
        .unwrap();

    let id = uuid::Uuid::new_v4();
    let job = Job::builder()
        .name("jobtype")
        .id(id)
        .dead_letter("jobtype_dead_letter_queue")
        .build();

    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);
}

#[tokio::test]
async fn send_job_with_dead_letter_does_not_exist() {
    let local = "send_job_with_dead_letter_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let id = uuid::Uuid::new_v4();
    let job = Job::builder()
        .name("jobtype")
        .id(id)
        .dead_letter("jobtype_dead_letter")
        .build();
    let err = c.send_job(&job).await.unwrap_err();
    if let Error::Unprocessable { msg } = err {
        assert_eq!(msg, "dead letter queue does not exist");
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send_job_queue_does_not_exist() {
    let local = "send_job_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    let job = Job::builder().name("jobtype").build();
    if let Error::Unprocessable { msg } = c.send_job(&job).await.unwrap_err() {
        assert!(msg.contains("queue does not exist"))
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send_data() {
    let local = "send_data";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    let data = serde_json::json!({"key": "value"});
    let _id = c.send_data("jobtype", &data).await.expect("no error");
}

#[tokio::test]
async fn send_data_queue_does_not_exist() {
    let local = "send_data_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    if let Error::Unprocessable { msg } = c
        .send_data("jobtype", serde_json::json!({"key": "value"}))
        .await
        .unwrap_err()
    {
        assert!(msg.contains("queue does not exist"))
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send_job_fully_customized() {
    let local = "send_job_fully_customized";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    c.create_standard_queue("jobtype_dead_letter_queue")
        .await
        .unwrap();

    let id = uuid::Uuid::new_v4();

    let job = Job::builder()
        .id(id)
        .name("jobtype")
        .data(json!({"key": "value"}))
        .priority(10)
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .expire_in(Duration::from_secs(30))
        .retain_for(Duration::from_secs(60 * 60 * 2))
        .delay_for(Duration::from_secs(5))
        .build();

    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);
}
