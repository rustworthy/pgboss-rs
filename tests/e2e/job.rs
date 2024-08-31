use crate::utils;
use pgboss::{Client, Error, Job, JobOptions};

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job = Job {
        name: "jobtype".into(),
        data: serde_json::Value::Null,
        opts: JobOptions {},
    };
    let _id = c.send_job(job).await.expect("no error");
}

#[tokio::test]
async fn send_job_queue_does_not_exist() {
    let local = "send_job_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    let job = Job {
        name: "jobtype".into(),
        data: serde_json::Value::Null,
        opts: JobOptions {},
    };

    if let Error::Application { msg } = c.send_job(job).await.unwrap_err() {
        assert!(msg.contains("queue does not exist"))
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send() {
    let local = "send";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    let _id = c
        .send("jobtype", serde_json::json!({"key": "value"}))
        .await
        .expect("no error");
}

#[tokio::test]
async fn send_queue_does_not_exist() {
    let local = "send_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    if let Error::Application { msg } = c
        .send("jobtype", serde_json::json!({"key": "value"}))
        .await
        .unwrap_err()
    {
        assert!(msg.contains("queue does not exist"))
    } else {
        unreachable!()
    }
}
