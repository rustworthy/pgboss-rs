use crate::utils;
use pgboss::{Client, Job};

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue(local).await.unwrap();
    assert!(c.send_job(Job {}).await.expect("no error").is_some());
}

#[tokio::test]
async fn create_and_send_job() {
    let local = "create_and_send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    let _id = c
        .send("jobtype", serde_json::json!({"key": "value"}))
        .await
        .expect("no error")
        .expect("uuid");
}
