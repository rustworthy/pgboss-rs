use crate::utils;
use pgboss::{Client, Job};

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    assert!(c.send(Job {}).await.expect("no error").is_some());
}
