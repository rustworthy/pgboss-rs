use crate::utils;
use pgboss::Client;
use uuid::Uuid;

#[tokio::test]
async fn delete_job_queue_does_not_exist() {
    let local = "delete_job_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";

    let _ = c.delete_job(queue_name, job_id).await.unwrap();
}

#[tokio::test]
async fn delete_job() {
    let local = "fetch_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";

    // fetch one
    let _ = c.delete_job(queue_name, job_id).await.unwrap();
}
