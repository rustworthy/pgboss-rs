use crate::utils;
use pgboss::{Client, Job};
use uuid::Uuid;

#[tokio::test]
async fn delete_job_queue_does_not_exist() {
    let local = "delete_job_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";

    let deleted = c.delete_job(queue_name, job_id).await.unwrap();
    assert!(!deleted)
}

#[tokio::test]
async fn delete_job_does_not_exist() {
    let local = "delete_job_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";

    let deleted = c.delete_job(queue_name, job_id).await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn delete_job() {
    let local = "delete_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";
    let job = Job::builder().id(job_id).queue_name(queue_name).build();

    c.send_job(job).await.unwrap();

    let deleted = c.delete_job(queue_name, job_id).await.unwrap();
    assert!(deleted);

    assert!(c.fetch_job(queue_name).await.unwrap().is_none());
}
