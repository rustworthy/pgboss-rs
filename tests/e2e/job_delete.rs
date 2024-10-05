use crate::utils::{self, prepare};
use pgboss::{Client, Job};
use uuid::Uuid;

#[tokio::test]
async fn delete_job_queue_does_not_exist() {
    let schema_name = "delete_job_queue_does_not_exist";
    let qname = "jobtype";
    utils::drop_schema(&schema_name).await.unwrap();

    let c = Client::builder()
        .schema(schema_name)
        .connect()
        .await
        .unwrap();
    let deleted = c.delete_job(qname, Uuid::new_v4()).await.unwrap();
    assert!(!deleted)
}

#[tokio::test]
async fn delete_job_does_not_exist() {
    let qname = "jobtype";
    let c = prepare("delete_job_does_not_exist", qname).await;
    let deleted = c.delete_job(qname, Uuid::new_v4()).await.unwrap();
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

#[tokio::test]
async fn delete_jobs() {
    let local = "delete_jobs";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let queue_name = "jobtype";

    let job1_id = Uuid::new_v4();
    let job1 = Job::builder()
        .id(job1_id)
        .queue_name(queue_name)
        .priority(50)
        .build();

    let job2_id = Uuid::new_v4();
    let job2 = Job::builder()
        .id(job2_id)
        .queue_name(queue_name)
        .priority(51)
        .build();

    let job3_id = Uuid::new_v4();
    let job3 = Job::builder()
        .id(job3_id)
        .queue_name(queue_name)
        .priority(0)
        .build();

    c.send_job(job1).await.unwrap();
    c.send_job(job2).await.unwrap();
    c.send_job(job3).await.unwrap();

    // deleting the high priority jobs
    let deleted_count = c.delete_jobs(queue_name, [job1_id, job2_id]).await.unwrap();

    assert_eq!(deleted_count, 2);

    let j = c
        .fetch_job(queue_name)
        .await
        .unwrap()
        .expect("the only remaining job");
    assert_eq!(j.id, job3_id);

    assert!(c.fetch_job(queue_name).await.unwrap().is_none());

    let deleted = c.delete_job(queue_name, j.id).await.unwrap();
    assert!(deleted);
}

#[tokio::test]
async fn delete_jobs_queue_does_not_exist() {
    let local = "delete_jobs_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    let deleted = c.delete_jobs("jobtype", [Uuid::new_v4()]).await.unwrap();
    assert_eq!(deleted, 0)
}

#[tokio::test]
async fn delete_jobs_do_not_exist() {
    let local = "delete_jobs_do_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let deleted = c.delete_jobs("jobtype", [Uuid::new_v4()]).await.unwrap();
    assert_eq!(deleted, 0);
}
