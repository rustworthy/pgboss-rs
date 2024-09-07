use crate::utils;
use pgboss::{Client, Job};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn fail_job_without_retry() {
    let local = "fail_job_without_fetching";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job_id = Uuid::new_v4();
    let queue_name = "jobtype";
    let job = Job::builder()
        .id(job_id)
        .queue_name(queue_name)
        .retry_limit(0)
        .build();
    c.send_job(job).await.unwrap();

    // fetch a job, making it transition
    // from `created` to `active`
    let job = c
        .fetch_job(queue_name)
        .await
        .expect("no error")
        .expect("one job");
    assert_eq!(job.id, job_id);

    let marked_as_failed = c
        .fail_job(queue_name, job_id, json!({"reason": "for demo purposes"}))
        .await
        .unwrap();
    assert!(marked_as_failed);

    // job transitioned from `created` directly to `failed`
    assert!(c.fetch_job(queue_name).await.unwrap().is_none());
}

#[tokio::test]
async fn fail_job_with_retry() {
    let local = "fail_job_with_retry";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job1_id = Uuid::new_v4();
    let queue_name = "jobtype";
    let job1 = Job::builder()
        .id(job1_id)
        .queue_name(queue_name)
        .retry_limit(1) // NB
        .build();
    c.send_job(job1).await.unwrap();

    let job2_id = Uuid::new_v4();
    let job2 = Job::builder()
        .id(job2_id)
        .queue_name(queue_name)
        .retry_limit(0) // NB
        .build();
    c.send_job(job2).await.unwrap();

    // fetch a job
    let job = c
        .fetch_job(queue_name)
        .await
        .expect("no error")
        .expect("one job");
    assert_eq!(job.id, job1_id);

    let marked_as_failed = c
        .fail_job(queue_name, job1_id, json!({"reason": "for demo purposes"}))
        .await
        .unwrap();
    assert!(marked_as_failed);

    // fetch a job again
    let job = c
        .fetch_job(queue_name)
        .await
        .expect("no error")
        .expect("one job");
    assert_eq!(job.id, job1_id); // retry!

    let marked_as_failed = c
        .fail_job(queue_name, job1_id, json!({"reason": "for demo purposes"}))
        .await
        .unwrap();
    assert!(marked_as_failed);

    // fetch a job
    let job = c
        .fetch_job(queue_name)
        .await
        .expect("no error")
        .expect("one job");
    assert_eq!(job.id, job2_id); // now job2's turn

    let marked_as_failed = c
        .fail_job(queue_name, job2_id, json!({"reason": "for demo purposes"}))
        .await
        .unwrap();
    assert!(marked_as_failed);

    // no retry for job2
    assert!(c.fetch_job(queue_name).await.unwrap().is_none());
}
