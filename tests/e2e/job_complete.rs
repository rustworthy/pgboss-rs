use crate::utils;
use pgboss::{Client, Job};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn complete_job() {
    let local = "complete_job";
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

    let marked_as_completed = c
        .complete_job(queue_name, job_id, json!({"result": "success!"}))
        .await
        .unwrap();
    assert!(marked_as_completed);

    // job transitioned from `active` to `completed`
    // and so the qeueu was drained
    assert!(c.fetch_job(queue_name).await.unwrap().is_none());
}
