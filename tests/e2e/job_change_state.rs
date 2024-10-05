use crate::utils::prepare;
use chrono::Utc;
use pgboss::{Job, JobState};
use serde_json::json;

#[tokio::test]
async fn complete_job() {
    let qname = "jobtype";
    let c = prepare("complete_job", qname).await;

    let job = Job::builder().queue_name(qname).retry_limit(0).build();
    let id = c.send_job(job).await.unwrap();

    // fetch a job, making it transition from `created` to `active`
    let _job = c.fetch_job(qname).await.unwrap().unwrap();

    // mark as completed
    let before_completed = Utc::now();
    assert!(c
        .complete_job(qname, id, json!({"result": "success!"}))
        .await
        .unwrap());

    // job transitioned from `active` to `completed` and so the qeueu was drained
    assert!(c.fetch_job(qname).await.unwrap().is_none());

    let job_details = c.get_job(qname, id).await.unwrap().unwrap();
    assert!(job_details.completed_at.unwrap() > before_completed);
    assert!(job_details.completed_at.unwrap() < Utc::now());
}

#[tokio::test]
async fn cancel_and_resume_job() {
    let qname = "jobtype";
    let c = prepare("cancel_and_resume_job", qname).await;

    // create and send a job, then get it (but do not consume just yet)
    let id = c.send_data(qname, json!({"job": "date"})).await.unwrap();
    let job_details = c.get_job(qname, id).await.unwrap().unwrap();
    assert_eq!(job_details.state, JobState::Created);

    // now, consume the job and  ...
    let consumed_job = c.fetch_job(qname).await.unwrap().unwrap();
    assert_eq!(consumed_job.state, JobState::Active);

    // ... cancel it, then check state
    assert!(c.cancel_job(qname, id).await.unwrap());
    let job_details = c.get_job(qname, id).await.unwrap().unwrap();
    assert_eq!(job_details.state, JobState::Cancelled);

    // ... resume and then check state yet again
    assert!(c.resume_job(qname, id).await.unwrap());
    let job_details = c.get_job(qname, id).await.unwrap().unwrap();
    assert_eq!(job_details.state, JobState::Created);
}

#[tokio::test]
async fn fail_job_without_retry() {
    let qname = "jobtype";
    let c = prepare("fail_job_without_retry", qname).await;

    // create and send a job where `retry_limit` is set to `0`
    let job = Job::builder().queue_name(qname).retry_limit(0).build();
    let id = c.send_job(job).await.unwrap();

    // fetch a job, making it transition from `created` to `active`
    let job = c.fetch_job(qname).await.unwrap().unwrap();
    assert_eq!(job.id, id);

    // fail it
    assert!(c.fail_job(qname, id).await.unwrap());

    // job transitioned from `created` directly to `failed`
    // and so will no longer be visible to consumers
    assert!(c.fetch_job(qname).await.unwrap().is_none());
}

#[tokio::test]
async fn fail_job_with_retry() {
    let qname = "jobtype";
    let c = prepare("fail_job_with_retry", qname).await;

    let job1 = Job::builder()
        .queue_name(qname)
        .retry_limit(1) // NB
        .build();
    let job_id1 = c.send_job(job1).await.unwrap();

    // fetch a job and fail it
    let job = c.fetch_job(qname).await.unwrap().unwrap();
    assert_eq!(job.id, job_id1);
    assert!(c.fail_job(qname, job_id1).await.unwrap());

    // `retry_count` should be `0` since the job was failed
    // but not retried just yet
    let job_info = c.get_job("jobtype", job_id1).await.unwrap().unwrap();
    assert_eq!(job_info.retry_count, 0);
    assert!(job.started_at.unwrap() < Utc::now());

    // fetch a job again and fail it again
    let job = c.fetch_job(qname).await.unwrap().unwrap();
    assert_eq!(job.id, job_id1); // first job retried
    assert!(c.fail_job(qname, job_id1).await.unwrap());

    // let's revify that `retry_count` has been updated:
    let job_info = c.get_job("jobtype", job.id).await.unwrap().unwrap();
    assert_eq!(job_info.retry_count, 1);
    assert_eq!(job_info.state, JobState::Failed);
}
