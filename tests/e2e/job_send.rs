use std::time::Duration;

use crate::utils;
use chrono::Utc;
use pgboss::{Client, Error, Job, JobState, QueuePolicy};
use serde_json::json;
use tokio::time;

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    let job = Job::builder().queue_name("jobtype").build();
    let _id = c.send_job(&job).await.expect("no error");
}

#[tokio::test]
async fn send_job_with_id() {
    let local = "send_job_with_id";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let id = uuid::Uuid::new_v4();
    let job = Job::builder().queue_name("jobtype").id(id).build();
    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);

    let job = Job::builder().queue_name("jobtype").id(id).build();
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
        .queue_name("jobtype")
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
        .queue_name("jobtype")
        .id(id)
        .dead_letter("jobtype_dead_letter")
        .build();
    let err = c.send_job(&job).await.unwrap_err();
    if let Error::DoesNotExist { msg } = err {
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
    let job = Job::builder().queue_name("jobtype").build();
    if let Error::DoesNotExist { msg } = c.send_job(&job).await.unwrap_err() {
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

    if let Error::DoesNotExist { msg } = c
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
    // `JobBuilder::retain_for` can do the following for us
    let keep_until = Utc::now() + Duration::from_secs(60 * 60 * 2);
    let job = Job::builder()
        .id(id)
        .queue_name("jobtype")
        .data(json!({"key": "value"}))
        .priority(10)
        .dead_letter("jobtype_dead_letter_queue")
        .retry_limit(5)
        .retry_delay(Duration::from_secs(60 * 5))
        .retry_backoff(true)
        .expire_in(Duration::from_secs(30))
        .keep_until(keep_until)
        .delay_for(Duration::from_secs(5))
        .singleton_for(Duration::from_secs(7))
        .singleton_key("buzz")
        .build();

    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);

    let job_info = c
        .get_job("jobtype", inserted_id)
        .await
        .expect("no error")
        .expect("this job to be present");

    assert_eq!(job_info.data, job.data);
    assert_eq!(job_info.id, job.id.unwrap());
    assert_eq!(job_info.policy.unwrap(), QueuePolicy::Standard);
    // Important bit, we have not _consumed_, rather just got it's details,
    // so the state is not 'active' rather still 'created'.
    assert_eq!(job_info.state, JobState::Created);
    assert_eq!(job_info.queue_name, "jobtype");
    assert_eq!(job_info.retry_count, 0);
    assert!(job_info.created_at < Utc::now());
    assert!(job_info.started_at.is_none());
    // opts
    assert_eq!(job_info.expire_in, job.expire_in.unwrap());
    assert_eq!(job_info.priority, job.priority);
    assert_eq!(job_info.retry_delay, job.retry_delay.unwrap());
    assert_eq!(job_info.retry_limit, job.retry_limit.unwrap());
    assert_eq!(job_info.retry_backoff, job.retry_backoff.unwrap());
    assert!(job_info.singleton_at.is_some());
    assert_eq!(job_info.singleton_key.unwrap(), "buzz");
    assert!(job_info.completed_at.is_none());

    // PostgreSQL will cut nanoseconds
    assert_eq!(
        job_info
            .start_after
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        job.start_after
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    );
    assert_eq!(
        job_info
            .keep_until
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        keep_until.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    );
}

#[tokio::test]
async fn send_jobs_throttled() {
    let local = "send_jobs_throttled";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job1 = Job::builder()
        .queue_name("jobtype")
        .singleton_for(Duration::from_secs(1))
        .build();
    let job2 = Job::builder()
        .queue_name("jobtype")
        .singleton_for(Duration::from_secs(1))
        .build();

    let id1 = c.send_job(&job1).await.expect("no error");
    let err = c.send_job(&job2).await.unwrap_err();
    if let Error::Throttled { msg } = err {
        assert_eq!(msg, "singleton policy applied to jobs with 'singleton_on' property and state not 'cancelled'");
    } else {
        unreachable!()
    }

    time::sleep(Duration::from_secs(1)).await;

    let id2 = c
        .send_job(&job2)
        .await
        .expect("queued this time and ID issued");
    assert_ne!(id1, id2);
}

#[tokio::test]
async fn send_job_dlq_named_as_main_queue() {
    let local = "send_job_dlq_named_as_main_queue";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();
    c.create_standard_queue("jobtype_dlq").await.unwrap();

    let job1 = Job::builder()
        .retry_limit(0)
        .queue_name("jobtype")
        .dead_letter("jobtype")
        .build();

    // but failed job where queue name == dlq name will not get into dlq
    let id1 = c.send_job(&job1).await.expect("no error");

    let fetched_job_1 = c.fetch_job("jobtype").await.unwrap().unwrap();
    assert_eq!(fetched_job_1.id, id1);

    // let's fail job1
    let ok = c
        .fail_job_with_details(
            "jobtype",
            fetched_job_1.id,
            json!({"details": "testing..."}),
        )
        .await
        .unwrap();
    assert!(ok);

    assert!(c.fetch_job("jobtype_dlq").await.unwrap().is_none());

    let job2 = Job::builder()
        .retry_limit(0)
        .queue_name("jobtype")
        .dead_letter("jobtype_dlq")
        .build();
    let id2 = c.send_job(&job2).await.expect("no error");

    let fetched_job_2 = c.fetch_job("jobtype").await.unwrap().unwrap();
    assert_ne!(fetched_job_2.id, id1);
    assert_eq!(fetched_job_2.id, id2);

    // let's fail job2
    let ok = c
        .fail_job_with_details(
            "jobtype",
            fetched_job_2.id,
            json!({"details": "testing again..."}),
        )
        .await
        .unwrap();
    assert!(ok);

    let job2_from_dlq = c.fetch_job("jobtype_dlq").await.unwrap().unwrap();
    assert_eq!(fetched_job_2.dead_letter.unwrap(), job2_from_dlq.queue_name);
    assert_eq!(fetched_job_2.data, job2_from_dlq.data);
    assert_eq!(fetched_job_2.retry_limit, job2_from_dlq.retry_limit);
    // we are efffectively resetting keep_until when writing a job
    // to the `schema_name.job` relation
    assert_ne!(fetched_job_2.keep_until, job2_from_dlq.keep_until);
    // latest output is preserved when sending job to dlq
    assert_eq!(
        json!({"details": "testing again..."}),
        job2_from_dlq.output.unwrap()
    );
}
