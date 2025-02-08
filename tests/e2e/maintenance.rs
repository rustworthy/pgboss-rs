use crate::utils;
use pgboss::{Client, Job, JobState};
use std::time::Duration;
use tokio::time;

#[tokio::test]
async fn force_maintain_check_expiration() {
    let sname = "force_maintain_check_expiration_schema";
    let qname = "force_maintain_check_expiration_queue";
    let timeout = Duration::from_secs(1);

    utils::drop_schema(&sname).await.unwrap();

    let c = Client::builder().schema(sname).connect().await.unwrap();
    c.create_standard_queue(qname).await.unwrap();

    // a job that can only be executed for 1 seconds _once consumed_,
    // i.e. when its stated changed to "active"
    let job = Job::builder()
        .queue_name(&qname)
        .retry_limit(0)
        .expire_in(timeout)
        .build();

    let jid = c.send_job(&job).await.expect("queued ok");

    // no expired jobs
    let maintain_stats = c.force_maintain().await.expect("no errors");
    assert_eq!(maintain_stats.expired, 0);

    let job = c
        .fetch_job(qname)
        .await
        .expect("no error when fetching and ...")
        .expect("... our one single job to be there");
    assert_eq!(jid, job.id);

    // out job has been consumed but is not expired just yet
    let maintain_stats = c.force_maintain().await.expect("no errors");
    assert_eq!(maintain_stats.expired, 0);

    let job_detail = c
        .get_job(qname, jid)
        .await
        .expect("no error and ...")
        .expect("... our job for sure");
    assert_eq!(job_detail.state, JobState::Active);

    // the job _should_ be expired now, but ...
    time::sleep(2 * timeout).await;

    // ... we will _not_ learn about this unless the maintence is
    // performed, which is normally happenning at the background, but
    // we can force it
    assert_eq!(
        c.get_job(qname, jid)
            .await
            .expect("no error and ...")
            .expect("... our job for sure")
            .state,
        JobState::Active // still active thought time is exceeded
    );

    // let's force maintenance
    let maintain_stats = c.force_maintain().await.expect("no errors");
    assert_eq!(maintain_stats.expired, 1);
    assert_eq!(maintain_stats.archived, 0);
    // just a sanity check
    assert_eq!(
        c.get_job(qname, jid)
            .await
            .expect("no error and ...")
            .expect("... our job is still there, it's just that it is failed now")
            .state,
        JobState::Failed // no longer "active"
    );
}
