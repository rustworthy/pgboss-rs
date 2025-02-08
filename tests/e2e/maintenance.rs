use crate::utils;
use pgboss::{Client, Job};
use std::time::Duration;

#[tokio::test]
async fn force_maintain_check_expiration() {
    let sname = "force_maintain_check_expiration_schema";
    let qname = "force_maintain_check_expiration_queue";
    let dlqname = "force_maintain_check_expiration_dlq";
    utils::drop_schema(&sname).await.unwrap();

    let c = Client::builder().schema(sname).connect().await.unwrap();
    c.create_standard_queue(qname).await.unwrap();
    c.create_standard_queue(dlqname).await.unwrap();

    // a job that can only be executed for 1 seconds _once consumed_,
    // i.e. when its stated changed to "active"
    let job = Job::builder()
        .queue_name(&qname)
        .dead_letter(&dlqname)
        .retry_limit(0)
        .expire_in(Duration::from_secs(1))
        .build();

    let inserted_id = c.send_job(&job).await.expect("queued ok");
    let job = c
        .fetch_job(qname)
        .await
        .expect("no error when fetching and ...")
        .expect("... our one single job to be there");
    assert_eq!(inserted_id, job.id)
}
