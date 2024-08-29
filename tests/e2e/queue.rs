use std::time::Duration;

use crate::utils::{self, POSRGRES_URL};
use chrono::Utc;
use pgboss::{Client, Error, QueueOptions, QueuePolicy};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn simple_connect() {
    utils::drop_schema("pgboss").await.unwrap();
    // This will crate `pgboss` schema, which is does not
    // allow us to isolate tests properly, so we only use it
    // once in this test - sanity check.
    //
    // We are also leaving it behind to able to inspect the db with psql.
    Client::connect().await.unwrap();
    Client::connect_to(POSRGRES_URL.as_str()).await.unwrap();
}

#[tokio::test]
async fn connect_to() {
    let local = "connect_to";
    let _c = Client::builder()
        .schema(local)
        .connect_to(POSRGRES_URL.as_str())
        .await
        .unwrap();
    utils::drop_schema(local).await.unwrap();
}

// On CI - when running on Ubuntu with our postgres service with TLS enabled - use '--include-ignored'
// to run this test, just like we do with `make test` and `make test/cov`
#[ignore = "this test requires a dedicated test run aganst PostgreSQL server with TLS enabled"]
#[tokio::test]
async fn bring_your_own_pool() {
    let local = "bring_your_own_pool";
    let url = format!("{}?sslmode=require", POSRGRES_URL.as_str());
    let p = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let _c = Client::builder().schema(local).with_pool(p).await.unwrap();
    utils::drop_schema(local).await.unwrap();
}

#[tokio::test]
async fn instantiated_idempotently() {
    let local = "instantiated_idempotently";

    // as if N containers from a ReplicaSet are performing bootstrapping
    let mut js = tokio::task::JoinSet::new();
    for _ in 0..20 {
        js.spawn(async move {
            Client::builder().schema(local).connect().await.unwrap();
        });
    }

    // wait for all the tasks to complete
    while let Some(res) = js.join_next().await {
        res.unwrap()
    }

    utils::drop_schema(local).await.unwrap();
}

#[tokio::test]
async fn v21_app_already_exists() {
    let local = "v21_app_already_exists";

    let create_schema_stmt = format!("CREATE SCHEMA {local};");
    let create_version_table_stmt = format!(
        "
        CREATE TABLE {local}.version (
            version int primary key,
            maintained_on timestamp with time zone,
            cron_on timestamp with time zone
        );
        "
    );
    let insert_app_stmt = format!(
        "INSERT INTO {local}.version VALUES ('{}', '{}','{}')",
        21,
        Utc::now(),
        Utc::now()
    );

    utils::ad_hoc_sql([
        create_schema_stmt,
        create_version_table_stmt,
        insert_app_stmt,
    ])
    .await
    .unwrap();

    let _c = Client::builder().schema(local).connect().await.unwrap();
    utils::drop_schema(local).await.unwrap();
}

#[tokio::test]
#[should_panic(expected = "Cannot migrate from the currently installed PgBoss application.")]
async fn less_than_v21_app_already_exists() {
    let local = "less_than_v21_app_already_exists";
    utils::drop_schema(local).await.unwrap();

    let create_schema_stmt = format!("CREATE SCHEMA {local};");
    let create_version_table_stmt = format!(
        "
        CREATE TABLE {local}.version (
            version int primary key,
            maintained_on timestamp with time zone,
            cron_on timestamp with time zone
        );
        "
    );
    let insert_app_stmt = format!(
        "INSERT INTO {local}.version VALUES ('{}', '{}','{}')",
        20,
        Utc::now(),
        Utc::now()
    );

    utils::ad_hoc_sql([
        create_schema_stmt,
        create_version_table_stmt,
        insert_app_stmt,
    ])
    .await
    .unwrap();

    let _c = Client::builder().schema(local).connect().await.unwrap();
}

#[tokio::test]
async fn create_standard_queue() {
    let local = "create_standard_queue";
    utils::drop_schema(local).await.unwrap();

    let client = Client::builder().schema(local).connect().await.unwrap();
    client.create_standard_queue("job_type").await.unwrap();

    let q = client
        .get_queue("job_type")
        .await
        .expect("no error")
        .expect("queue info");

    assert_eq!(q.name, "job_type");
    assert_eq!(q.policy, QueuePolicy::Standard);
    assert_eq!(q.retry_limit, None);
    assert_eq!(q.retry_delay, None);
    assert_eq!(q.retry_backoff, None);
    assert_eq!(q.expire_in, None);
    assert_eq!(q.retain_for, None);
    assert_eq!(q.dead_letter, None);
}

#[tokio::test]
async fn create_queue_already_exists() {
    let local = "create_queue_already_exists";
    utils::drop_schema(local).await.unwrap();

    let client = Client::builder().schema(local).connect().await.unwrap();
    client.create_standard_queue("job_type").await.unwrap();

    if let Error::Sqlx(e) = client.create_standard_queue("job_type").await.unwrap_err() {
        assert_eq!(
            e.into_database_error().unwrap().constraint().unwrap(),
            "queue_pkey"
        );
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn create_non_standard_queue() {
    let local = "create_non_standard_queue";
    utils::drop_schema(local).await.unwrap();

    let client = Client::builder().schema(local).connect().await.unwrap();
    let dlq_opts = QueueOptions {
        name: "image_processing_dlq",
        ..Default::default()
    };
    client.create_queue(&dlq_opts).await.unwrap();

    let queue_opts = QueueOptions {
        name: "image_processing",
        policy: QueuePolicy::Singleton,
        retry_limit: Some(3),
        retry_delay: Some(Duration::from_secs(10)),
        retry_backoff: Some(true),
        expire_in: Some(Duration::from_secs(60 * 60)),
        retain_for: Some(Duration::from_secs(60 * 60 * 24)),
        dead_letter: Some(&dlq_opts.name),
    };
    client.create_queue(&queue_opts).await.unwrap();

    let queues = client.get_queues().await.unwrap();
    assert_eq!(queues.len(), 2); // queue + dlq

    let q = queues
        .iter()
        .find(|&q| q.name == "image_processing")
        .unwrap();
    assert_eq!(q.name, "image_processing");
    assert_eq!(q.policy, QueuePolicy::Singleton);
    assert_eq!(q.retry_limit.unwrap(), 3);
    assert_eq!(q.retry_delay.unwrap(), Duration::from_secs(10));
    assert_eq!(q.retry_backoff.unwrap(), true);
    assert_eq!(q.expire_in.unwrap(), Duration::from_secs(60 * 60));
    assert_eq!(q.retain_for.unwrap(), Duration::from_secs(60 * 60 * 24));
    assert_eq!(q.dead_letter.as_ref().unwrap(), dlq_opts.name);
}

#[tokio::test]
async fn delete_queue() {
    let local = "delete_queue";
    utils::drop_schema(local).await.unwrap();

    let client = Client::builder().schema(local).connect().await.unwrap();

    client.create_standard_queue("job_type_1").await.unwrap();

    client.create_standard_queue("job_type_2").await.unwrap();

    assert!(client
        .get_queue("job_type_1")
        .await
        .expect("no error")
        .is_some());

    assert!(client
        .get_queue("job_type_2")
        .await
        .expect("no error")
        .is_some());

    client.delete_queue("job_type_1").await.unwrap();

    assert!(client
        .get_queue("job_type_1")
        .await
        .expect("no error")
        .is_none()); // NB

    assert!(client
        .get_queue("job_type_2")
        .await
        .expect("no error")
        .is_some());
}
