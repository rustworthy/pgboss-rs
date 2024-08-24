use crate::utils::{self, POSRGRES_URL};
use chrono::Utc;
use pgboss::{Client, QueueOptions};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn simple_connect() {
    utils::drop_schema("pgboss").await.unwrap();
    // This will crate `pgboss` schema, which is does not
    // allow us to isolate tests properly, so we only use it
    // once in this test - sanity check.
    //
    // We are also leaving it behind to able to inspect the db with psql.
    let _c = Client::connect().await.unwrap();
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
async fn create_queue() {
    let local = "create_queue";
    utils::drop_schema(local).await.unwrap();
    let client = Client::builder().schema(local).connect().await.unwrap();
    client
        .create_queue("job_type", QueueOptions::default())
        .await
        .unwrap();
}
