use crate::utils;
use chrono::Utc;
use pgboss::Client;

#[tokio::test]
async fn simple_connect() {
    // This will crate `pgboss` schema, which is does not
    // allow us to isolate tests properly, so we only use it
    // once in this test - sanity check.
    let _c = Client::connect().await.unwrap();
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
