use crate::utils;
use pgboss::Client;

#[tokio::test]
async fn test_client_instantiated_idempotently() {
    let local = "test_client_instantiated";

    // as if N containers from a ReplicaSet are performing bootstrapping
    let mut js = tokio::task::JoinSet::new();
    for _ in 0..100 {
        js.spawn(async move {
            Client::builder().schema(local).connect(None).await.unwrap();
        });
    }

    // wait for all the tasks to complete
    while let Some(res) = js.join_next().await {
        res.unwrap()
    }

    utils::drop_schema(local).await.unwrap();
}
