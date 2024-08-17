use crate::utils;
use pgboss::Client;

#[tokio::test]
async fn test_client_instantiated() {
    let local = "test_client_instantiated";
    let _c = Client::builder().schema(local).connect(None).await.unwrap();
    utils::drop_schema(local).await.unwrap();
}
