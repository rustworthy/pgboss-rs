use pgboss::Client;

#[tokio::test]
async fn test_client_instantiated() {
    let _c = Client::builder().connect(None).await.unwrap();
}
