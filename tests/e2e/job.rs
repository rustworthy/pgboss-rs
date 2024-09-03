use crate::utils;
use pgboss::{Client, Error, Job};

#[tokio::test]
async fn send_job() {
    let local = "send_job";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let job = Job {
        name: "jobtype".into(),
        ..Default::default()
    };
    let _id = c.send_job(&job).await.expect("no error");
}

#[tokio::test]
async fn send_job_with_id() {
    let local = "send_job_with_id";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();
    c.create_standard_queue("jobtype").await.unwrap();

    let id = uuid::Uuid::new_v4();
    let job = Job {
        id: Some(id),
        name: "jobtype".into(),
        ..Default::default()
    };
    let inserted_id = c.send_job(&job).await.expect("no error");
    assert_eq!(inserted_id, id);

    let job = Job {
        id: Some(id),
        name: "jobtype".into(),
        ..Default::default()
    };
    let err = c.send_job(&job).await.unwrap_err();
    if let Error::Conflict{msg} = err {
        assert_eq!(msg, "job with this id already exists");
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn send_job_queue_does_not_exist() {
    let local = "send_job_queue_does_not_exist";
    utils::drop_schema(&local).await.unwrap();

    let c = Client::builder().schema(local).connect().await.unwrap();

    let job = Job {
        name: "jobtype".into(),
        ..Default::default()
    };

    if let Error::Unprocessable { msg } = c.send_job(&job).await.unwrap_err() {
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

    if let Error::Unprocessable { msg } = c
        .send_data("jobtype", serde_json::json!({"key": "value"}))
        .await
        .unwrap_err()
    {
        assert!(msg.contains("queue does not exist"))
    } else {
        unreachable!()
    }
}
