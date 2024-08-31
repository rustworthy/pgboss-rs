use crate::Error;
use serde::Serializer;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::time::Duration;

pub(crate) async fn create_pool(url: Option<&str>) -> Result<PgPool, Error> {
    let pool = match url {
        Some(url) => {
            PgPoolOptions::new()
                .max_connections(10)
                .connect(url)
                .await?
        }
        None => {
            let var_name =
                std::env::var("POSTGRES_PROVIDER").unwrap_or_else(|_| "POSTGRES_URL".to_string());
            let url =
                std::env::var(var_name).unwrap_or_else(|_| "postgres://localhost:5432".to_string());
            PgPoolOptions::new()
                .max_connections(10)
                .connect(&url)
                .await?
        }
    };
    Ok(pool)
}

pub(crate) fn serialize_duration_as_secs<S>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(dur) => serializer.serialize_u64(dur.as_secs()),
    }
}

pub(crate) fn serialize_duration_as_mins<S>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(dur) => serializer.serialize_u64(dur.as_secs() / 60),
    }
}
