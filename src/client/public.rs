use std::borrow::Borrow;

use crate::queue::QueueOptions;
use crate::sql;
use crate::utils;
use sqlx::postgres::PgPool;
use sqlx::types::Json;

use super::{builder, opts, Client};

impl Client {
    pub fn builder() -> builder::ClientBuilder {
        builder::ClientBuilder::default()
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect() -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(None).await?;
        Client::connect_with(pool).await
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect_to(url: &str) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(Some(url)).await?;
        Client::connect_with(pool).await
    }

    /// Bring your own pool.
    pub async fn connect_with(pool: PgPool) -> Result<Self, sqlx::Error> {
        let opts = opts::ClientOptions::default();
        Client::new(pool, opts).await
    }

    /// Registers a new queue in the database.
    pub async fn create_queue<S, Q>(&self, qname: S, opts: Q) -> Result<(), sqlx::Error>
    where
        S: AsRef<str>,
        Q: Borrow<QueueOptions>,
    {
        let stmt = sql::proc::create_queue(&self.opts.schema);
        sqlx::query(&stmt)
            .bind(qname.as_ref())
            .bind(Json(opts.borrow()))
            .execute(&self.pool)
            .await
            .map(|_| ())
    }
}
