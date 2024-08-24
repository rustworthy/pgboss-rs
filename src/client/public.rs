use std::borrow::Borrow;

use crate::queue::QueueOptions;
use crate::sql;
use crate::utils;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPool;
use sqlx::types::Json;

use super::{builder::ClientBuilder, opts, Client};

impl Client {
    /// Create an instance of [`ClientBuilder`]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect() -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(None).await?;
        Client::use_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specific url.
    pub async fn connect_to(url: &str) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(Some(url)).await?;
        Client::use_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specific `PgConnectOptions`
    pub async fn connect_with(opts: PgConnectOptions) -> Result<Self, sqlx::Error> {
        let pool = utils::create_pool_with(opts).await?;
        Client::use_pool(pool).await
    }

    /// Bring your own pool.
    pub async fn use_pool(pool: PgPool) -> Result<Self, sqlx::Error> {
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
