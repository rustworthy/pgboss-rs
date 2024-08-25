use std::borrow::Borrow;

use crate::queue::QueueOptions;
use crate::sql;
use crate::utils;
use crate::QueueInfo;
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
        Client::with_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specific url.
    ///
    /// To configure `ssl` (e.g. `sslmode=require`), you will need to build
    /// your own `Pool` and use [`ClientBuilder::with_pool`] method instead.
    pub async fn connect_to<S>(url: S) -> Result<Client, sqlx::Error>
    where
        S: AsRef<str>,
    {
        let pool = utils::create_pool(Some(url.as_ref())).await?;
        Client::with_pool(pool).await
    }

    /// Bring your own pool.
    pub async fn with_pool(pool: PgPool) -> Result<Self, sqlx::Error> {
        let opts = opts::ClientOptions::default();
        Client::new(pool, opts).await
    }

    /// Registers a customized queue in the database.
    pub async fn create_queue<'a, Q>(&self, opts: Q) -> Result<(), sqlx::Error>
    where
        Q: Borrow<QueueOptions<'a>>,
    {
        let stmt = sql::proc::create_queue(&self.opts.schema);
        let q_opts = opts.borrow();
        sqlx::query(&stmt)
            .bind(q_opts.name)
            .bind(Json(q_opts))
            .execute(&self.pool)
            .await
            .map(|_| ())
    }

    /// Registers a standard queue in the database.
    pub async fn create_standard_queue<S>(&self, name: S) -> Result<(), sqlx::Error>
    where
        S: AsRef<str>,
    {
        let q_opts = QueueOptions {
            name: name.as_ref(),
            ..Default::default()
        };
        self.create_queue(q_opts).await
    }

    /// Returns all queues.
    pub async fn get_queues(&self) -> Result<Vec<QueueInfo>, sqlx::Error> {
        let stmt = sql::dml::get_all_queues(&self.opts.schema);
        let queues = sqlx::query_as(&stmt).fetch_all(&self.pool).await;
        let queues: Vec<QueueInfo> = queues?;
        Ok(queues)
    }
}
