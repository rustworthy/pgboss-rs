use std::borrow::Borrow;

use crate::job::Job;
use crate::queue::QueueOptions;
use crate::sql;
use crate::utils;
use crate::QueueInfo;
use sqlx::postgres::PgPool;
use sqlx::types::Json;
use uuid::Uuid;

use super::{builder::ClientBuilder, opts, Client};
use crate::Error;

impl Client {
    /// Create an instance of [`ClientBuilder`]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect() -> Result<Client, Error> {
        let pool = utils::create_pool(None).await?;
        Client::with_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specific url.
    ///
    /// To configure `ssl` (e.g. `sslmode=require`), you will need to build
    /// your own `Pool` and use [`ClientBuilder::with_pool`] method instead.
    pub async fn connect_to<U>(url: U) -> Result<Client, Error>
    where
        U: AsRef<str>,
    {
        let pool = utils::create_pool(Some(url.as_ref())).await?;
        Client::with_pool(pool).await
    }

    /// Bring your own pool.
    pub async fn with_pool(pool: PgPool) -> Result<Self, Error> {
        let opts = opts::ClientOptions::default();
        Ok(Client::new(pool, opts).await?)
    }

    /// Registers a customized queue in the database.
    pub async fn create_queue<'a, Q>(&self, opts: Q) -> Result<(), Error>
    where
        Q: Borrow<QueueOptions<'a>>,
    {
        let stmt = sql::proc::create_queue(&self.opts.schema);
        let q_opts = opts.borrow();
        Ok(sqlx::query(&stmt)
            .bind(q_opts.name)
            .bind(Json(q_opts))
            .execute(&self.pool)
            .await
            .map(|_| ())?)
    }

    /// Registers a standard queue in the database.
    pub async fn create_standard_queue<Q>(&self, name: Q) -> Result<(), Error>
    where
        Q: AsRef<str>,
    {
        let q_opts = QueueOptions {
            name: name.as_ref(),
            ..Default::default()
        };
        self.create_queue(q_opts).await
    }

    /// Returns [`QueueInfo`] on the queue with this name, if any.
    pub async fn get_queue<Q>(&self, queue_name: Q) -> Result<Option<QueueInfo>, Error>
    where
        Q: AsRef<str>,
    {
        let stmt = sql::dml::get_queue(&self.opts.schema);
        let queue: Option<QueueInfo> = sqlx::query_as(&stmt)
            .bind(queue_name.as_ref())
            .fetch_optional(&self.pool)
            .await?;
        Ok(queue)
    }

    /// Return info on all the queues in the system.
    pub async fn get_queues(&self) -> Result<Vec<QueueInfo>, Error> {
        let stmt = sql::dml::get_queues(&self.opts.schema);
        let queues: Vec<QueueInfo> = sqlx::query_as(&stmt).fetch_all(&self.pool).await?;
        Ok(queues)
    }

    /// Deletes a named queue.
    ///
    /// Deletes a queue and all jobs from the active job table.
    /// Any jobs in the archive table are retained.
    pub async fn delete_queue<Q>(&self, queue_name: Q) -> Result<(), Error>
    where
        Q: AsRef<str>,
    {
        let stmt = sql::proc::delete_queue(&self.opts.schema);
        Ok(sqlx::query(&stmt)
            .bind(queue_name.as_ref())
            .execute(&self.pool)
            .await
            .map(|_| ())?)
    }

    /// Enqueue a job.
    pub async fn send_job<J>(&self, job: J) -> Result<Option<Uuid>, Error>
    where
        J: Into<Job>,
    {
        let stmt = sql::proc::create_job(&self.opts.schema);
        let id: Option<Uuid> = sqlx::query_scalar(&stmt)
            .bind(Option::<Uuid>::None)
            .bind("send_job")
            .bind(Json(serde_json::json!({})))
            .bind(Json(serde_json::json!({})))
            .fetch_optional(&self.pool)
            .await?;

        Ok(id)
    }

    /// Create and enqueue a job.
    pub async fn send<Q, D>(&self, name: Q, data: D) -> Result<Option<Uuid>, Error>
    where
        Q: AsRef<str>,
        D: Into<serde_json::Value>,
    {
        let stmt = sql::proc::create_job(&self.opts.schema);
        let id: Option<Uuid> = sqlx::query_scalar(&stmt)
            .bind(Option::<Uuid>::None)
            .bind(name.as_ref())
            .bind(Json(data.into()))
            .bind(Json(serde_json::json!({})))
            .fetch_optional(&self.pool)
            .await?;

        Ok(id)
    }
}
