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
    pub async fn connect_to<U>(url: U) -> Result<Client, sqlx::Error>
    where
        U: AsRef<str>,
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
    pub async fn create_standard_queue<Q>(&self, name: Q) -> Result<(), sqlx::Error>
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
    pub async fn get_queue<Q>(&self, queue_name: Q) -> Result<Option<QueueInfo>, sqlx::Error>
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
    pub async fn get_queues(&self) -> Result<Vec<QueueInfo>, sqlx::Error> {
        let stmt = sql::dml::get_queues(&self.opts.schema);
        let queues: Vec<QueueInfo> = sqlx::query_as(&stmt).fetch_all(&self.pool).await?;
        Ok(queues)
    }

    /// Deletes a named queue.
    ///
    /// Deletes a queue and all jobs from the active job table.
    /// Any jobs in the archive table are retained.
    pub async fn delete_queue<Q>(&self, queue_name: Q) -> Result<(), sqlx::Error>
    where
        Q: AsRef<str>,
    {
        let stmt = sql::proc::delete_queue(&self.opts.schema);
        sqlx::query(&stmt)
            .bind(queue_name.as_ref())
            .execute(&self.pool)
            .await
            .map(|_| ())
    }

    /// Enqueue a job.
    pub async fn send<J>(&self, job: J) -> Result<Option<Uuid>, sqlx::Error>
    where
        J: Into<Job>,
    {
        println!("{:?}", job.into());
        Ok(None)
    }
}
