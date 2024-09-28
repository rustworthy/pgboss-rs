use super::Client;
use crate::queue::QueueOptions;
use crate::Error;
use crate::QueueInfo;
use sqlx::types::Json;
use std::borrow::Borrow;

impl Client {
    /// Registers a customized queue in the database.
    /// 
    /// This operation will _not_ fail if the queue already exists.
    pub async fn create_queue<'a, Q>(&self, opts: Q) -> Result<(), Error>
    where
        Q: Borrow<QueueOptions<'a>>,
    {
        let q_opts = opts.borrow();
        Ok(sqlx::query(&self.stmt.create_queue)
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
        let queue: Option<QueueInfo> = sqlx::query_as(&self.stmt.get_queue)
            .bind(queue_name.as_ref())
            .fetch_optional(&self.pool)
            .await?;
        Ok(queue)
    }

    /// Return info on all the queues in the system.
    pub async fn get_queues(&self) -> Result<Vec<QueueInfo>, Error> {
        let queues: Vec<QueueInfo> = sqlx::query_as(&self.stmt.get_queues)
            .fetch_all(&self.pool)
            .await?;
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
        Ok(sqlx::query(&self.stmt.delete_queue)
            .bind(queue_name.as_ref())
            .execute(&self.pool)
            .await
            .map(|_| ())?)
    }
}
