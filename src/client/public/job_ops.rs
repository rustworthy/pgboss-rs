use super::Client;
use crate::job::{ActiveJob, Job};
use crate::Error;
use crate::JobOptions;
use sqlx::types::Json;
use std::borrow::Borrow;
use uuid::Uuid;

impl Client {
    /// Enqueue a job.
    pub async fn send_job<J>(&self, job: J) -> Result<Uuid, Error>
    where
        J: Borrow<Job>,
    {
        let job = job.borrow();
        let id: Option<Uuid> = sqlx::query_scalar(&self.stmt.create_job)
            .bind(job.id)
            .bind(&job.queue_name)
            .bind(Json(&job.data))
            .bind(Json(&job.opts))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                if let Some(db_error) = e.as_database_error() {
                    if let Some(constraint) = db_error.constraint() {
                        if constraint.starts_with('j') && constraint.ends_with("_pkey") {
                            return Error::Conflict {
                                msg: "job with this id already exists",
                            };
                        }
                        if constraint == "dlq_fkey" {
                            return Error::Unprocessable {
                                msg: "dead letter queue does not exist",
                            };
                        }
                    }
                }
                Error::Sqlx(e)
            })?;
        id.ok_or(Error::Unprocessable {
            msg: "queue does not exist",
        })
    }

    /// Create and enqueue a job.
    pub async fn send_data<Q, D>(&self, queue_name: Q, data: D) -> Result<Uuid, Error>
    where
        Q: AsRef<str>,
        D: Borrow<serde_json::Value>,
    {
        let id: Option<Uuid> = sqlx::query_scalar(&self.stmt.create_job)
            .bind(Option::<Uuid>::None)
            .bind(queue_name.as_ref())
            .bind(Json(data.borrow()))
            .bind(Json(JobOptions::default()))
            .fetch_one(&self.pool)
            .await?;
        id.ok_or(Error::Unprocessable {
            msg: "queue does not exist",
        })
    }

    /// Fetch a job from a queue.
    pub async fn fetch_job<Q>(&self, queue_name: Q) -> Result<Option<ActiveJob>, Error>
    where
        Q: AsRef<str>,
    {
        let maybe_job: Option<ActiveJob> = sqlx::query_as(&self.stmt.fetch_jobs)
            .bind(queue_name.as_ref())
            .bind(1f64)
            .fetch_optional(&self.pool)
            .await?;
        Ok(maybe_job)
    }

    /// Get this job's details including metadata.
    ///
    /// Unlike [`Client::fetch_job`] _will not consume_ a job from the queue,
    /// rather will only get this job's details. Useful for monitoring, analyzing
    /// the execution progress, e.g. how many times this job has been retried or what
    /// `output` has been written to this jobs.
    pub async fn get_job_info<Q>(
        &self,
        queue_name: Q,
        job_id: Uuid,
    ) -> Result<Option<ActiveJob>, Error>
    where
        Q: AsRef<str>,
    {
        let maybe_job: Option<ActiveJob> = sqlx::query_as(&self.stmt.get_job_info)
            .bind(queue_name.as_ref())
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(maybe_job)
    }

    /// Fetch a batch of jobs.
    pub async fn fetch_jobs<Q>(
        &self,
        queue_name: Q,
        batch_size: u64,
    ) -> Result<Vec<ActiveJob>, Error>
    where
        Q: AsRef<str>,
    {
        let maybe_job: Vec<ActiveJob> = sqlx::query_as(&self.stmt.fetch_jobs)
            .bind(queue_name.as_ref())
            .bind(batch_size as f64)
            .fetch_all(&self.pool)
            .await?;
        Ok(maybe_job)
    }

    /// Delete a job from a queue.
    ///
    /// In a happy path, returns `false`, if the specified queue or the job with this ID does not exist,
    /// otherwise returns `true`.
    ///
    /// To delete numerous jobs from a queue, use [Client::delete_jobs].
    pub async fn delete_job<Q>(&self, queue_name: Q, job_id: Uuid) -> Result<bool, Error>
    where
        Q: AsRef<str>,
    {
        let deleted_count = self.delete_jobs(queue_name, [job_id]).await?;
        Ok(deleted_count == 1)
    }

    /// Delete numerous jobs from a queue.
    ///
    /// In a happy path, returns the number of deleted records, where `0` means
    /// either the specified queue does not exist, or there are no jobs with
    /// these ids in the queue.
    pub async fn delete_jobs<Q, J>(&self, queue_name: Q, job_ids: J) -> Result<usize, Error>
    where
        Q: AsRef<str>,
        J: IntoIterator<Item = Uuid>,
    {
        let deleted_count: (i64,) = sqlx::query_as(&self.stmt.delete_jobs)
            .bind(queue_name.as_ref())
            .bind(job_ids.into_iter().collect::<Vec<Uuid>>())
            .fetch_one(&self.pool)
            .await?;
        Ok(deleted_count.0 as usize)
    }

    /// Mark a job as failed.
    pub async fn fail_job<Q, O>(
        &self,
        queue_name: Q,
        job_id: Uuid,
        details: O,
    ) -> Result<bool, Error>
    where
        Q: AsRef<str>,
        O: Into<serde_json::Value>,
    {
        let deleted_count = self.fail_jobs(queue_name, [job_id], details).await?;
        Ok(deleted_count == 1)
    }

    /// Mark numerous jobs as failed.
    ///
    /// In a happy path, returns the number of jobs marked as `failed`,
    /// where `0` means there are no jobs with these ids in the queue or
    /// no such queue.
    pub async fn fail_jobs<Q, I, O>(
        &self,
        queue_name: Q,
        job_ids: I,
        details: O,
    ) -> Result<usize, Error>
    where
        Q: AsRef<str>,
        I: IntoIterator<Item = Uuid>,
        O: Into<serde_json::Value>,
    {
        let deleted_count: (i64,) = sqlx::query_as(&self.stmt.fail_jobs)
            .bind(queue_name.as_ref())
            .bind(job_ids.into_iter().collect::<Vec<Uuid>>())
            .bind(details.into())
            .fetch_one(&self.pool)
            .await?;
        Ok(deleted_count.0 as usize)
    }

    /// Mark a job as completed.
    pub async fn complete_job<Q, O>(
        &self,
        queue_name: Q,
        job_id: Uuid,
        details: O,
    ) -> Result<bool, Error>
    where
        Q: AsRef<str>,
        O: Into<serde_json::Value>,
    {
        let completed_count = self.complete_jobs(queue_name, [job_id], details).await?;
        Ok(completed_count == 1)
    }

    /// Mark numerous jobs as completed.
    ///
    /// In a happy path, returns the number of jobs marked as `completed`,
    /// where `0` means there are no jobs with these ids in the queue or
    /// no such queue.
    pub async fn complete_jobs<Q, I, O>(
        &self,
        queue_name: Q,
        job_ids: I,
        details: O,
    ) -> Result<usize, Error>
    where
        Q: AsRef<str>,
        I: IntoIterator<Item = Uuid>,
        O: Into<serde_json::Value>,
    {
        let deleted_count: (i64,) = sqlx::query_as(&self.stmt.complete_jobs)
            .bind(queue_name.as_ref())
            .bind(job_ids.into_iter().collect::<Vec<Uuid>>())
            .bind(details.into())
            .fetch_one(&self.pool)
            .await?;
        Ok(deleted_count.0 as usize)
    }
}
