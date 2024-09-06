use super::Client;
use crate::job::Job;
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
            .bind(&job.name)
            .bind(Json(serde_json::json!({})))
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
    pub async fn fetch_one<Q>(&self, queue_name: Q) -> Result<(), Error>
    where
        Q: AsRef<str>,
    {
        let _d = sqlx::query_as(&self.stmt.fetch_one)
            .bind(queue_name.as_ref())
            .bind(1)
            .fetch_one(&self.pool)
            .await?;
        unimplemented!("{:?}", queue_name.as_ref())
    }
}
