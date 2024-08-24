use sqlx::postgres::PgPool;

use super::{opts, Client};
use crate::utils;

/// Builder for [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    schema: String,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        ClientBuilder {
            schema: "pgboss".to_string(),
        }
    }
}

impl ClientBuilder {
    /// Schema name.
    pub fn schema<S>(mut self, schema: S) -> Self
    where
        S: Into<String>,
    {
        self.schema = schema.into();
        self
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect(self) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(None).await?;
        self.with_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specific url.
    ///
    /// To configure `ssl` (e.g. `sslmode=require`), you will need to build
    /// your own `Pool` and use [`ClientBuilder::with_pool`] method instead.
    pub async fn connect_to<S>(self, url: S) -> Result<Client, sqlx::Error>
    where
        S: AsRef<str>,
    {
        let pool = utils::create_pool(Some(url.as_ref())).await?;
        self.with_pool(pool).await
    }

    /// Bring your own pool.
    pub async fn with_pool(self, pool: PgPool) -> Result<Client, sqlx::Error> {
        let opts = opts::ClientOptions {
            schema: self.schema,
        };
        Client::new(pool, opts).await
    }
}
