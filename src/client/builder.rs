use sqlx::postgres::{PgConnectOptions, PgPool};

use super::{opts, Client};
use crate::utils;

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
        self.use_pool(pool).await
    }

    /// Connect to the PostgreSQL server using specifi url.
    pub async fn connect_to(self, url: &str) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(Some(url)).await?;
        self.use_pool(pool).await
    }

    // Connect to the PostgreSQL server using specific `PgConnectOptions`
    pub async fn connect_with(opts: PgConnectOptions) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool_with(opts).await?;
        Client::use_pool(pool).await
    }

    /// Bring your own pool.
    pub async fn use_pool(self, pool: PgPool) -> Result<Client, sqlx::Error> {
        let opts = opts::ClientOptions {
            schema: self.schema,
        };
        Client::new(pool, opts).await
    }
}
