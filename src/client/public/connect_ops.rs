use super::opts;
use super::Client;
use crate::utils;
use crate::Error;
use sqlx::postgres::PgPool;

#[cfg(doc)]
use super::ClientBuilder;

impl Client {
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
}