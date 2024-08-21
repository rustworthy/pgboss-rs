use sqlx::postgres::PgPool;

use crate::utils;
use crate::{app::App, sql};

mod builder;
mod opts;

pub use builder::ClientBuilder;

#[derive(Debug, Clone)]
pub struct Client {
    pool: PgPool,
    opts: opts::ClientOptions,
}

impl Client {
    async fn new(pool: PgPool, opts: opts::ClientOptions) -> Result<Self, sqlx::Error> {
        let mut c = Client { pool, opts };
        c.init().await?;
        Ok(c)
    }

    async fn init(&mut self) -> Result<(), sqlx::Error> {
        if let Some(app) = self.maybe_existing_app().await? {
            println!(
                "App already exists: version={}, maintained_on={:?}, cron_on={:?}",
                app.version, app.maintained_on, app.cron_on
            );
            if app.version < crate::MINIMUM_SUPPORTED_PGBOSS_APP_VERSION as i32 {
                panic!("Cannot migrate from the currently installed PgBoss application.")
            }
            return Ok(());
        }
        self.install_app().await?;
        Ok(())
    }

    async fn install_app(&mut self) -> Result<(), sqlx::Error> {
        let ddl = sql::install_app(&self.opts.schema);
        sqlx::raw_sql(&ddl).execute(&self.pool).await?;
        Ok(())
    }

    async fn maybe_existing_app(&mut self) -> Result<Option<App>, sqlx::Error> {
        let stmt = sql::dml::check_if_app_installed(&self.opts.schema);
        let installed: bool = sqlx::query_scalar(&stmt).fetch_one(&self.pool).await?;
        if !installed {
            return Ok(None);
        }
        let stmt = sql::dml::get_app(&self.opts.schema);
        let app: Option<App> = sqlx::query_as(&stmt).fetch_optional(&self.pool).await?;
        Ok(app)
    }
}

// --------------------------- PUBLIC -------------------------------
impl Client {
    pub fn builder() -> builder::ClientBuilder {
        builder::ClientBuilder::default()
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect() -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(None).await?;
        Client::connect_with(pool).await
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect_to(url: &str) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(Some(url)).await?;
        Client::connect_with(pool).await
    }

    /// Bring your own pool.
    pub async fn connect_with(pool: PgPool) -> Result<Self, sqlx::Error> {
        let opts = opts::ClientOptions::default();
        Client::new(pool, opts).await
    }

    /// Registers a new queue in the database.
    pub async fn create_queue() -> Result<(), sqlx::Error> {
        Ok(())
    }
}
