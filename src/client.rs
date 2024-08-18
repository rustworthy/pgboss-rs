use sqlx::postgres::PgPool;

use crate::utils;
use crate::{app::App, stmt};

#[derive(Debug, Clone)]
struct ClientOptions {
    schema: String,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            schema: "pgboss".to_string(),
        }
    }
}

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

    /// Bring your own pool.
    pub async fn connect_with(self, pool: PgPool) -> Result<Client, sqlx::Error> {
        let opts = ClientOptions {
            schema: self.schema,
        };
        Client::new(pool, opts).await
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect_to(self, url: &str) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(Some(url)).await?;
        self.connect_with(pool).await
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect(self) -> Result<Client, sqlx::Error> {
        let pool = utils::create_pool(None).await?;
        self.connect_with(pool).await
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    pool: PgPool,
    opts: ClientOptions,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
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
        let opts = ClientOptions::default();
        Client::new(pool, opts).await
    }

    async fn new(pool: PgPool, opts: ClientOptions) -> Result<Self, sqlx::Error> {
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
        self.create_all().await?;
        Ok(())
    }

    async fn create_all(&mut self) -> Result<(), sqlx::Error> {
        let ddl = stmt::compile_ddl(&self.opts.schema);
        sqlx::raw_sql(&ddl).execute(&self.pool).await?;
        Ok(())
    }

    async fn maybe_existing_app(&mut self) -> Result<Option<App>, sqlx::Error> {
        let stmt = stmt::check_if_app_installed(&self.opts.schema);
        let installed: bool = sqlx::query_scalar(&stmt).fetch_one(&self.pool).await?;
        if !installed {
            return Ok(None);
        }
        let stmt = stmt::get_app(&self.opts.schema);
        let app: Option<App> = sqlx::query_as(&stmt).fetch_optional(&self.pool).await?;
        Ok(app)
    }
}
