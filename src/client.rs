use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{app::App, stmt};

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
        let mut client = Client {
            pool,
            schema: self.schema,
        };
        if let Some(app) = client.existing_app().await? {
            println!(
                "App already exists: version={}, maintained_on={}, cron_on={}",
                app.version, app.maintained_on, app.cron_on
            );
            if app.version < crate::MINIMUM_SUPPORTED_PGBOSS_DDL_REVISION {
                panic!("Cannot migrate from the currently installed PgBoss application.")
            }
            return Ok(client);
        }
        client.migrate().await.map(|_| client)
    }

    /// Connect to the PostgreSQL server.
    pub async fn connect(self, url: Option<&str>) -> Result<Client, sqlx::Error> {
        let pool = match url {
            Some(url) => {
                PgPoolOptions::new()
                    .max_connections(10)
                    .connect(url)
                    .await?
            }
            None => {
                let var_name = std::env::var("POSTGRES_PROVIDER")
                    .unwrap_or_else(|_| "POSTGRES_URL".to_string());
                let url = std::env::var(var_name)
                    .unwrap_or_else(|_| "postgres://localhost:5432".to_string());
                PgPoolOptions::new()
                    .max_connections(10)
                    .connect(&url)
                    .await?
            }
        };
        self.connect_with(pool).await
    }
}

#[derive(Debug)]
pub struct Client {
    pool: PgPool,
    schema: String,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    async fn migrate(&mut self) -> Result<(), sqlx::Error> {
        let ddl = stmt::compile_ddl(&self.schema);
        sqlx::raw_sql(&ddl).execute(&self.pool).await?;
        Ok(())
    }

    async fn existing_app(&mut self) -> Result<Option<App>, sqlx::Error> {
        let stmt = stmt::check_if_app_installed(&self.schema);
        let installed: bool = sqlx::query_scalar(&stmt).fetch_one(&self.pool).await?;
        if !installed {
            return Ok(None);
        }
        let stmt = stmt::get_app(&self.schema);
        let app: App = sqlx::query_as(&stmt).fetch_one(&self.pool).await?;
        Ok(Some(app))
    }
}
