use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::stmt;

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
    pub fn schema(mut self, schema: String) -> Self {
        self.schema = schema;
        self
    }

    /// Bring your own pool.
    pub async fn connect_with(self, pool: PgPool) -> Result<Client, sqlx::Error> {
        let mut client = Client {
            pool,
            schema: self.schema,
        };
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
        let ddl = stmt::compile_all(&self.schema);
        println!("{}", ddl);
        sqlx::raw_sql(&ddl).execute(&self.pool).await?;

        Ok(())
    }
}
