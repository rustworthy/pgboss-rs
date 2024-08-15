use sqlx::postgres::PgPool;

mod builder;

pub use builder::ClientBuilder;

#[derive(Debug)]
pub struct Client {
    pool: PgPool,
    schema: String,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    async fn ensure_data_definition(&mut self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
