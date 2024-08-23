use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};

pub(crate) async fn create_pool(url: Option<&str>) -> Result<PgPool, sqlx::Error> {
    let pool = match url {
        Some(url) => {
            PgPoolOptions::new()
                .max_connections(10)
                .connect(url)
                .await?
        }
        None => {
            let var_name =
                std::env::var("POSTGRES_PROVIDER").unwrap_or_else(|_| "POSTGRES_URL".to_string());
            let url =
                std::env::var(var_name).unwrap_or_else(|_| "postgres://localhost:5432".to_string());
            PgPoolOptions::new()
                .max_connections(10)
                .connect(&url)
                .await?
        }
    };
    Ok(pool)
}

pub(crate) async fn create_pool_with(opts: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
    Ok(PgPoolOptions::new()
        .max_connections(10)
        .connect_with(opts)
        .await?)
}
