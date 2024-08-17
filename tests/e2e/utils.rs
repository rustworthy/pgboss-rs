use sqlx::Connection;

lazy_static::lazy_static! {
    pub(crate) static ref POSRGRES_URL: String = {
        let var_name =
        std::env::var("POSTGRES_PROVIDER")
            .unwrap_or_else(|_| "POSTGRES_URL".to_string());
        std::env::var(var_name)
            .unwrap_or_else(|_| "postgres://localhost:5432".to_string())
    };
}

pub(crate) async fn ad_hoc_sql<I>(stmt: I) -> Result<(), sqlx::Error>
where
    I: IntoIterator<Item = String>,
{
    let mut conn = sqlx::PgConnection::connect(&*POSRGRES_URL).await?;
    let r = sqlx::raw_sql(&stmt.into_iter().collect::<Vec<_>>().join("\n"))
        .execute(&mut conn)
        .await;
    conn.close().await?;
    Ok(r.map(|_| ())?)
}

pub(crate) async fn drop_schema(schema: &str) -> Result<(), sqlx::Error> {
    let stmt = format!("DROP SCHEMA {} CASCADE", schema);
    ad_hoc_sql([stmt]).await
}
