use crate::{sql, App};
use sqlx::postgres::PgPool;

mod builder;
mod opts;
mod public;

pub use builder::ClientBuilder;

#[derive(Debug, Clone)]
struct Statements {
    fetch_jobs: String,
    get_job_info: String,
    delete_jobs: String,
    fail_jobs: String,
    complete_jobs: String,
    create_job: String,
    create_queue: String,
    get_queue: String,
    get_queues: String,
    delete_queue: String,
}

impl Statements {
    fn for_schema(name: &str) -> Statements {
        Statements {
            fetch_jobs: sql::dml::fetch_jobs(name),
            get_job_info: sql::dml::get_job_info(name),
            delete_jobs: sql::dml::delete_jobs(name),
            create_job: sql::proc::create_job(name),
            fail_jobs: sql::dml::fail_jobs(name),
            complete_jobs: sql::dml::complete_jobs(name),
            create_queue: sql::proc::create_queue(name),
            get_queue: sql::dml::get_queue(name),
            get_queues: sql::dml::get_queues(name),
            delete_queue: sql::proc::delete_queue(name),
        }
    }
}

/// PgBoss client.
#[derive(Debug, Clone)]
pub struct Client {
    pool: PgPool,
    opts: opts::ClientOptions,
    stmt: Statements,
}

impl Client {
    async fn new(pool: PgPool, opts: opts::ClientOptions) -> Result<Self, sqlx::Error> {
        let stmt = Statements::for_schema(&opts.schema);
        let mut c = Client { pool, opts, stmt };
        c.init().await?;
        Ok(c)
    }

    async fn init(&mut self) -> Result<(), sqlx::Error> {
        if let Some(app) = self.maybe_existing_app().await? {
            log::info!(
                "App already exists: version={}, maintained_on={:?}, cron_on={:?}",
                app.version,
                app.maintained_on,
                app.cron_on
            );
            if app.version < crate::MINIMUM_SUPPORTED_PGBOSS_APP_VERSION as i32 {
                panic!("Cannot migrate from the currently installed PgBoss application.")
            }
            // We are still (re)installing functions, because:
            // - we are using `create_job` function (not used in Node.js PgBoss implementation)
            // - in the `crate_queue` function, we are using `jsonb` as `options` type (`json` in Node.js PgBoss)
            self.install_functions().await?;
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

    async fn install_functions(&self) -> Result<(), sqlx::Error> {
        let ddl = sql::install_functions(&self.opts.schema);
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
