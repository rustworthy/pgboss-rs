use crate::{App, sql};
use log::info;
use sqlx::postgres::PgPool;

mod builder;
mod opts;
mod public;

pub use builder::ClientBuilder;
pub use public::maintain_ops::MaintenanceStats;

#[derive(Debug, Clone)]
struct DmlStatements {
    fetch_jobs: String,
    get_job_info: String,
    delete_jobs: String,
    fail_jobs_by_jids: String,
    fail_jobs_by_timeout: String,
    cancel_jobs: String,
    complete_jobs: String,
    resume_jobs: String,
    create_job: String,
    create_queue: String,
    get_queue: String,
    get_all_queues: String,
    delete_queue: String,
}

impl DmlStatements {
    fn for_schema(name: &str) -> DmlStatements {
        DmlStatements {
            fetch_jobs: sql::dml::fetch_jobs(name),
            get_job_info: sql::dml::get_job_info(name),
            delete_jobs: sql::dml::delete_jobs(name),
            cancel_jobs: sql::dml::cancel_jobs(name),
            resume_jobs: sql::dml::resume_jobs(name),
            complete_jobs: sql::dml::complete_jobs(name),
            get_queue: sql::dml::get_queue(name),
            get_all_queues: sql::dml::get_all_queues(name),
            create_job: sql::dml::create_job(name),
            create_queue: sql::dml::create_queue(name),
            delete_queue: sql::dml::delete_queue(name),
            fail_jobs_by_jids: sql::dml::fail_jobs_by_jids(name),
            fail_jobs_by_timeout: sql::dml::fail_jobs_by_timeout(name),
        }
    }
}

/// PgBoss client.
#[derive(Debug, Clone)]
pub struct Client {
    pool: PgPool,
    opts: opts::ClientOptions,
    stmt: DmlStatements,
}

impl Client {
    async fn new(pool: PgPool, opts: opts::ClientOptions) -> Result<Self, sqlx::Error> {
        let stmt = DmlStatements::for_schema(&opts.schema);
        let mut c = Client { pool, opts, stmt };
        c.init().await?;
        Ok(c)
    }

    async fn init(&mut self) -> Result<(), sqlx::Error> {
        if let Some(app) = self.maybe_existing_app().await? {
            log::info!(
                "app already exists: version={}, cron_on={:?}",
                app.version,
                app.cron_on
            );
            if app.version < crate::MINIMUM_SUPPORTED_PGBOSS_APP_VERSION as i32 {
                panic!(
                    "cannot migrate from currently installed PgBoss application version: installed={}, minimal={}",
                    app.version,
                    crate::MINIMUM_SUPPORTED_PGBOSS_APP_VERSION
                )
            }
            if app.version < crate::CURRENT_PGBOSS_APP_VERSION as i32 {
                log::info!(
                    "need to apply migratations to the existing app: version={}, latest={}",
                    app.version,
                    crate::CURRENT_PGBOSS_APP_VERSION
                );
                panic!("unreachable as of release 0.1.0")
            }
            return Ok(());
        }
        self.install_app().await?;
        Ok(())
    }

    async fn install_app(&mut self) -> Result<(), sqlx::Error> {
        let ddl = sql::install_app(&self.opts.schema);
        if let Err(sqlx_err) = sqlx::raw_sql(&ddl).execute(&self.pool).await {
            if let sqlx::Error::Database(sqlx_db_err) = &sqlx_err {
                let msg = sqlx_db_err.message();
                if msg.ends_with("already exists") {
                    info!("assuming the mogrationn are already applied, message: {msg}");
                    return Ok(());
                }
            }
            Err(sqlx_err)
        } else {
            Ok(())
        }
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
