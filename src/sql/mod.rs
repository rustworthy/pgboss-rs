pub(crate) mod ddl;
pub(crate) mod dml;

fn locked<I>(schema: &str, stmts: I) -> String
where
    I: IntoIterator<Item = String>,
{
    format!(
        "
        BEGIN;
        SET LOCAL lock_timeout = '30s';
        SET LOCAL idle_in_transaction_session_timeout = '30s';
        SELECT pg_advisory_xact_lock(('x' || encode(sha224((current_database() || '.pgboss.{schema}')::bytea), 'hex'))::bit(64)::bigint);
        {};
        COMMIT;
        ",
        stmts.into_iter().collect::<Vec<_>>().join("\n"),
    )
}

///
/// \d
///```md
/// List of relations
/// Schema |     Name     |       Type        |    Owner    
/// --------+--------------+-------------------+-------------
/// pgboss | archive      | table             | pgboss_user
/// pgboss | job          | partitioned table | pgboss_user
/// pgboss | queue        | table             | pgboss_user
/// pgboss | subscription | table             | pgboss_user
/// pgboss | version      | table             | pgboss_user
/// (5 rows)
/// ```
///
pub(crate) fn install_app(schema: &str) -> String {
    locked(
        schema,
        [
            ddl::create_schema(schema),
            ddl::create_job_state_enum(schema),
            ddl::create_version_table(schema),
            ddl::create_queue_table(schema),
            ddl::create_subscription_table(schema),
            ddl::create_job_table(schema),
            ddl::create_archive_table(schema),
            ddl::create_create_queue_function(schema),
            // ...
            dml::insert_version(schema, crate::CURRENT_PGBOSS_APP_VERSION),
        ],
    )
}
