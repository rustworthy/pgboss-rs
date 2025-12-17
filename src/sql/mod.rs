pub(crate) mod ddl;
pub(crate) mod dml;

// https://github.com/timgit/pg-boss/blob/3da860f0e6f0650dcb95f62e5b71af6dfbeb44f1/src/plans.ts#L987
fn locked<I>(schema: &str, stmts: I, key: Option<&str>) -> String
where
    I: IntoIterator<Item = String>,
{
    format!(
        "
        BEGIN;
        SET LOCAL lock_timeout = 30000;
        SET LOCAL idle_in_transaction_session_timeout = 30000;
        SELECT pg_advisory_xact_lock(('x' || encode(sha224((current_database() || '.pgboss.{schema}${1}')::bytea), 'hex'))::bit(64)::bigint);
        {0};
        COMMIT;
        ",
        stmts.into_iter().collect::<Vec<_>>().join("\n"),
        key.unwrap_or_default(),
    )
}

// Example output after `tests::e2e::queue::create_queue` test run:
// \d
//```
//                                            List of relations
//    Schema    |                           Name                            |       Type        |  Owner
//--------------+-----------------------------------------------------------+-------------------+----------
// create_queue | je2814a52c8bc616deb91915a8307a2c14e29e12838bdbf7c681509e0 | table             | username
// create_queue | job                                                       | partitioned table | username
// create_queue | job_common                                                | table             | username
// create_queue | queue                                                     | table             | username
// create_queue | schedule                                                  | table             | username
// create_queue | subscription                                              | table             | username
// create_queue | version                                                   | table             | username
//(7 rows)
// ```
pub(crate) fn install_app(schema: &str) -> String {
    locked(
        schema,
        [
            ddl::create_schema(schema),
            ddl::create_job_state_enum(schema),
            // 6 tables (7th in example above is a dedicated table for a partitioned queue)
            ddl::create_version_table(schema),
            ddl::create_queue_table(schema),
            ddl::create_schedule_table(schema),
            ddl::create_subscription_table(schema),
            ddl::create_job_table(schema),
            ddl::create_job_common_table(schema),
            // 2 procedures
            ddl::proc::create_create_queue_function(schema),
            ddl::proc::create_delete_queue_function(schema),
            // app's current version
            dml::insert_version(schema, crate::CURRENT_PGBOSS_APP_VERSION),
        ],
        None,
    )
}
