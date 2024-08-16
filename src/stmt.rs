use crate::job::JobState;

pub(crate) fn create_schema(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {};", schema)
}

fn create_job_state_enum(schema: &str) -> String {
    format!(
        "DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type typ INNER JOIN pg_namespace nsp on (typ.typnamespace = nsp.oid) where typ.typname = 'job_state' AND nsp.nspname = '{schema}') THEN
                CREATE TYPE {schema}.job_state AS ENUM ('{}', '{}', '{}', '{}', '{}', '{}');
            END IF;
        END $$;",
        JobState::Created,
        JobState::Retry,
        JobState::Active,
        JobState::Completed,
        JobState::Cancelled,
        JobState::Failed,
    )
}

fn create_version_table(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.version (
        version int primary key,
        maintained_on timestamp with time zone,
        cron_on timestamp with time zone);",
        schema
    )
}

fn locked<I>(schema: &str, stmts: I) -> String
where
    I: IntoIterator<Item = String>,
{
    format!(
        "BEGIN;
        SET LOCAL lock_timeout = '30s';
        SET LOCAL idle_in_transaction_session_timeout = '30s';
        SELECT pg_advisory_xact_lock(('x' || encode(sha224((current_database() || '.pgboss.{schema}')::bytea), 'hex'))::bit(64)::bigint);
        {};
        COMMIT;",
        stmts.into_iter().collect::<Vec<_>>().join("\n"),
    )
}

pub(crate) fn compile_all(schema: &str) -> String {
    locked(
        schema,
        [
            create_schema(schema),
            create_job_state_enum(schema),
            create_version_table(schema),
        ],
    )
}
