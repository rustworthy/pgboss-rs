use crate::job::JobState;
use askama::Template;

#[derive(Template)]
#[template(
    source = "
        WITH deleted_jobs AS (
            DELETE FROM {{ schema }}.job
            {{ where_clause }}
            RETURNING *
        ),
        retried_jobs AS (
            INSERT INTO {{ schema }}.job (
                id,
                name,
                priority,
                data,
                state,
                retry_limit,
                retry_count,
                retry_delay,
                retry_backoff,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_in,
                created_on,
                completed_on,
                keep_until,
                dead_letter,
                policy,
                output
            )
            SELECT
                id,
                name,
                priority,
                data,
                CASE
                    WHEN retry_count < retry_limit THEN '{{ JobState::Retry }}'::{{ schema }}.job_state
                    ELSE '{{ JobState::Failed }}'::{{ schema }}.job_state
                END as state,
                retry_limit,
                retry_count,
                retry_delay,
                retry_backoff,
                CASE
                    WHEN retry_count = retry_limit THEN start_after
                    WHEN NOT retry_backoff THEN now() + retry_delay * interval '1'
                    ELSE now() + (
                        retry_delay * 2 ^ LEAST(16, retry_count + 1) / 2 +
                        retry_delay * 2 ^ LEAST(16, retry_count + 1) / 2 * random()
                    ) * interval '1'
                END as start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_in,
                created_on,
                CASE
                    WHEN retry_count < retry_limit THEN NULL
                    ELSE now()
                END as completed_on,
                keep_until,
                dead_letter,
                policy,        
                {{ output }}
            FROM deleted_jobs
            ON CONFLICT DO NOTHING
            RETURNING *
        ),
        failed_jobs as (
            INSERT INTO {{ schema }}.job (
                id,
                name,
                priority,
                data,
                state,
                retry_limit,
                retry_count,
                retry_delay,
                retry_backoff,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_in,
                created_on,
                completed_on,
                keep_until,
                dead_letter,
                policy,
                output
            )
            SELECT
                id,
                name,
                priority,
                data,
                '{{ JobState::Failed }}'::{{ schema }}.job_state as state,
                retry_limit,
                retry_count,
                retry_delay,
                retry_backoff,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_in,
                created_on,
                now() as completed_on,
                keep_until,
                dead_letter,
                policy,
                {{ output }}
            FROM deleted_jobs
            WHERE id NOT IN (SELECT id from retried_jobs)
            RETURNING *
        ),
        results as (
            SELECT * FROM retried_jobs
            UNION ALL
            SELECT * FROM failed_jobs
        ),
        dlq_jobs as (
            INSERT INTO {{ schema }}.job (name, data, output, retry_limit, keep_until)
            SELECT dead_letter, data, output, retry_limit, keep_until + (keep_until - start_after)
            FROM results
            WHERE state = '{{ JobState::Failed }}'::{{ schema }}.job_state
            AND dead_letter IS NOT NULL
            AND NOT name = dead_letter
        )
        {% if let Some(destination) = result_destination %}
        SELECT COUNT(*) FROM results INTO {{ destination }}
        {% else %}
        SELECT COUNT(*) FROM results
        {% endif %}
        ",
    ext = "txt"
)]
pub(crate) struct FailJobsTemplate<'a> {
    pub schema: &'a str,
    pub where_clause: String,
    pub output: &'static str,
    pub result_destination: Option<&'static str>,
}

pub(crate) fn create_fail_job_by_jids_function(schema: &str) -> String {
    format!(
        r#"
        CREATE OR REPLACE FUNCTION {schema}.fail_jobs_by_jids(qname TEXT, jids UUID[], details JSONB, OUT failed_count BIGINT) AS $$
        BEGIN
            {};
        END;
        $$ LANGUAGE plpgsql;
        "#,
        FailJobsTemplate {
            schema,
            where_clause: format!(
                "WHERE name = qname AND id IN (SELECT UNNEST(jids)) AND state < '{}'::{}.job_state",
                JobState::Completed,
                schema
            ),
            output: "details",
            result_destination: Some("failed_count"),
        }
    )
}

pub(crate) fn fail_jobs_by_jids(schema: &str) -> String {
    format!("SELECT {schema}.fail_jobs_by_jids($1::TEXT, $2::UUID[], $3::JSONB);",)
}

pub(crate) fn create_fail_job_by_timeout_procedure(schema: &str) -> String {
    let fail_returning_failed_count = FailJobsTemplate {
        schema,
        where_clause: format!(
            "WHERE state = '{}'::{}.job_state AND (started_on + expire_in) < now()",
            JobState::Active,
            schema
        ),
        output: r#"'{ "value": { "message": "job failed by timeout in active state" } }'::jsonb"#,
        result_destination: Some("failed_count"),
    }
    .to_string();
    // https://www.postgresql.org/docs/current/plpgsql-transactions.html
    format!(
        r#"
        CREATE OR REPLACE PROCEDURE {schema}.fail_active_jobs_by_timeout(OUT failed_count BIGINT) AS $$
        BEGIN
            COMMIT;
            SET LOCAL lock_timeout = '30s';
            SET LOCAL idle_in_transaction_session_timeout = '30s';
            {fail_returning_failed_count};
            COMMIT;
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
}

pub(crate) fn fail_jobs_by_timeout(schema: &str) -> String {
    format!("CALL {schema}.fail_active_jobs_by_timeout(NULL);")
}
