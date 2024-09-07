use crate::job::JobState;

pub(crate) fn check_if_app_installed(schema: &str) -> String {
    format!(
        "
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = 'version'
        );
        "
    )
}

pub(crate) fn get_app(schema: &str) -> String {
    format!("SELECT * FROM {schema}.version;")
}

pub(crate) fn insert_version(schema: &str, version: u8) -> String {
    format!(
        "INSERT INTO {schema}.version (version) VALUES ({}) ON CONFLICT DO NOTHING;",
        version
    )
}

pub(crate) fn get_queue(schema: &str) -> String {
    format!(
        "
        SELECT
            name,
            policy,
            retry_limit,
            retry_delay,
            retry_backoff,
            expire_seconds,
            retention_minutes,
            dead_letter,
            created_on,
            updated_on
        FROM {schema}.queue
        WHERE name = $1;
        "
    )
}

pub(crate) fn get_queues(schema: &str) -> String {
    format!(
        "
        SELECT
            name,
            policy,
            retry_limit,
            retry_delay,
            retry_backoff,
            expire_seconds,
            retention_minutes,
            dead_letter,
            created_on,
            updated_on
        FROM {schema}.queue;
        "
    )
}

pub(crate) fn fetch_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH next AS (
            SELECT id FROM {schema}.job
            WHERE name = $1 AND state < 'active' AND start_after < now()
            ORDER BY priority DESC, created_on, id
            LIMIT $2
            FOR UPDATE
            SKIP LOCKED
        )
        UPDATE {schema}.job j SET
            state = 'active',
            started_on = now(),
            retry_count = CASE WHEN started_on IS NULL THEN retry_count ELSE retry_count + 1 END
        FROM next
        WHERE name = $1 AND j.id = next.id
        RETURNING j.id, name, data, EXTRACT(epoch FROM expire_in)::float8 as expire_in;
        "#
    )
}

pub(crate) fn delete_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH results AS (
            DELETE FROM {schema}.job
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[]))        
            RETURNING 1
        )
        SELECT COUNT(*) from results;
        "#
    )
}

pub(crate) fn fail_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH results AS (
            UPDATE {schema}.job SET
                state = CASE WHEN retry_count < retry_limit THEN '{0}'::{schema}.job_state ELSE '{1}'::{schema}.job_state END,
                completed_on = CASE WHEN retry_count < retry_limit THEN NULL ELSE now() END,
                start_after = CASE
                    WHEN retry_count = retry_limit THEN start_after
                    WHEN NOT retry_backoff THEN now() + retry_delay * interval '1'
                    ELSE now() + (
                        retry_delay * 2 ^ LEAST(16, retry_count + 1) / 2 +
                        retry_delay * 2 ^ LEAST(16, retry_count + 1) / 2 * random()
                    ) * interval '1'
                    END,
                output = $3
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state < '{2}'::{schema}.job_state
            RETURNING *
        ), dlq_jobs AS (
            INSERT INTO {schema}.job (name, data, output, retry_limit, keep_until)
            SELECT dead_letter, data, output, retry_limit, keep_until + (keep_until - start_after)
            FROM results WHERE state = '{3}'::{schema}.job_state AND dead_letter IS NOT NULL AND NOT name = dead_letter
        )
        SELECT COUNT(*) FROM results
        "#,
        JobState::Retry,     // 0
        JobState::Failed,    // 1
        JobState::Completed, // 2
        JobState::Failed,    // 3
    )
}
