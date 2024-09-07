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
        WITH next as (
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
            retry_count = CASE WHEN started_on IS NULL THEN 0 ELSE retry_count + 1 END
        FROM next
        WHERE name = $1 AND j.id = next.id
        RETURNING j.id, name, data, EXTRACT(epoch FROM expire_in)::float8 as expire_in;
        "#,
    )
}
