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

pub(crate) fn get_all_queues(schema: &str) -> String {
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
