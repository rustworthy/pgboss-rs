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
        RETURNING 
            j.id,
            name,
            data,
            EXTRACT(epoch FROM expire_in)::float8 as expire_in,
            policy,
            priority,
            retry_limit,
            retry_delay,
            retry_count,
            retry_backoff,
            start_after,
            created_on as "created_at",
            started_on as "started_at",
            singleton_on as "singleton_at";
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
        WITH deleted_jobs AS (
            DELETE FROM {schema}.job
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state < '{2}'::{schema}.job_state
            RETURNING *
        ),
        retried_jobs AS (
            INSERT INTO {schema}.job (
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
                    WHEN retry_count < retry_limit THEN '{0}'::{schema}.job_state
                    ELSE '{1}'::{schema}.job_state
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
                $3::jsonb
            FROM deleted_jobs
            ON CONFLICT DO NOTHING
            RETURNING *
        ),
        failed_jobs as (
            INSERT INTO {schema}.job (
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
                '{1}'::{schema}.job_state as state,
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
                $3::jsonb
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
            INSERT INTO {schema}.job (name, data, output, retry_limit, keep_until)
            SELECT dead_letter, data, output, retry_limit, keep_until + (keep_until - start_after)
            FROM results WHERE state = '{1}'::{schema}.job_state AND dead_letter IS NOT NULL AND NOT name = dead_letter
        )
        SELECT COUNT(*) FROM results
        "#,
        JobState::Retry,     // 0
        JobState::Failed,    // 1
        JobState::Completed, // 2
    )
}

pub(crate) fn complete_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH results AS (
            UPDATE {schema}.job
            SET state = '{1}'::{schema}.job_state, completed_on = now(), output = $3::jsonb
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state = '{0}'::{schema}.job_state
            RETURNING 1
        )
        SELECT COUNT(*) from results;
        "#,
        JobState::Active,    // 0
        JobState::Completed, // 1
    )
}
//                   +                       +          +        +                     +             +             +              +                      +                                +                                      +              +                   +                                                                                                                              +
//                   id                  |  name   | priority | data |   state   | retry_limit | retry_count | retry_delay | retry_backoff |          start_after          |          started_on           | singleton_key | singleton_on | expire_in |          created_on           |         completed_on          |          keep_until           |         output         | dead_letter |  policy
// --------------------------------------+---------+----------+------+-----------+-------------+-------------+-------------+---------------+-------------------------------+-------------------------------+---------------+--------------+-----------+-------------------------------+-------------------------------+-------------------------------+------------------------+-------------+----------
//  71c7e215-0528-417c-951b-fc01b3fac4b3 | jobtype |        0 | null | completed |           0 |           0 |           0 | f             | 2024-09-29 09:23:09.502695+00 | 2024-09-29 09:23:09.514796+00 |               |              | 00:15:00  | 2024-09-29 09:23:09.502695+00 | 2024-09-29 09:23:09.526609+00 | 2024-10-13 09:23:09.502695+00 | {"result": "success!"} |             | standard
pub(crate) fn get_job_info(schema: &str) -> String {
    format!(
        r#"
        SELECT
            id,
            name,
            data,
            EXTRACT(epoch FROM expire_in)::float8 as expire_in,
            policy,
            priority,
            retry_limit,
            retry_delay,
            retry_count,
            retry_backoff,
            start_after,
            created_on as "created_at",
            started_on as "started_at",
            singleton_on as "singleton_at"
        FROM {schema}.job
        WHERE name = $1 and id = $2;
        "#,
    )
}
