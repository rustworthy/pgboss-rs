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
    format!("INSERT INTO {schema}.version (version) VALUES ({version}) ON CONFLICT DO NOTHING;")
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
            retention_seconds,
            deletion_seconds,
            dead_letter,
            created_on as created_at,
            updated_on as updated_at
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
            retention_seconds,
            deletion_seconds,
            dead_letter,
            created_on as created_at,
            updated_on as updated_at
        FROM {schema}.queue;
        "
    )
}

// $1 - jid
// $2 - job name (and, hence, queue name)
// $3 - data
// $4 - opts
pub(crate) fn create_job(schema: &str) -> String {
    format!(
        r#"
        INSERT INTO {schema}.job (
            id,
            name,
            data,
            priority,
            start_after,
            singleton_key,
            singleton_on,
            expire_seconds,
            deletion_seconds,
            keep_until,
            retry_limit,
            retry_delay,
            retry_backoff,
            retry_delay_max,
            policy,
            dead_letter
        )
        SELECT
            COALESCE($1, gen_random_uuid()) as id,
            $2,
            $3::jsonb,
            COALESCE(priority, 0) as priority,
            j.start_after,
            singleton_key,
            CASE
                WHEN singleton_for IS NOT NULL 
                THEN 'epoch'::timestamp + '1s'::interval * (singleton_for * floor(( date_part('epoch', now()) + COALESCE(singleton_offset,0)) / singleton_for ))
                ELSE NULL
            END as singleton_on,
            COALESCE(expire_in, q.expire_seconds) as expire_seconds,
            COALESCE(delete_after, q.deletion_seconds) as deletion_seconds,
            j.start_after + (COALESCE(retain_for, q.retention_seconds) * interval '1s') as keep_until,
            COALESCE(retry_limit, q.retry_limit) as retry_limit,
            COALESCE(retry_delay, q.retry_delay) as retry_delay,
            COALESCE(retry_backoff, q.retry_backoff, false) as retry_backoff,
            COALESCE(retry_delay_max, q.retry_delay_max) as retry_delay_max,
            q.policy,
            q.dead_letter
        FROM (
            SELECT * FROM json_to_recordset($4::json) as x (
                priority         integer,
                start_after      timestamptz,
                retry_limit      integer,
                retry_delay      integer,
                retry_delay_max  integer,
                retry_backoff    boolean,
                singleton_key    text,
                singleton_for    integer,
                singleton_offset integer,
                expire_in        integer,
                delete_after     integer,
                retain_for       integer
            )
        ) j JOIN {schema}.queue q ON q.name = $2
        ON CONFLICT DO NOTHING
        RETURNING id;
        "#
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
            state,
            policy,
            priority,
            retry_limit,
            retry_delay,
            retry_count,
            retry_backoff,
            start_after,
            created_on as created_at,
            started_on as started_at,
            singleton_on as singleton_at,
            completed_on as completed_at,
            singleton_key,
            dead_letter,
            keep_until,
            output;
        "#
    )
}

pub(crate) fn cancel_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH results AS (
            UPDATE {schema}.job
            SET completed_on = now(), state = '{0}'::{schema}.job_state
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state < '{1}'::{schema}.job_state      
            RETURNING 1
        )
        SELECT COUNT(*) from results;
        "#,
        JobState::Cancelled,
        JobState::Completed,
    )
}

pub(crate) fn resume_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH results AS (
            UPDATE {schema}.job
            SET completed_on = NULL, state = '{0}'::{schema}.job_state
            WHERE name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state = '{1}'::{schema}.job_state      
            RETURNING 1
        )
        SELECT COUNT(*) from results;
        "#,
        JobState::Created,
        JobState::Cancelled,
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
//                   id                  |  name       | priority | data |   state   | retry_limit | retry_count | retry_delay | retry_backoff |          start_after          |          started_on           | singleton_key | singleton_on | expire_in |          created_on           |         completed_on          |          keep_until           |         output                  | dead_letter |  policy
// --------------------------------------+-------------+----------+------+-----------+-------------+-------------+-------------+---------------+-------------------------------+-------------------------------+---------------+--------------+-----------+-------------------------------+-------------------------------+-------------------------------+---------------------------------+-------------+----------
//  71c7e215-0528-417c-951b-fc01b3fac4b3 | jobtype     |        0 | null | completed |           0 |           0 |           0 | f             | 2024-09-29 09:23:09.502695+00 | 2024-09-29 09:23:09.514796+00 |               |              | 00:15:00  | 2024-09-29 09:23:09.502695+00 | 2024-09-29 09:23:09.526609+00 | 2024-10-13 09:23:09.502695+00 | {"result": "success!"}          |             | standard
//  b4d1a8e0-c214-46aa-a796-7ac738cc0a76 | jobtype_dlq |        0 | null | active    |           0 |           0 |           0 | f             | 2024-10-02 20:11:13.056306+00 | 2024-10-02 20:11:13.068546+00 |               |              | 00:15:00  | 2024-10-02 20:11:13.056306+00 |                               | 2024-10-30 20:11:13.02769+00  | {"details": "testing again..."} |             |
pub(crate) fn get_job_info(schema: &str) -> String {
    format!(
        r#"
        SELECT
            id,
            name,
            data,
            EXTRACT(epoch FROM expire_in)::float8 as expire_in,
            state,
            policy,
            priority,
            retry_limit,
            retry_delay,
            retry_count,                                                    
            retry_backoff,
            start_after,
            created_on as created_at,
            started_on as started_at,
            singleton_on as singleton_at,
            completed_on as completed_at,
            singleton_key,
            keep_until,
            dead_letter,
            output
        FROM {schema}.job
        WHERE name = $1 and id = $2;
        "#,
    )
}

pub(crate) fn create_queue(schema: &str) -> String {
    format!("SELECT {schema}.create_queue($1, $2);")
}

pub(crate) fn delete_queue(schema: &str) -> String {
    format!("SELECT {schema}.delete_queue($1);")
}

// -------------------------- MAINTENANCE -------------------------------------
pub(crate) fn _g(schema: &str) -> String {
    format!(
        "UPDATE {schema}.job WHERE status = '{}' AND creaated_on + expire_in < now()",
        JobState::Active
    )
}
