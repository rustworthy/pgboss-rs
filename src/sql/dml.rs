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
    get_queues(schema, Some("name = $1"))
}

pub(crate) fn get_all_queues(schema: &str) -> String {
    get_queues(schema, None)
}

pub(crate) fn get_queues(schema: &str, where_clause: Option<&str>) -> String {
    let where_clause = if let Some(clause) = where_clause {
        format!("WHERE {clause}")
    } else {
        String::default()
    };
    format!(
        "
        SELECT
            name,
            policy,
            retry_limit,
            retry_delay,
            retry_backoff,
            retry_delay_max,
            expire_seconds,
            retention_seconds,
            deletion_seconds,
            partition,
            dead_letter,
            deferred_count,
            warning_queued,
            queued_count,
            active_count,
            total_count,
            singletons_active,
            table_name,
            created_on as created_at,
            updated_on as updated_at
        FROM {schema}.queue
        {where_clause};
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
            COALESCE(j.priority, 0) as priority,
            COALESCE(j.start_after, now()),
            j.singleton_key,
            CASE
                WHEN j.singleton_for IS NOT NULL
                THEN 'epoch'::timestamp + '1s'::interval * (j.singleton_for * floor(( date_part('epoch', now()) + COALESCE(j.singleton_offset,0)) / j.singleton_for ))
                ELSE NULL
            END as singleton_on,
            COALESCE(j.expire_in, q.expire_seconds) as expire_seconds,
            COALESCE(j.delete_after, q.deletion_seconds) as deletion_seconds,
            coalesce(j.start_after, now()) + (COALESCE(j.retain_for, q.retention_seconds) * interval '1s') as keep_until,
            COALESCE(j.retry_limit, q.retry_limit) as retry_limit,
            COALESCE(j.retry_delay, q.retry_delay) as retry_delay,
            COALESCE(j.retry_backoff, q.retry_backoff, false) as retry_backoff,
            COALESCE(j.retry_delay_max, q.retry_delay_max) as retry_delay_max,
            q.policy,
            q.dead_letter
        FROM (
            SELECT * FROM jsonb_to_record($4) as x (
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
        RETURNING id;
        "#
    )
}

// $1 -  queue name
// $2 -  limit
pub(crate) fn fetch_jobs(schema: &str) -> String {
    format!(
        r#"
        WITH next AS (
            SELECT id FROM {schema}.job
            WHERE name = $1 AND state < '{0}' AND start_after < now()
            ORDER BY priority DESC, created_on, id
            LIMIT $2
            FOR UPDATE
            SKIP LOCKED
        )
        UPDATE {schema}.job j SET
            state = '{0}',
            started_on = now(),
            retry_count = CASE WHEN started_on IS NULL THEN retry_count ELSE retry_count + 1 END
        FROM next
        WHERE name = $1 AND j.id = next.id
        RETURNING
            j.id,
            name,
            priority,
            data,
            state,
            retry_limit,
            retry_count,
            retry_delay,
            retry_backoff,
            retry_delay_max,
            expire_seconds,
            deletion_seconds,
            singleton_key,
            singleton_on as singleton_at,
            start_after,
            created_on as created_at,
            started_on as started_at,
            completed_on as completed_at,
            keep_until,
            output,
            dead_letter,
            policy;
        "#,
        JobState::Active
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
            RETURNING *
        )
        SELECT COUNT(*) from results;
        "#,
        JobState::Active,    // 0
        JobState::Completed, // 1
    )
}

pub(crate) fn get_job_info(schema: &str) -> String {
    format!(
        r#"
        SELECT
            id,
            name,
            priority,
            data,
            state,
            retry_limit,
            retry_count,
            retry_delay,
            retry_backoff,
            retry_delay_max,
            expire_seconds,
            deletion_seconds,
            singleton_key,
            singleton_on as singleton_at,
            start_after,
            created_on as created_at,
            started_on as started_at,
            completed_on as completed_at,
            keep_until,
            output,
            dead_letter,
            policy
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

pub(crate) fn fail_jobs_by_jids(schema: &str) -> String {
    let where_clause = format!(
        "name = $1 AND id IN (SELECT UNNEST($2::uuid[])) AND state < '{0}'",
        JobState::Completed
    );
    let failure_details = "$3::jsonb";
    fail_jobs(schema, where_clause.as_str(), failure_details)
}

pub(crate) fn fail_jobs_by_timeout(schema: &str) -> String {
    // TODO: we will actually be setting monitor on on the queues
    // TODO: returning those names that are ready for maintence, and
    // TODO: so here we will be also specifying queue names like:
    // TODO: `AND ANY(ARRAY['queue1,queue2,etc']::text[])
    let where_clause = format!(
        r#"
        state = '{0}'
        AND (started_on + expire_seconds * interval '1s') < now()
        "#,
        JobState::Active
    );
    // same message as PgBoss is using
    let failure_details = r#"'{ "value": { "message": "job timed out" } }'::jsonb"#;
    fail_jobs(schema, where_clause.as_str(), failure_details)
}

pub(crate) fn fail_jobs(schema: &str, where_clause: &str, failure_details: &str) -> String {
    format!(
        r#"
        WITH deleted_jobs AS (
            DELETE FROM {schema}.job
            WHERE {where_clause}
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
                retry_delay_max,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_seconds,
                deletion_seconds,
                created_on,
                completed_on,
                keep_until,
                policy,
                output,
                dead_letter
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
                retry_delay_max,
                CASE WHEN retry_count = retry_limit THEN start_after
                    WHEN NOT retry_backoff THEN now() + retry_delay * interval '1'
                    ELSE now() + LEAST(
                    retry_delay_max,
                    retry_delay + (
                        2 ^ LEAST(16, retry_count + 1) / 2 +
                        2 ^ LEAST(16, retry_count + 1) / 2 * random()
                    )
                    ) * interval '1s'
                END as start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_seconds,
                deletion_seconds,
                created_on,
                CASE WHEN retry_count < retry_limit THEN NULL ELSE now() END as completed_on,
                keep_until,
                policy,
                {failure_details},
                dead_letter
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
                retry_delay_max,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_seconds,
                deletion_seconds,
                created_on,
                completed_on,
                keep_until,
                policy,
                output,
                dead_letter
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
                retry_delay_max,
                start_after,
                started_on,
                singleton_key,
                singleton_on,
                expire_seconds,
                deletion_seconds,
                created_on,
                now() as completed_on,
                keep_until,
                policy,
                {failure_details},
                dead_letter
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
            INSERT INTO {schema}.job (
                name,
                data,
                output,
                retry_limit,
                retry_backoff,
                retry_delay,
                keep_until,
                deletion_seconds
            )
            SELECT
                r.dead_letter,
                data,
                output,
                q.retry_limit,
                q.retry_backoff,
                q.retry_delay,
                now() + q.retention_seconds * interval '1s',
                q.deletion_seconds
            FROM results r
                JOIN {schema}.queue q ON q.name = r.dead_letter
            WHERE state = '{1}'::{schema}.job_state
        )
        SELECT COUNT(*) FROM results
        "#,
        JobState::Retry,
        JobState::Failed,
    )
}

// -------------------------- MAINTENANCE -------------------------------------
pub(crate) fn _g(schema: &str) -> String {
    format!(
        "UPDATE {schema}.job WHERE status = '{}' AND creaated_on + expire_in < now()",
        JobState::Active
    )
}
