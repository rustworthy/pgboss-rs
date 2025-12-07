use crate::job::JobState;
use crate::queue::QueuePolicy;
use askama::Template;

// To make sure this matches the resulting def'n in the original pgboss package:
// ```sql
// SELECT pg_get_functiondef((SELECT oid FROM pg_proc WHERE proname = 'create_queue'));
// ```
pub(super) fn create_create_queue_function(schema: &str) -> String {
    format!(
        r#"
        CREATE FUNCTION {schema}.create_queue(queue_name text, options jsonb)
        RETURNS VOID AS
        $$
        DECLARE
        tablename varchar :=
            CASE
                WHEN options->>'partition' = 'true'
                THEN 'j' || encode(sha224(queue_name::bytea), 'hex')
                ELSE 'job_common'
            END;
        queue_created_on timestamptz;
        BEGIN

            WITH q as (
                INSERT INTO {schema}.queue (
                    name,
                    policy,
                    retry_limit,
                    retry_delay,
                    retry_backoff,
                    retry_delay_max,
                    expire_seconds,
                    retention_seconds,
                    deletion_seconds,
                    warning_queued,
                    dead_letter,
                    partition,
                    table_name
                )
                VALUES (
                    queue_name,
                    options->>'policy',
                    COALESCE((options->>'retryLimit')::int, 2),
                    COALESCE((options->>'retryDelay')::int, 0),
                    COALESCE((options->>'retryBackoff')::bool, false),
                    (options->>'retryDelayMax')::int,
                    COALESCE((options->>'expireInSeconds')::int, 900),       -- 15 mins
                    COALESCE((options->>'retentionSeconds')::int, 1209600),  -- 14 days
                    COALESCE((options->>'deleteAfterSeconds')::int, 604800), -- 7 days
                    COALESCE((options->>'warningQueueSize')::int, 0),
                    options->>'deadLetter',
                    COALESCE((options->>'partition')::bool, false),
                    tablename
                )
                ON CONFLICT DO NOTHING
                RETURNING created_on
            )
            SELECT created_on into queue_created_on from q;

            -- queue either existed, or has been registered to work with the default "job_common" partition
            IF queue_created_on IS NULL OR options->>'partition' IS DISTINCT FROM 'true' THEN
                RETURN;
            END IF;

            -- queue has been registered and requires a dedicated job table partition
            EXECUTE format('CREATE TABLE {schema}.%I (LIKE {schema}.job INCLUDING DEFAULTS)', tablename);

            EXECUTE format('ALTER TABLE {schema}.%1$I ADD PRIMARY KEY (name, id)', tablename);
            EXECUTE format('ALTER TABLE {schema}.%1$I ADD CONSTRAINT q_fkey FOREIGN KEY (name) REFERENCES {schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', tablename);
            EXECUTE format('ALTER TABLE {schema}.%1$I ADD CONSTRAINT dlq_fkey FOREIGN KEY (dead_letter) REFERENCES {schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', tablename);

            EXECUTE format('CREATE INDEX %1$s_i5 ON {schema}.%1$I (name, start_after) INCLUDE (priority, created_on, id) WHERE state < ''{2}''', tablename);
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i4 ON {schema}.%1$I (name, singleton_on, COALESCE(singleton_key, '''')) WHERE state <> ''{5}'' AND singleton_on IS NOT NULL', tablename);


            IF options->>'policy' = 'short' THEN
                EXECUTE format('CREATE UNIQUE INDEX %1$s_i1 ON {schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''{0}'' AND policy = ''{1}''', tablename);
            ELSIF options->>'policy' = 'singleton' THEN
                EXECUTE format('CREATE UNIQUE INDEX %1$s_i2 ON {schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''{2}'' AND policy = ''{3}''', tablename);
            ELSIF options->>'policy' = 'stately' THEN
                EXECUTE format('CREATE UNIQUE INDEX %1$s_i3 ON {schema}.%1$I (name, state, COALESCE(singleton_key, '''')) WHERE state <= ''{2}'' AND policy = ''{4}''', tablename);
            ELSIF options->>'policy' = 'exclusive' THEN
                EXECUTE format('CREATE UNIQUE INDEX %1$s_i6 ON {schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state <= ''{2}'' AND policy = ''{6}''', tablename);
            END IF;

            EXECUTE format('ALTER TABLE {schema}.%I ADD CONSTRAINT cjc CHECK (name=%L)', tablename, queue_name);
            EXECUTE format('ALTER TABLE {schema}.job ATTACH PARTITION {schema}.%I FOR VALUES IN (%L)', tablename, queue_name);
        END;
        $$
        LANGUAGE plpgsql;
        "#,
        JobState::Created,      // 0
        QueuePolicy::Short,     // 1
        JobState::Active,       // 2
        QueuePolicy::Singleton, // 3
        QueuePolicy::Stately,   // 4
        JobState::Cancelled,    // 5
        QueuePolicy::Exclusive, // 6
    )
}

pub(crate) fn create_queue(schema: &str) -> String {
    format!("SELECT {schema}.create_queue($1, $2);")
}

pub(super) fn create_delete_queue_function(schema: &str) -> String {
    format!(
        r#"
        CREATE OR REPLACE FUNCTION ${schema}.delete_queue(queue_name text)
        RETURNS VOID AS
        $$
        DECLARE
            v_table varchar;
            v_partition bool;
        BEGIN
            SELECT table_name, partition
            FROM {schema}.queue
            WHERE name = queue_name
            INTO v_table, v_partition;

            IF v_partition THEN
                EXECUTE format('DROP TABLE IF EXISTS {schema}.%I', v_table);
            ELSE
                EXECUTE format('DELETE FROM {schema}.%I WHERE name = %L', v_table, queue_name);
            END IF;

            DELETE FROM {schema}.queue WHERE name = queue_name;
        END;
        $$
        LANGUAGE plpgsql;
        "#
    )
}

pub(crate) fn delete_queue(schema: &str) -> String {
    format!("SELECT {schema}.delete_queue($1);")
}

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
