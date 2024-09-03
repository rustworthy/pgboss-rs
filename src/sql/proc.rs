use crate::job::JobState;
use crate::queue::QueuePolicy;

pub(super) fn create_create_queue_function(schema: &str) -> String {
    format!(
        r#"
        CREATE OR REPLACE FUNCTION {schema}.create_queue(queue_name text, options jsonb)
        RETURNS VOID AS
        $$
        DECLARE
            table_name varchar := 'j' || encode(sha224(queue_name::bytea), 'hex');
            queue_created_on timestamptz;
        BEGIN
            INSERT INTO {schema}.queue (
                name,
                policy,
                retry_limit,
                retry_delay,
                retry_backoff,
                expire_seconds,
                retention_minutes,
                dead_letter,
                partition_name
            )
            VALUES (
                queue_name,
                options->>'policy',
                (options->>'retryLimit')::int,
                (options->>'retryDelay')::int,
                (options->>'retryBackoff')::bool,
                (options->>'expireInSeconds')::int,
                (options->>'retentionMinutes')::int,
                options->>'deadLetter',
                table_name
            ) ON CONFLICT DO NOTHING RETURNING created_on INTO queue_created_on;
            
            IF queue_created_on IS NULL THEN
                RETURN;
            END IF;
        
            EXECUTE format('CREATE TABLE {schema}.%I (LIKE {schema}.job INCLUDING DEFAULTS)', table_name);
            EXECUTE format('ALTER TABLE {schema}.%I ADD PRIMARY KEY (name, id)', table_name);
            EXECUTE format('ALTER TABLE {schema}.%I ADD CONSTRAINT q_fkey FOREIGN KEY (name) REFERENCES {schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', table_name);
            EXECUTE format('ALTER TABLE {schema}.%I ADD CONSTRAINT dlq_fkey FOREIGN KEY (dead_letter) REFERENCES {schema}.queue (name) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED', table_name);
            
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i1 ON {schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''{0}'' AND policy = ''{1}''', table_name);
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i2 ON {schema}.%1$I (name, COALESCE(singleton_key, '''')) WHERE state = ''{2}'' AND policy = ''{3}''', table_name);
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i3 ON {schema}.%1$I (name, state, COALESCE(singleton_key, '''')) WHERE state <= ''{4}'' AND policy = ''{5}''', table_name);
            EXECUTE format('CREATE UNIQUE INDEX %1$s_i4 ON {schema}.%1$I (name, singleton_on, COALESCE(singleton_key, '''')) WHERE state <> ''{6}'' AND singleton_on IS NOT NULL', table_name);
            
            EXECUTE format('CREATE INDEX %1$s_i5 ON {schema}.%1$I (name, start_after) INCLUDE (priority, created_on, id) WHERE state < ''{7}''', table_name);
            
            EXECUTE format('ALTER TABLE {schema}.%I ADD CONSTRAINT cjc CHECK (name=%L)', table_name, queue_name);
            EXECUTE format('ALTER TABLE {schema}.job ATTACH PARTITION {schema}.%I FOR VALUES IN (%L)', table_name, queue_name);
            
        END;
        $$
        LANGUAGE plpgsql;
        "#,
        JobState::Created,      // 0
        QueuePolicy::Short,     // 1
        JobState::Active,       // 2
        QueuePolicy::Singleton, // 3
        JobState::Active,       // 4
        QueuePolicy::Stately,   // 5
        JobState::Cancelled,    // 6
        JobState::Active,       // 7
    )
}

pub(crate) fn create_queue(schema: &str) -> String {
    format!("SELECT {schema}.create_queue($1, $2);")
}

pub(super) fn create_delete_queue_function(schema: &str) -> String {
    format!(
        r#"

        CREATE OR REPLACE FUNCTION {schema}.delete_queue(queue_name text)
        RETURNS VOID AS
        $$
        DECLARE
            table_name varchar;
        BEGIN
            WITH deleted as (
                DELETE FROM {schema}.queue WHERE name = queue_name RETURNING partition_name
            )
            SELECT partition_name FROM deleted INTO table_name;
            EXECUTE format('DROP TABLE IF EXISTS {schema}.%I', table_name);
        END;
        $$
        LANGUAGE plpgsql;
        "#
    )
}

pub(crate) fn delete_queue(schema: &str) -> String {
    format!("SELECT {schema}.delete_queue($1);")
}

pub(crate) fn create_create_job_function(schema: &str) -> String {
    format!(
        r#"
        CREATE OR REPLACE FUNCTION {schema}.create_job(job_id uuid, name text, data jsonb, options jsonb, OUT inserted_id uuid)
        RETURNS uuid AS
        $$
        BEGIN
        INSERT INTO {schema}.job (
            id,
            name,
            data,
            priority,
            start_after,
            singleton_key,
            singleton_on,
            dead_letter,
            expire_in,
            keep_until,
            retry_limit,
            retry_delay,
            retry_backoff,
            policy
        )
        SELECT
            job_id_provided_or_generated,
            j.name,
            job_data,
            priority,
            start_after,
            singleton_key,
            singleton_on,
            COALESCE(j.dead_letter, q.dead_letter) as dead_letter,
            CASE
                WHEN expire_in IS NOT NULL THEN CAST(expire_in as interval)
                WHEN q.expire_seconds IS NOT NULL THEN q.expire_seconds * interval '1s'
                ELSE interval '15 minutes'
            END as expire_in,
            CASE
                WHEN right(keep_until, 1) = 'Z' THEN CAST(keep_until as timestamptz)
                ELSE start_after + CAST(COALESCE(keep_until, (q.retention_minutes * 60)::text, '14 days') as interval)
            END as keep_until,
            COALESCE(j.retry_limit, q.retry_limit, 2) as retry_limit,
            CASE
                WHEN COALESCE(j.retry_backoff, q.retry_backoff, false)
                THEN GREATEST(COALESCE(j.retry_delay, q.retry_delay), 1)
                ELSE COALESCE(j.retry_delay, q.retry_delay, 0)
            END as retry_delay,
            COALESCE(j.retry_backoff, q.retry_backoff, false) as retry_backoff,
            q.policy
        FROM (
            SELECT 
                COALESCE(job_id, gen_random_uuid()) as job_id_provided_or_generated,
                name,
                data as job_data,
                COALESCE((options->>'priority')::int, 0) as priority,
                CASE
                    WHEN right(options->>'start_after', 1) = 'Z' THEN CAST(options->>'start_after' as timestamptz)
                    ELSE now() + CAST(COALESCE(options->>'start_after','0') as interval)
                END as start_after,
                options->>'singleton_key' as singleton_key,
                CASE
                    WHEN (options->>'singleton_on')::integer IS NOT NULL THEN 'epoch'::timestamp + '1 second'::interval * ((options->>'singleton_on')::integer * floor((date_part('epoch', now()) + (options->>'singleton_offset')::integer) / (options->>'singleton_on')::integer))
                    ELSE NULL
                END as singleton_on,
                options->>'dead_letter' as dead_letter,
                options->>'expire_in' as expire_in,
                options->>'keep_until' as keep_until,
                (options->>'retry_limit')::integer as retry_limit,
                (options->>'retry_delay')::integer as retry_delay,
                (options->>'retry_backoff')::boolean as retry_backoff
            ) j JOIN {schema}.queue q ON j.name = q.name
        RETURNING id INTO inserted_id;
        END;
        $$
        LANGUAGE plpgsql;
        "#
    )
}
///                  id                  |      name      | priority | data |  state  | retry_limit | retry_count | retry_delay | retry_backoff |          start_after          | started_on | singleton_key | singleton_on | expire_in |          created_on           | completed_on |          keep_until           | output | dead_letter |  policy  
/// --------------------------------------+----------------+----------+------+---------+-------------+-------------+-------------+---------------+-------------------------------+------------+---------------+--------------+-----------+-------------------------------+--------------+-------------------------------+--------+-------------+----------
/// cb1144a7-5fd3-49df-a691-01ba1e1f06a7 | send_job_queue |        0 | {}   | created |           2 |           0 |           0 | f             | 2024-08-27 19:39:24.933367+00 |            |               |              | 00:15:00  | 2024-08-27 19:39:24.933367+00 |              | 2024-09-10 19:39:24.933367+00 |        |             | standard
/// (1 row)
pub(crate) fn create_job(schema: &str) -> String {
    format!("SELECT {schema}.create_job($1, $2, $3, $4);")
}
