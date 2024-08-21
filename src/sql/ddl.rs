use crate::job::{JobState, QueuePolicy};

pub(crate) fn create_schema(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {};", schema)
}

pub(super) fn create_job_state_enum(schema: &str) -> String {
    format!(
        "
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type typ INNER JOIN pg_namespace nsp on (typ.typnamespace = nsp.oid) where typ.typname = 'job_state' AND nsp.nspname = '{schema}') THEN
                CREATE TYPE {schema}.job_state AS ENUM ('{}', '{}', '{}', '{}', '{}', '{}');
            END IF;
        END $$;
        ",
        JobState::Created,
        JobState::Retry,
        JobState::Active,
        JobState::Completed,
        JobState::Cancelled,
        JobState::Failed,
    )
}

pub(super) fn create_version_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.version (
            version int primary key,
            maintained_on timestamptz,
            cron_on timestamptz,
            monitored_on timestamptz
        );
        "
    )
}

pub(super) fn create_queue_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.queue (
            name text,
            policy text,
            retry_limit int,
            retry_delay int,
            retry_backoff bool,
            expire_seconds int,
            retention_minutes int,
            dead_letter text REFERENCES {schema}.queue (name),
            partition_name text,
            created_on timestamptz not null default now(),
            updated_on timestamptz not null default now(),
            PRIMARY KEY (name) 
        );
        "
    )
}

pub(super) fn create_subscription_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.subscription (
            event text not null,
            name text not null REFERENCES {schema}.queue ON DELETE CASCADE,
            created_on timestamptz not null default now(),
            updated_on timestamptz not null default now(),
            PRIMARY KEY (event, name)
        );
        "
    )
}

pub(super) fn create_job_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.job (
            id uuid not null default gen_random_uuid(),
            name text not null,
            priority integer not null default(0),
            data jsonb,
            state {schema}.job_state not null default('{}'),
            retry_limit integer not null default(0),
            retry_count integer not null default(0),
            retry_delay integer not null default(0),
            retry_backoff boolean not null default false,
            start_after timestamptz not null default now(),
            started_on timestamptz,
            singleton_key text,
            singleton_on timestamp without time zone,
            expire_in interval not null default interval '15 minutes',
            created_on timestamptz not null default now(),
            completed_on timestamptz,
            keep_until timestamptz not null default now() + interval '14 days',
            output jsonb,
            dead_letter text,
            policy text      
        ) PARTITION BY LIST (name);
        ",
        JobState::Created
    )
}

pub(super) fn create_archive_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.archive (
            LIKE {schema}.job,
            archived_on timestamptz not null default now(),
            PRIMARY KEY (name, id)
        );
        CREATE INDEX IF NOT EXISTS archive_i1 ON {schema}.archive (archived_on);
        "
    )
}

pub(super) fn create_create_queue_function(schema: &str) -> String {
    format!(
        r#"
        CREATE OR REPLACE FUNCTION {schema}.create_queue (queue_name text, options json)
        RETURNS VOID AS
        $$
        DECLARE
            table_name varchar := 'j' || encode(sha224(queue_name::bytea), 'hex');
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
            );
        
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
