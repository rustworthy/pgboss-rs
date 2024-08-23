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

pub(crate) fn create_queue(schema: &str) -> String {
    format!("SELECT {schema}.create_queue($1, $2);")
}
