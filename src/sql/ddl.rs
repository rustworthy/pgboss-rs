use crate::{QueuePolicy, job::JobState};

pub(crate) fn create_schema(schema: &str) -> String {
    format!("CREATE SCHEMA {schema};")
}

pub(super) fn create_job_state_enum(schema: &str) -> String {
    format!(
        "CREATE TYPE {schema}.job_state AS ENUM ('{}', '{}', '{}', '{}', '{}', '{}');",
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
        CREATE TABLE {schema}.version (
            version int primary key,
            cron_on timestamptz
        );
        "
    )
}

pub(super) fn create_queue_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE {schema}.queue (
            name                text not null,
            policy              text not null,
            retry_limit         int not null,
            retry_delay         int not null,
            retry_backoff       bool not null,
            retry_delay_max     int,
            expire_seconds      int not null,
            retention_seconds   int not null,
            deletion_seconds    int not null,
            dead_letter         text references {schema}.queue (name) check (dead_letter is distinct from name),
            partition           bool not null,
            table_name          text not null,
            deferred_count      int not null default 0,
            queued_count        int not null default 0,
            warning_queued      int not null default 0,
            active_count        int not null default 0,
            total_count         int not null default 0,
            singletons_active   text[],
            monitor_on          timestamptz,
            maintain_on         timestamptz,
            created_on          timestamptz not null default now(),
            updated_on          timestamptz not null default now(),

            PRIMARY KEY (name)
        );
        "
    )
}

pub(super) fn create_schedule_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE {schema}.schedule (
            name                text references {schema}.queue on delete cascade,
            key                 text not null default '',
            cron                text not null,
            timezone            text,
            data                jsonb,
            options             jsonb,
            created_on          timestamptz not null default now(),
            updated_on          timestamptz not null default now(),

            PRIMARY KEY (name, key)
        );
        "
    )
}

pub(super) fn create_subscription_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE {schema}.subscription (
            event               text not null,
            name                text not null references {schema}.queue on delete cascade,
            created_on          timestamptz not null default now(),
            updated_on          timestamptz not null default now(),

            PRIMARY KEY (event, name)
        );
        "
    )
}

pub(super) fn create_job_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE {schema}.job (
            id                  uuid not null default gen_random_uuid(),
            name                text not null,
            priority            int not null default(0),
            data                jsonb,
            state               {schema}.job_state not null default '{}',
            retry_limit         int not null default 2,
            retry_count         int not null default 0,
            retry_delay         int not null default 0,
            retry_backoff       bool not null default false,
            retry_delay_max     int,
            expire_seconds      int not null default 900, -- 15 mins
            deletion_seconds    int not null default 604800, -- 7days
            singleton_key       text,
            singleton_on        timestamp without time zone,
            start_after         timestamptz not null default now(),
            created_on          timestamptz not null default now(),
            started_on          timestamptz,
            completed_on        timestamptz,
            keep_until          timestamptz not null default now() + interval '1209600', -- 14 days
            output              jsonb,
            dead_letter         text,
            policy              text,

            PRIMARY KEY (name, id)
        ) PARTITION BY LIST (name);
        ",
        JobState::Created
    )
}

pub(super) fn create_job_common_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE {schema}.job_common (
            LIKE {schema}.job INCLUDING GENERATED INCLUDING DEFAULTS
        );

        ALTER TABLE {schema}.job_common ADD PRIMARY KEY (name, id);

        ALTER TABLE {schema}.job_common ADD CONSTRAINT
            q_fkey FOREIGN KEY (name) REFERENCES {schema}.queue (name)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

        ALTER TABLE {schema}.job_common ADD CONSTRAINT
            dlq_fkey FOREIGN KEY (dead_letter) REFERENCES {schema}.queue (name)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

        CREATE UNIQUE INDEX job_i1 ON {schema}.job_common (name, COALESCE(singleton_key, ''))
            WHERE state = '{0}' AND policy = '{1}';

        CREATE UNIQUE INDEX job_i2 ON {schema}.job_common (name, COALESCE(singleton_key, ''))
            WHERE state = '{2}' AND policy = '{3}';

        CREATE UNIQUE INDEX job_i3 ON {schema}.job_common (name, state, COALESCE(singleton_key, ''))
            WHERE state <= '{2}' AND policy = '{4}';

        CREATE UNIQUE INDEX job_i4 ON {schema}.job_common (name, singleton_on, COALESCE(singleton_key, ''))
            WHERE state <> '{5}' AND singleton_on IS NOT NULL;

        CREATE INDEX job_i5 ON {schema}.job_common (name, start_after) INCLUDE (priority, created_on, id)
            WHERE state < '{2}';

        CREATE UNIQUE INDEX job_i6 ON {schema}.job_common (name, COALESCE(singleton_key, ''))
            WHERE state <= '{2}' AND policy = '{6}';

        ALTER TABLE {schema}.job ATTACH PARTITION {schema}.job_common DEFAULT;
        ",
        JobState::Created,      // 0
        QueuePolicy::Short,     // 1
        JobState::Active,       // 2
        QueuePolicy::Singleton, // 3
        QueuePolicy::Stately,   // 4
        JobState::Cancelled,    // 5
        QueuePolicy::Exclusive, // 6
    )
}

pub(crate) mod proc {
    use crate::{JobState, QueuePolicy};

    // To make sure this matches the resulting def'n in the original pgboss package:
    // ```sql
    // SELECT pg_get_functiondef((SELECT oid FROM pg_proc WHERE proname = 'create_queue'));
    // ```
    pub(crate) fn create_create_queue_function(schema: &str) -> String {
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

    pub(crate) fn create_delete_queue_function(schema: &str) -> String {
        format!(
            r#"
                CREATE FUNCTION {schema}.delete_queue(queue_name text)
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
}
