use crate::{QueuePolicy, job::JobState};

pub(crate) fn create_schema(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {schema};")
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
            cron_on timestamptz
        );
        "
    )
}

pub(super) fn create_queue_table(schema: &str) -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {schema}.queue (
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
        CREATE TABLE IF NOT EXISTS {schema}.schedule (
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
        CREATE TABLE IF NOT EXISTS {schema}.subscription (
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
        CREATE TABLE IF NOT EXISTS {schema}.job (
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
        CREATE TABLE IF NOT EXISTS {schema}.job_common (
            LIKE {schema}.job INCLUDING GENERATED INCLUDING DEFAULTS
            PRIMARY KEY (name, id)
        );

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

        CREATE UNIQUE INDEX job_i3 ON ${schema}.job_common (name, state, COALESCE(singleton_key, ''))
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
