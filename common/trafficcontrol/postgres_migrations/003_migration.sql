CREATE TABLE {{schema}}.mbox_traffic_migration_jobs (
    instance_id text NOT NULL,
    migration_id text NOT NULL,
    source_fingerprint bytea NOT NULL,
    source_size bigint NOT NULL,
    mode text NOT NULL,
    from_bucket bigint NOT NULL,
    to_bucket bigint NOT NULL,
    active_config_revision text NOT NULL,
    routing_fingerprint bytea NOT NULL,
    source_target_available_from timestamptz NOT NULL,
    source_destination_available_from timestamptz NOT NULL,
    effective_target_available_from timestamptz NOT NULL,
    effective_destination_available_from timestamptz NOT NULL,
    summary_cursor bytea NOT NULL DEFAULT ''::bytea,
    target_cursor bytea NOT NULL DEFAULT ''::bytea,
    summary_processed bigint NOT NULL DEFAULT 0,
    summary_inserted bigint NOT NULL DEFAULT 0,
    summary_skipped bigint NOT NULL DEFAULT 0,
    target_processed bigint NOT NULL DEFAULT 0,
    target_inserted bigint NOT NULL DEFAULT 0,
    target_skipped bigint NOT NULL DEFAULT 0,
    summary_complete boolean NOT NULL DEFAULT false,
    target_complete boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CONSTRAINT mbox_traffic_migration_jobs_pkey
        PRIMARY KEY (instance_id, migration_id),
    CONSTRAINT mbox_traffic_migration_jobs_instance_fk
        FOREIGN KEY (instance_id)
        REFERENCES {{schema}}.mbox_traffic_instances (instance_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_migration_jobs_id_format
        CHECK (migration_id ~ '^[0-9a-f]{64}$'),
    CONSTRAINT mbox_traffic_migration_jobs_source_fingerprint_length
        CHECK (octet_length(source_fingerprint) = 32),
    CONSTRAINT mbox_traffic_migration_jobs_source_size
        CHECK (source_size >= 0),
    CONSTRAINT mbox_traffic_migration_jobs_mode
        CHECK (mode = 'restore'),
    CONSTRAINT mbox_traffic_migration_jobs_bounds
        CHECK (from_bucket < to_bucket),
    CONSTRAINT mbox_traffic_migration_jobs_active_revision_format
        CHECK (active_config_revision ~ '^[0-9a-f]{32}$'),
    CONSTRAINT mbox_traffic_migration_jobs_routing_fingerprint_length
        CHECK (octet_length(routing_fingerprint) = 32),
    CONSTRAINT mbox_traffic_migration_jobs_source_availability_order
        CHECK (source_destination_available_from >= source_target_available_from),
    CONSTRAINT mbox_traffic_migration_jobs_effective_availability_order
        CHECK (effective_destination_available_from >= effective_target_available_from),
    CONSTRAINT mbox_traffic_migration_jobs_effective_availability_floor
        CHECK (
            effective_target_available_from >= source_target_available_from
            AND effective_destination_available_from >= source_destination_available_from
        ),
    CONSTRAINT mbox_traffic_migration_jobs_summary_cursor_length
        CHECK (octet_length(summary_cursor) IN (0, 40)),
    CONSTRAINT mbox_traffic_migration_jobs_target_cursor_length
        CHECK (octet_length(target_cursor) IN (0, 40)),
    CONSTRAINT mbox_traffic_migration_jobs_summary_counts
        CHECK (
            summary_processed >= 0
            AND summary_inserted >= 0
            AND summary_skipped >= 0
            AND summary_inserted + summary_skipped = summary_processed
        ),
    CONSTRAINT mbox_traffic_migration_jobs_target_counts
        CHECK (
            target_processed >= 0
            AND target_inserted >= 0
            AND target_skipped >= 0
            AND target_inserted + target_skipped = target_processed
        ),
    CONSTRAINT mbox_traffic_migration_jobs_completion
        CHECK (
            (completed_at IS NOT NULL)
            = (summary_complete AND target_complete)
        )
);
-- mbox:statement
CREATE INDEX mbox_traffic_migration_jobs_source_idx
    ON {{schema}}.mbox_traffic_migration_jobs
    (instance_id, source_fingerprint, created_at);
-- mbox:statement
CREATE TABLE {{schema}}.mbox_traffic_migration_revisions (
    instance_id text NOT NULL,
    migration_id text NOT NULL,
    config_revision text NOT NULL,
    first_bucket bigint NOT NULL,
    last_bucket bigint NOT NULL,
    summary_records bigint NOT NULL,
    target_records bigint NOT NULL,
    CONSTRAINT mbox_traffic_migration_revisions_pkey
        PRIMARY KEY (instance_id, migration_id, config_revision),
    CONSTRAINT mbox_traffic_migration_revisions_job_fk
        FOREIGN KEY (instance_id, migration_id)
        REFERENCES {{schema}}.mbox_traffic_migration_jobs
        (instance_id, migration_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_migration_revisions_revision_format
        CHECK (config_revision ~ '^[0-9a-f]{32}$'),
    CONSTRAINT mbox_traffic_migration_revisions_bucket_order
        CHECK (first_bucket <= last_bucket),
    CONSTRAINT mbox_traffic_migration_revisions_counts
        CHECK (
            summary_records >= 0
            AND target_records >= 0
            AND summary_records + target_records > 0
        )
);
-- mbox:statement
CREATE TABLE {{schema}}.mbox_traffic_migration_batches (
    instance_id text NOT NULL,
    migration_id text NOT NULL,
    kind text NOT NULL,
    cursor_start bytea NOT NULL,
    cursor_end bytea NOT NULL,
    payload_sha256 bytea NOT NULL,
    record_count integer NOT NULL,
    inserted_count integer NOT NULL,
    skipped_count integer NOT NULL,
    first_bucket bigint NOT NULL,
    last_bucket bigint NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT mbox_traffic_migration_batches_pkey
        PRIMARY KEY (instance_id, migration_id, kind, cursor_end),
    CONSTRAINT mbox_traffic_migration_batches_start_key
        UNIQUE (instance_id, migration_id, kind, cursor_start),
    CONSTRAINT mbox_traffic_migration_batches_job_fk
        FOREIGN KEY (instance_id, migration_id)
        REFERENCES {{schema}}.mbox_traffic_migration_jobs
        (instance_id, migration_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_migration_batches_kind
        CHECK (kind IN ('summary', 'target')),
    CONSTRAINT mbox_traffic_migration_batches_cursors
        CHECK (
            octet_length(cursor_start) IN (0, 40)
            AND octet_length(cursor_end) = 40
            AND cursor_end > cursor_start
        ),
    CONSTRAINT mbox_traffic_migration_batches_payload_length
        CHECK (octet_length(payload_sha256) = 32),
    CONSTRAINT mbox_traffic_migration_batches_record_count
        CHECK (record_count >= 1 AND record_count <= 10000),
    CONSTRAINT mbox_traffic_migration_batches_counts
        CHECK (
            inserted_count >= 0
            AND skipped_count >= 0
            AND inserted_count + skipped_count = record_count
        ),
    CONSTRAINT mbox_traffic_migration_batches_bucket_order
        CHECK (first_bucket <= last_bucket)
);
