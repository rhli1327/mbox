CREATE TABLE {{schema}}.mbox_traffic_ingest_batches (
    instance_id text NOT NULL,
    batch_id uuid NOT NULL,
    payload_sha256 bytea NOT NULL,
    record_count integer NOT NULL,
    first_bucket timestamptz,
    last_bucket timestamptz,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, batch_id),
    CONSTRAINT mbox_traffic_ingest_batches_instance_fk
        FOREIGN KEY (instance_id)
        REFERENCES {{schema}}.mbox_traffic_instances(instance_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_ingest_batches_payload_length
        CHECK (octet_length(payload_sha256) = 32),
    CONSTRAINT mbox_traffic_ingest_batches_record_count
        CHECK (record_count >= 0),
    CONSTRAINT mbox_traffic_ingest_batches_bucket_pair
        CHECK (
            (first_bucket IS NULL AND last_bucket IS NULL)
            OR
            (
                first_bucket IS NOT NULL
                AND last_bucket IS NOT NULL
                AND last_bucket >= first_bucket
            )
        )
);

-- mbox:statement

CREATE INDEX mbox_traffic_summary_actual_outbound_idx
    ON {{schema}}.mbox_traffic_minute_summary (
        instance_id,
        actual_outbound_tag,
        bucket_start
    );

-- mbox:statement

CREATE INDEX mbox_traffic_targets_domain_idx
    ON {{schema}}.mbox_traffic_minute_targets (
        instance_id,
        destination_domain,
        bucket_start
    )
    WHERE destination_domain <> '';

-- mbox:statement

CREATE INDEX mbox_traffic_targets_ip_idx
    ON {{schema}}.mbox_traffic_minute_targets (
        instance_id,
        destination_ip,
        bucket_start
    )
    WHERE destination_ip <> '';
