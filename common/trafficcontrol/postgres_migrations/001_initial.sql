CREATE TABLE {{schema}}.mbox_traffic_instances (
    instance_id text PRIMARY KEY,
    revision_key bytea NOT NULL,
    target_available_from timestamptz NOT NULL,
    destination_available_from timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT mbox_traffic_instances_revision_key_length
        CHECK (octet_length(revision_key) = 32),
    CONSTRAINT mbox_traffic_instances_id_length
        CHECK (octet_length(instance_id) BETWEEN 1 AND 128),
    CONSTRAINT mbox_traffic_instances_availability_order
        CHECK (destination_available_from >= target_available_from)
);

-- mbox:statement

CREATE TABLE {{schema}}.mbox_traffic_config_revisions (
    instance_id text NOT NULL,
    routing_fingerprint bytea NOT NULL,
    config_revision text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, routing_fingerprint),
    CONSTRAINT mbox_traffic_config_revisions_instance_fk
        FOREIGN KEY (instance_id)
        REFERENCES {{schema}}.mbox_traffic_instances(instance_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_config_revisions_fingerprint_length
        CHECK (octet_length(routing_fingerprint) = 32),
    CONSTRAINT mbox_traffic_config_revisions_format
        CHECK (config_revision ~ '^[0-9a-f]{32}$')
);

-- mbox:statement

CREATE TABLE {{schema}}.mbox_traffic_minute_summary (
    instance_id text NOT NULL,
    bucket_start timestamptz NOT NULL,
    config_revision text NOT NULL,
    route_tag text NOT NULL,
    group_path text[] NOT NULL,
    actual_outbound_tag text NOT NULL,
    actual_outbound_type text NOT NULL,
    network text NOT NULL,
    uplink_bytes numeric(20,0) NOT NULL,
    downlink_bytes numeric(20,0) NOT NULL,
    connections numeric(20,0) NOT NULL,
    PRIMARY KEY (
        instance_id,
        bucket_start,
        config_revision,
        route_tag,
        group_path,
        actual_outbound_tag,
        actual_outbound_type,
        network
    ),
    CONSTRAINT mbox_traffic_minute_summary_instance_fk
        FOREIGN KEY (instance_id)
        REFERENCES {{schema}}.mbox_traffic_instances(instance_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_minute_summary_bucket
        CHECK (bucket_start = date_trunc('minute', bucket_start)),
    CONSTRAINT mbox_traffic_minute_summary_uplink
        CHECK (uplink_bytes BETWEEN 0 AND 18446744073709551615),
    CONSTRAINT mbox_traffic_minute_summary_downlink
        CHECK (downlink_bytes BETWEEN 0 AND 18446744073709551615),
    CONSTRAINT mbox_traffic_minute_summary_connections
        CHECK (connections BETWEEN 0 AND 18446744073709551615)
);

-- mbox:statement

CREATE TABLE {{schema}}.mbox_traffic_minute_targets (
    instance_id text NOT NULL,
    bucket_start timestamptz NOT NULL,
    config_revision text NOT NULL,
    route_tag text NOT NULL,
    group_path text[] NOT NULL,
    actual_outbound_tag text NOT NULL,
    actual_outbound_type text NOT NULL,
    network text NOT NULL,
    destination_domain text NOT NULL,
    destination_ip text NOT NULL,
    uplink_bytes numeric(20,0) NOT NULL,
    downlink_bytes numeric(20,0) NOT NULL,
    connections numeric(20,0) NOT NULL,
    PRIMARY KEY (
        instance_id,
        bucket_start,
        config_revision,
        route_tag,
        group_path,
        actual_outbound_tag,
        actual_outbound_type,
        network,
        destination_domain,
        destination_ip
    ),
    CONSTRAINT mbox_traffic_minute_targets_instance_fk
        FOREIGN KEY (instance_id)
        REFERENCES {{schema}}.mbox_traffic_instances(instance_id)
        ON DELETE CASCADE,
    CONSTRAINT mbox_traffic_minute_targets_bucket
        CHECK (bucket_start = date_trunc('minute', bucket_start)),
    CONSTRAINT mbox_traffic_minute_targets_uplink
        CHECK (uplink_bytes BETWEEN 0 AND 18446744073709551615),
    CONSTRAINT mbox_traffic_minute_targets_downlink
        CHECK (downlink_bytes BETWEEN 0 AND 18446744073709551615),
    CONSTRAINT mbox_traffic_minute_targets_connections
        CHECK (connections BETWEEN 0 AND 18446744073709551615)
);
