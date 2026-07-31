# Traffic Statistics

!!! warning "mbox traffic-statistics builds"

    This is an mbox extension. It is not part of the upstream sing-box API.

Traffic statistics persist routed upload and download totals. Collection is
disabled by default.

Configure this object at `experimental.traffic_statistics`.

### Structure

```json
{
  "enabled": true,
  "path": "traffic.db"
}
```

### Fields

#### enabled

Enable traffic statistics collection and persistence.

When enabled, the REST endpoints are mounted on each configured
[Clash API](./clash-api/) listener or
[sing-box API service](../service/api/). A non-empty listener secret protects
the endpoints with that listener's existing authentication. An empty secret
leaves them unauthenticated.

#### path

Path to the traffic statistics database.

`traffic.db` is used when empty. Relative paths use the standard sing-box base
path resolution.

Each route-path row has an opaque `config_revision`. It is a keyed HMAC of the
serialized parsed configuration. The random HMAC key is stored with the traffic
state, so the revision is stable for the same configuration and instance but is
not comparable across independent storage identities. It does not expose
configuration secrets and is not a portable content hash.

### Configuration examples and storage selection

The legacy object without `storage` continues to select Bolt:

<!-- mbox-test:traffic-statistics-bolt -->
```json
{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "path": "traffic.db"
    }
  }
}
```
<!-- mbox-test:end -->

`path` defaults to `traffic.db`. Relative paths use the normal mbox
base-directory resolution. Setting both the legacy `path` and `storage` is
invalid. With an explicit storage object, `storage.type` must be `bolt` or
`postgres`.

This direct PostgreSQL example deliberately uses the database `observability`;
an arbitrary database selected by the DSN is valid and no database named
`traffic` is required:

<!-- mbox-test:traffic-statistics-postgres-direct -->
```json
{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "storage": {
        "type": "postgres",
        "dsn": "postgres://mbox_data@db.example/observability?sslmode=verify-full&sslrootcert=/etc/mbox/postgresql-ca.crt"
      }
    }
  }
}
```
<!-- mbox-test:end -->

The minimal PostgreSQL example resolves to schema `public`,
`schema_management: auto`, four maximum and one minimum-idle connection,
`connect_timeout: 10s`, `statement_timeout: 30s`, identity file
`traffic-instance.json`, spool file `traffic-spool.db`, spool capacity 256 MiB,
overflow policy `drop_oldest`, and startup policy `degraded`.

This example shows every PostgreSQL operational control and sends the logical
database TCP stream through `hy2-out`:

<!-- mbox-test:traffic-statistics-postgres-detour -->
```json
{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "instance_id": "edge-a",
      "identity_path": "traffic-instance.json",
      "storage": {
        "type": "postgres",
        "dsn": "postgres://mbox_data@db.internal/observability?sslmode=disable",
        "schema": "mbox_traffic",
        "schema_management": "validate",
        "dialer": {
          "detour": "hy2-out"
        },
        "max_open_connections": 8,
        "min_idle_connections": 2,
        "connect_timeout": "15s",
        "statement_timeout": "45s"
      },
      "spool": {
        "path": "traffic-spool.db",
        "max_size": "512MB",
        "overflow": "drop_oldest"
      },
      "startup_policy": "strict"
    }
  }
}
```
<!-- mbox-test:end -->

Only `dialer.detour` is accepted under PostgreSQL storage. Configuration files
use the normal root loading and merge mechanism: `-c/--config`,
`-C/--config-directory`, and `-D/--directory`. There is no additional DSN
environment-expansion layer.

| Field | Storage | Meaning and default |
|-------|---------|---------------------|
| `enabled` | both | Enables collection, persistence, and the REST resources. Default: `false`. |
| `path` | legacy Bolt | Bolt path. Default: `traffic.db`. Mutually exclusive with `storage`. |
| `instance_id` | PostgreSQL | Stable per-instance identity. Empty loads or creates the identity file. |
| `identity_path` | PostgreSQL | Persisted identity path. Default: `traffic-instance.json`. |
| `storage.type` | explicit | `bolt` or `postgres`; required when `storage` is present. |
| `storage.path` | explicit Bolt | Bolt path. Default: `traffic.db`. |
| `storage.dsn` | PostgreSQL | Required pgx connection string; selects the existing database through either the URI database component or the pgx `dbname` keyword. No database named `traffic` is required. |
| `storage.schema` | PostgreSQL | Valid identifier. Default: `public`. |
| `storage.schema_management` | PostgreSQL | `auto` or `validate`. Default: `auto`. |
| `storage.dialer.detour` | PostgreSQL | Optional outbound tag for the logical database TCP stream. |
| `storage.max_open_connections` | PostgreSQL | Pool maximum. Default: `4`; minimum: `1`. |
| `storage.min_idle_connections` | PostgreSQL | Idle minimum. Default: `1`; no greater than the maximum. |
| `storage.connect_timeout` | PostgreSQL | Positive connection timeout. Default: `10s`. |
| `storage.statement_timeout` | PostgreSQL | Positive operation timeout. Default: `30s`. |
| `spool.path` | PostgreSQL | Local durable queue. Default: `traffic-spool.db`. |
| `spool.max_size` | PostgreSQL | Logical capacity. Default: 256 MiB. |
| `spool.overflow` | PostgreSQL | Only `drop_oldest` is supported. |
| `startup_policy` | PostgreSQL | `degraded` or `strict`. Default: `degraded`. |

### PostgreSQL ownership and schema lifecycle

PostgreSQL 14 and newer are supported. The server, selected database, login
roles, and grants must already exist. The database name comes entirely from
`storage.dsn`; mbox never executes `CREATE DATABASE`, and neither the schema
role nor the runtime role needs superuser or `CREATEDB`.

With `schema_management: auto`, mbox may create the configured schema and its
own objects, acquires a schema advisory lock, verifies embedded migration
checksums, and applies migrations transactionally where PostgreSQL permits. It
does not create databases or roles. With `schema_management: validate`, it
performs no DDL and requires the exact compatible schema version, currently
version 3. Missing, incompatible, future, checksum, authentication, and
privilege failures are classified without returning raw connection secrets.

An administrator can apply the same embedded migrations with:

```bash
mbox tools traffic-statistics schema migrate
```

This command loads the normal configured PostgreSQL connection and performs
schema auto migration. It is currently direct-only. If the configured storage
uses a detour, it returns the stable detour-not-ready error. Use production
`auto` after outbound startup, or run the command with a separate direct
administration configuration.

For a database `observability`, custom schema `mbox_traffic`, schema role
`mbox_schema_admin`, and runtime role `mbox_data`, a least-privilege deployment
normally grants:

- the schema role `CONNECT` on `observability`, enough database/schema DDL
  privilege to create or upgrade `mbox_traffic`, and ownership or equivalent
  rights over mbox-owned tables, indexes, constraints, and the migration
  ledger;
- the validate-only runtime role `CONNECT` on `observability`, `USAGE` on
  `mbox_traffic`, `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on the mbox-owned
  tables, plus read access required to validate the migration ledger.

Update runtime grants after upgrades introduce new tables. Schema version 3
owns:

| Object | Purpose |
|--------|---------|
| `mbox_traffic_instances` | Per-instance identity and availability metadata. |
| `mbox_traffic_config_revisions` | Opaque routing configuration revisions. |
| `mbox_traffic_minute_summary` | Minute-level route-path totals. |
| `mbox_traffic_minute_targets` | Minute-level destination detail. |
| `mbox_traffic_ingest_batches` | Accepted durable-batch identities for exactly-once effect. |
| `mbox_traffic_migration_jobs` | Offline migration identity, selection, state, and fingerprint. |
| `mbox_traffic_migration_revisions` | Migrated revision metadata. |
| `mbox_traffic_migration_batches` | Durable migration cursor and marker chain. |

### First run, startup, and local durability

- `Box.New` validates options, resolves or atomically creates identity, derives
  the keyed revision, opens and validates the local durable spool, constructs
  a PostgreSQL pool factory without connecting, and registers history services.
- The first remote PostgreSQL connection is made during `Box.Start`, after the
  selected outbound can dial. It initializes or validates schema, reconciles
  remote identity and revision state, and starts ordered recovery and delivery.

Local identity or spool failures always fail construction. `strict` returns the
initial remote failure and closes the inner store. `degraded` keeps the local
durable spool and retry worker after a retryable first remote failure, lets
proxy traffic continue, and returns committed-query HTTP 503 until available.
Permanent errors require operator correction.

Production identity selection is explicit `instance_id`, then persisted
identity, then a generated UUID. The default identity file is atomically
written with mode 0600. Identity is independent from the spool. Writes become
durable locally before delivery is awakened, retain batch IDs across restart,
and drain older batches first. PostgreSQL accepted-batch identity provides
exactly-once remote effect.

The only overflow behavior is `drop_oldest`. Dropped data is acceptable
telemetry loss, not billing behavior. Do not inspect, copy, replace, or open a
live spool with another writer. Protect identity and spool as sensitive backup
state; restoring one without matching spool/database state can cause identity
or revision conflicts.

### TLS, detours, consistency, and availability

Direct connections need no detour. Both `sslmode=disable` and
`sslmode=verify-full` with `sslrootcert` are supported. pgx owns TLS above the
logical connection. TLS is independent of the selected detour: a detour neither
disables nor requires it. The stream remains logical TCP even when the outbound
uses UDP/QUIC; Hysteria2 is a tested logical-TCP-over-QUIC path. Missing or
UDP-only non-stream outbounds fail clearly. Database self-traffic is excluded
from user traffic accounting.

Bolt queries retain the in-memory pending overlay. PostgreSQL queries establish
a durable committed boundary before opening the remote snapshot. Filtering,
aggregation, sorting, totals, and pagination use that single consistent
snapshot. A successful stale remote result is never substituted. Timeout or
unavailability maps to stable, redacted HTTP 503; explicit client cancellation
remains cancellation.

### Offline Bolt migration

```bash
mbox tools traffic-statistics migrate
```

The command uses normal root configuration loading. The target must resolve to
configured PostgreSQL; DSN, schema, and detour settings come from that
configuration.

| Flag | Default | Meaning |
|------|---------|---------|
| `--source` | `traffic.db` | Source Bolt database. |
| `--instance-id` | empty | Target instance identity. |
| `--batch-size` | `500` | Source records per transaction. |
| `--from` | empty | Inclusive RFC 3339 minute boundary. |
| `--to` | empty | Exclusive RFC 3339 minute boundary. |
| `--resume` | empty | Expected deterministic migration ID. |
| `--active-config-revision` | empty | Active historical revision. |
| `--dry-run` | `false` | Validate and compare without persistent writes. |

The Bolt source must be offline. Lock contention fails clearly; source bytes
and metadata remain unchanged. The complete raw Bolt domain is validated before
runtime/network work. Summary/target records, availability, revisions, revision
key, and unsigned counters are preserved.

Safe restore inserts missing rows, skips exact rows, and stops on conflicts.
Migration IDs and cursor ranges are deterministic. Interrupted runs continue
after the durable marker chain; uncertain commits reconcile exactly once.
`--from` is inclusive and `--to` exclusive. `--dry-run` performs zero
persistent writes and forces schema validation-only. Progress JSON goes to
stderr and one final result JSON object goes to stdout.

Normal production cleanup retains 30 days, but offline migration does not apply
that retention cutoff: it restores every selected historical source row,
including rows older than 30 days, unless the operator narrows the restore with
`--from` or `--to`. Normal production cleanup may later remove restored rows
outside its retention window.

There is no migration `--dsn`, `--schema`, `--merge`, or `--delete-source`
flag. Inherited `--outbound` is rejected. Direct mode constructs no outbound
runtime. Detour mode constructs its scoped network-namespace, DNS
transport/router, network, connection/router, outbound, endpoint, and
certificate dependency graph, plus an HTTP-client manager only when a Hysteria2
outbound realm requires it. The registered inbound manager creates and starts
zero inbound objects or listeners. No API, traffic collector, NTP, cache-file
service, or debug server is started. Migration never deletes the source and
does not support an online snapshot.

### Monitoring, recovery, and security

A successful query proves its requested committed boundary was available.
Redacted HTTP 503 means the boundary could not be committed and read within the
request context. Startup errors distinguish local durability, retryable remote,
and permanent classified failures. Administrators can inspect instance,
accepted-batch, and migration metadata; operators can monitor spool file size
and filesystem health externally.

No new public traffic-statistics readiness or status endpoint exists. Internal
queue, drop, retry, flush, and error-category fields are not public API.
Transient recovery is automatic under `degraded`. Authentication,
missing-database, permission, incompatible/future schema, identity, revision,
and collision errors require correction. Disk-full, permission, corruption,
and lock errors are local durability failures; do not blindly delete the spool.

DSNs and passwords are sensitive. There is no command-line DSN override.
Restrict configuration, identity, and spool permissions. Errors and HTTP
responses are redacted, but traffic history contains sensitive destinations,
so authenticate API listeners and use trusted bindings. Rows and REST views are
per instance; there is no cross-instance REST aggregate.

### Storage and accounting

| Property | Current value |
|----------|---------------|
| Bucket interval | 1 minute |
| Retention | 30 days |
| Active-flow sampling and pending-data disk flush tick | 5 seconds |

These values are implementation defaults, not stable protocol commitments.
Clients must inspect the capabilities response.

Every sampled delta is written to the summary bucket. At or after the target
collection boundary, the same Bolt transaction also writes it to the
target-detail bucket:

- A low-cardinality summary bucket stores route path, group, leaf outbound,
  network, and configuration revision. Existing databases already contain this
  bucket, so summary queries continue to include pre-upgrade history.
- A target-detail bucket additionally stores the pure `destination_domain` and,
  in current builds, the fallback destination IP. Domain detail starts at the
  first complete minute boundary after the domain-aware build opens the
  database. The partial minute containing the upgrade is deliberately excluded.
  The `target_available_from` field reports the later of this domain-detail
  boundary and the current retention boundary.

Pre-upgrade summary traffic is not fabricated as an empty/unknown domain.
Domain grouping and any query with `destination_domains` therefore cover only
the target-detail period. Within that period, an empty `destination_domain`
represents a flow for which no valid logical domain was available; this includes
IP-only flows.

The preferred `destination` dimension was introduced after pure domain detail.
An existing database cannot recover the IPs represented by its old empty-domain
records. Such records are not fabricated as complete targets. The
`destination_available_from` field reports the first complete minute collected
with domain-or-IP detail, clamped to the current retention boundary. Grouping by
`destination` and any query with `destinations` cover only that later period.
New databases normally initialize both availability boundaries to the same
minute.

The completeness boundary assumes one-way upgrades while reusing a database.
If the database is temporarily opened by an older statistics build that does
not write fallback IPs and is then upgraded again, the service cannot
distinguish missing IPs from genuinely unknown targets, so the existing
`destination_available_from` value is no longer reliable. Restore a
pre-downgrade database backup or start a new traffic database to establish a
new completeness boundary.

Pending counters are flushed on the five-second tick and at clean shutdown. An
unclean failure can lose the most recent unsampled and unflushed deltas
(roughly two current ticks). Long-lived-flow deltas belong to the bucket
containing their sampling time, so range boundaries are approximate by about
one sampling interval. These are operational counters, not billing-grade event
timestamps.

`uplink_bytes` and `downlink_bytes` use the `logical_payload` metric scope at
the routed-flow boundary. They exclude encrypted wire overhead, framing,
padding, and link-layer overhead. Uplink bytes have been read from the client
side but are not proof of delivery to the destination. Downlink bytes are
counted when written toward the client.

History-enabled trackers disable replaceable-reader/writer and fast-copy paths
such as splice or zero-copy to preserve this accounting boundary. This can
reduce throughput, which is why persistent history remains opt-in.

### Dimensions and groupings

For a route that selects `AI`, traverses `AI-Auto`, and dispatches `ai-node`,
the stored route dimensions are:

```json
{
  "route_tag": "AI",
  "group_path": ["AI", "AI-Auto"],
  "outbound_group": "AI-Auto",
  "actual_outbound_tag": "ai-node"
}
```

| Field | Meaning |
|-------|---------|
| `config_revision` | Database-local opaque configuration revision. |
| `route_tag` | Outbound tag selected by the route action, or by the final route. It is not the optional tag of the route rule. |
| `group_path` | Ordered outbound-group tags traversed from the routed group to the innermost group. A direct leaf route has an empty array. |
| `outbound_group` | The last element of `group_path`, or an empty string for a direct leaf route. |
| `actual_outbound_tag` | Network-specific leaf outbound actually selected and dispatched. |
| `actual_outbound_type` | Type of the selected leaf. When `actual_outbound` grouping combines one tag used with multiple types, this is an empty string. |
| `network` | `tcp` or `udp`. |
| `destination` | Preferred logical target frozen when routing hands the flow to the outbound: the valid logical domain when available, otherwise `Destination.Addr`. Domains are lower-cased with one trailing dot removed. IPs use canonical text; IPv4-mapped IPv6 is unwrapped and an IPv6 zone is removed. This is neither the proxy-node server address nor a domain's resolved/dialed IP. |
| `destination_type` | `domain` or `ip`, matching `destination`; empty when neither is available. |
| `destination_domain` | Backward-compatible pure-domain dimension. A valid `Destination.Fqdn` has priority; otherwise a valid sniffed/reverse-mapped `Metadata.Domain` is used. Invalid names and IP-only targets produce an empty string. It never contains an IP. |
| `connections` | Number of resolved TCP flows or UDP packet sessions. A resolved zero-byte dispatch still counts once. |

The v2 query supports these `group_by` values:

| Value | Result key and aggregation |
|-------|----------------------------|
| `route_path` | Preserves the complete summary dimensions (`config_revision`, route path, leaf tag/type, and network) while folding target dimensions. This is the default. |
| `destination` | Groups by the preferred normalized domain-or-IP target across configuration revisions, networks, routes, groups, and nodes. This is the grouping intended for current target views. |
| `destination_domain` | Backward-compatible grouping by pure normalized target domain. IP-only and otherwise domain-less flows share the empty-domain row. |
| `outbound_group` | Groups only by the innermost/leaf-parent group tag across revisions and networks. |
| `actual_outbound` | Groups only by leaf outbound tag across revisions and networks. |

Every response row always contains every dimension field. Fields not applicable
to the selected grouping are empty strings or an empty `group_path`.

### REST API v2

Both supported listeners expose:

| Resource | Method | Purpose |
|----------|--------|---------|
| `/mbox/v2/traffic/capabilities` | `GET` | Discover dimensions, groupings, sort fields, storage parameters, and domain/target availability. |
| `/mbox/v2/traffic/query` | `POST` | Filter, aggregate, search, sort, and page summary rows. |

This build mounts the v2 path only; the incompatible v1 endpoint is not kept.

Authentication is inherited from the listener:

| Listener | Authentication |
|----------|----------------|
| Clash API | `Authorization: Bearer <secret>` when `experimental.clash_api.secret` is non-empty; otherwise none. |
| Native API service | `Authorization: Bearer <secret>` when the service `secret` is non-empty; otherwise none. |

!!! warning

    Traffic history, especially target domains and IPs, is sensitive. If the
    listener has an empty secret, restrict it to loopback or a trusted private
    network.

Example capabilities response:

```json
{
  "api_version": "2",
  "metric_scope": "logical_payload",
  "features": {
    "summary": true,
    "series": false,
    "targets": true,
    "pagination": true,
    "sorting": true,
    "filtering": true
  },
  "dimensions": [
    "config_revision",
    "route_tag",
    "group_path",
    "destination",
    "destination_type",
    "destination_domain",
    "outbound_group",
    "actual_outbound_tag",
    "actual_outbound_type",
    "network"
  ],
  "groupings": [
    "route_path",
    "destination",
    "destination_domain",
    "outbound_group",
    "actual_outbound"
  ],
  "sort_fields": [
    "name",
    "total_bytes",
    "uplink_bytes",
    "downlink_bytes",
    "connections"
  ],
  "max_page_size": 200,
  "bucket_seconds": 60,
  "retention_seconds": 2592000,
  "target_available_from": "2026-07-25T12:00:00Z",
  "destination_available_from": "2026-07-25T15:00:00Z"
}
```

`target_available_from` is an empty string before history storage has been
initialized. It moves forward with the retention window after the original
domain-detail collection start ages out. `destination_available_from` follows
the same rule for complete preferred domain-or-IP detail. On an upgraded
database it can be later than `target_available_from`.

### Query

```json
{
  "from": "2026-07-25T00:00:00Z",
  "to": "2026-07-26T00:00:00Z",
  "group_by": "destination",
  "route_tags": ["AI"],
  "group_tags": ["AI-Auto"],
  "actual_outbound_tags": ["ai-node"],
  "destinations": ["api.openai.com", "192.0.2.1"],
  "networks": ["tcp", "udp"],
  "search": "openai",
  "sort_by": "total_bytes",
  "sort_order": "desc",
  "page": 1,
  "page_size": 50
}
```

`from` and `to` are optional RFC 3339 timestamps and must satisfy `from < to`.
The six list filters are exact-match allowlists. Values within one filter are
ORed, and different filter dimensions are combined with AND.
`group_tags` matches `outbound_group`, not every ancestor in `group_path`.
`destinations` accepts normalized domains or IPs; IPv4 and IPv6 inputs are
canonicalized. Its empty string explicitly selects flows with neither a valid
domain nor a valid fallback IP. `destination_domains` remains domain-only; its
empty string selects flows with no valid domain, including known IP-only flows.
Each filter accepts at most 256 values, each at most 1024 UTF-8 bytes.
`networks` accepts only `tcp` and `udp`.

`search` is a case-insensitive substring match on the current grouping label:
the displayed route path, preferred destination, legacy destination domain,
outbound group, or leaf outbound tag. It is applied after aggregation and
before totals and pagination.

The server performs filtering, aggregation, search, stable sorting, and only
then pagination. `sort_by` accepts the fields advertised in capabilities.
`sort_order` is `asc` or `desc`. Defaults are `total_bytes` and `desc`.
Deterministic dimension/name ordering breaks equal-value ties, so page
boundaries remain stable for a fixed snapshot.

`page` is one-based. The default is 1. `page_size` defaults to 50 and is limited
to 200. `total_rows` counts all rows after filters and search, before
pagination. `totals` also covers that complete set, not only the current page.

Example response:

```json
{
  "group_by": "destination",
  "page": 1,
  "page_size": 50,
  "total_rows": 1,
  "target_available_from": "2026-07-25T12:00:00Z",
  "destination_available_from": "2026-07-25T15:00:00Z",
  "actual_from": "2026-07-25T15:00:00Z",
  "actual_to": "2026-07-26T00:00:00Z",
  "totals": {
    "uplink_bytes": "12345",
    "downlink_bytes": "67890",
    "connections": "12"
  },
  "rows": [
    {
      "config_revision": "",
      "route_tag": "",
      "group_path": [],
      "destination": "api.openai.com",
      "destination_type": "domain",
      "destination_domain": "api.openai.com",
      "outbound_group": "",
      "actual_outbound_tag": "",
      "actual_outbound_type": "",
      "network": "",
      "uplink_bytes": "12345",
      "downlink_bytes": "67890",
      "connections": "12"
    }
  ]
}
```

Counters are decimal JSON strings so JavaScript clients do not lose 64-bit
integer precision. `actual_from` and `actual_to` are empty strings when no rows
match. Pure-domain queries cover target details at or after
`target_available_from`; preferred-destination grouping or filtering covers
complete domain-or-IP details at or after `destination_available_from`.
