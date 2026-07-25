# Traffic Statistics

!!! warning "mbox traffic-statistics builds"

    This is an mbox extension. It is not part of the upstream sing-box API.

Traffic statistics persist per-route and per-outbound upload and download totals.
Collection is disabled by default.

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

The REST endpoints are mounted only when this option is enabled and the
corresponding [Clash API](./clash-api/) listener or
[sing-box API service](../service/api/) has a non-empty secret.

#### path

Path to the traffic statistics database.

`traffic.db` is used when empty. Relative paths use the standard sing-box base
path resolution.

Changing the configuration does not merge rows from different configurations.
Each row includes an opaque `config_revision` so that identical tags used by
different configurations remain distinguishable. It is a keyed HMAC of the
serialized parsed configuration options. The random HMAC key is stored in the
traffic database, so the revision is stable for the same serialized
configuration and database but is not comparable across databases. The
revision does not expose configuration secrets and must not be treated as a
portable content hash. Configurations with equivalent runtime behavior are not
guaranteed to share a revision.

### Current storage behavior

| Property | Current value |
|----------|---------------|
| Bucket interval | 1 minute |
| Retention | 30 days |
| Active-flow sampling and pending-data disk flush tick | 5 seconds |

These values are current implementation defaults, not stable protocol
commitments. Clients must read `bucket_seconds` and `retention_seconds` from the
capabilities endpoint instead of hard-coding them. The sampling and flush
schedule may also change between mbox builds.

In the current implementation, pending data is written to disk on the
five-second flush tick.

Remaining pending counters are flushed during a clean shutdown.
An unclean process or machine failure can lose the most recent unsampled and
unflushed deltas (roughly up to two current five-second ticks).

For an active long-lived flow, deltas are assigned to the bucket containing the
sampling time. A final delta is sampled when the flow closes. A time-range
boundary can therefore be approximate by about one sampling interval; the
bucket timestamps are not billing-grade event timestamps.

### Dimensions and counters

Suppose routing selects the `AI` selector, which selects the `AI-Auto` URLTest
group, which in turn selects `ai-node`. The corresponding dimensions are:

```json
{
  "route_tag": "AI",
  "group_path": ["AI", "AI-Auto"],
  "actual_outbound_tag": "ai-node"
}
```

| Field | Meaning |
|-------|---------|
| `route_tag` | Outbound tag selected by the matched route action, or by the final route when no rule overrides it. It is not the optional tag of the route rule itself, and remains `AI` in the example. |
| `group_path` | Ordered group tags traversed from the routed group to the innermost group. The leaf outbound is not included. A direct leaf route has an empty array. |
| `actual_outbound_tag` | Network-specific leaf outbound actually selected and dispatched for the flow. A flow that ends before a leaf can be resolved is not recorded. |
| `actual_outbound_type` | Type of the selected leaf outbound. |
| `network` | `tcp` or `udp`. URLTest selection is resolved independently for each network. |
| `uplink_bytes` | Logical payload bytes traveling from the inbound/client toward the destination. |
| `downlink_bytes` | Logical payload bytes traveling from the destination toward the inbound/client. |
| `connections` | Number of routed TCP flows or UDP packet sessions whose actual leaf was resolved. A resolved zero-byte dispatch, including one that later fails, still counts once. |
| `config_revision` | Database-local opaque revision of the serialized configuration options. |

The path describes logical outbound-group dispatch. A lower-level outbound
detour or the transport socket used by the leaf is intentionally not another
element of `group_path` and does not replace `actual_outbound_tag`.

`uplink_bytes` and `downlink_bytes` use the `logical_payload` metric scope. They
measure bytes observed at the logical routed-flow boundary and do not represent
outbound wire bytes, encrypted transport overhead, protocol framing, padding,
or link-layer overhead. In particular, uplink bytes are counted after they are
read from the inbound/client side; this does not guarantee that the same bytes
were successfully written to the destination. Downlink bytes are counted when
written toward the inbound/client side. These counters are operational traffic
accounting, not proof of end-to-end delivery.

To preserve this accounting boundary, history-enabled trackers disable
replaceable-reader/writer and fast-copy paths such as splice or zero-copy for
tracked traffic. This can reduce throughput compared with an ordinary build,
which is why persistent history is opt-in and intended only for machines that
need it.

Target-domain and target-IP statistics are not currently collected. The
capabilities response therefore reports `features.targets` as `false`.

### REST API

Both supported listeners expose the same REST resources:

| Resource | Method | Purpose |
|----------|--------|---------|
| `/mbox/v1/traffic/capabilities` | `GET` | Discover the schema, metric scope, dimensions, and current storage parameters. |
| `/mbox/v1/traffic/query` | `POST` | Query summary rows. |

The listener and authentication source differ:

| Listener | Configuration | Authentication |
|----------|---------------|----------------|
| Clash API | `experimental.clash_api.external_controller` | `Authorization: Bearer <secret>`, using `experimental.clash_api.secret`. |
| Native sing-box API | A top-level service with `"type": "api"` | `Authorization: Bearer <secret>`, using that API service's `secret`. |

If a listener's secret is empty, the traffic-statistics resources are not
mounted on that listener. Other resources on the listener keep their existing
authentication behavior.

For example:

```bash
curl \
  -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:9090/mbox/v1/traffic/capabilities
```

The current capabilities response is similar to:

```json
{
  "api_version": "1",
  "metric_scope": "logical_payload",
  "features": {
    "summary": true,
    "series": false,
    "targets": false
  },
  "dimensions": [
    "config_revision",
    "route_tag",
    "group_path",
    "actual_outbound_tag",
    "actual_outbound_type",
    "network"
  ],
  "bucket_seconds": 60,
  "retention_seconds": 2592000
}
```

### Query

```json
{
  "from": "2026-07-25T00:00:00Z",
  "to": "2026-07-26T00:00:00Z",
  "route_tags": ["AI"],
  "actual_outbound_tags": ["ai-node"],
  "networks": ["tcp", "udp"],
  "limit": 500
}
```

`from` and `to` are optional RFC 3339 timestamps and must satisfy `from < to`
when both are present. Each filter is an exact-match allowlist. An omitted or
empty filter accepts all values; different filter fields are combined with AND.
Each filter accepts at most 256 values, and each value is limited to 1024 UTF-8
bytes. `networks` accepts only `tcp` and `udp`.

The query returns a summary aggregated across all overlapping one-minute
buckets. It does not return a per-bucket series. `actual_from` and `actual_to`
describe the complete bucket range that contributed matching rows, so they can
extend beyond the exact requested timestamps.

Rows are ordered by `uplink_bytes + downlink_bytes`, largest first. The default
limit is 500 and the maximum accepted limit is 5000. An omitted or zero limit
uses the default; a negative limit or a limit above 5000 is rejected.

```json
{
  "actual_from": "2026-07-25T00:00:00Z",
  "actual_to": "2026-07-26T00:00:00Z",
  "totals": {
    "uplink_bytes": "12345",
    "downlink_bytes": "67890",
    "connections": "12"
  },
  "rows": [
    {
      "config_revision": "81dc9bdb52d04dc20036dbd8313ed055",
      "route_tag": "AI",
      "group_path": ["AI", "AI-Auto"],
      "actual_outbound_tag": "ai-node",
      "actual_outbound_type": "vmess",
      "network": "tcp",
      "uplink_bytes": "12345",
      "downlink_bytes": "67890",
      "connections": "12"
    }
  ],
  "truncated": false
}
```

Counters are decimal JSON strings so JavaScript clients do not lose 64-bit
integer precision.

An empty result is:

```json
{
  "totals": {
    "uplink_bytes": "0",
    "downlink_bytes": "0",
    "connections": "0"
  },
  "rows": [],
  "truncated": false
}
```

`actual_from` and `actual_to` are omitted when no rows match. If more rows match
than the applied limit, only the highest-traffic rows are returned and
`truncated` is `true`. `totals` is calculated from every matching row before the
limit is applied, so it remains a complete summary even when the detail rows are
truncated. There is currently no pagination; use a larger limit or narrower
filters when every detail row is required.
