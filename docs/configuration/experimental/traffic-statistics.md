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
serialized parsed configuration. The random HMAC key is stored in the traffic
database, so the revision is stable for the same configuration and database
but is not comparable across databases. It does not expose configuration
secrets and is not a portable content hash.

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
- A target-detail bucket additionally stores `destination_domain`. It starts
  at the first complete minute boundary after the domain-aware build opens the
  database. The partial minute containing the upgrade is deliberately excluded.
  The `target_available_from` field reports the later of this complete
  collection boundary and the current retention boundary.

Pre-upgrade summary traffic is not fabricated as an empty/unknown domain.
Domain grouping and any query with `destination_domains` therefore cover only
the target-detail period. Within that period, an empty `destination_domain`
represents a flow for which no valid logical domain was available, such as an
IP-only flow. This makes target-period known and unknown domain totals
reconcilable.

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
| `destination_domain` | Best valid logical target domain frozen when routing hands the flow to the outbound. A valid `Destination.Fqdn` has priority; otherwise a valid sniffed/reverse-mapped `Metadata.Domain` is used. It is lower-cased and trailing dots are removed. Invalid names and IP-only targets produce an empty string. This is not the proxy-node server hostname. |
| `connections` | Number of resolved TCP flows or UDP packet sessions. A resolved zero-byte dispatch still counts once. |

The v2 query supports these `group_by` values:

| Value | Result key and aggregation |
|-------|----------------------------|
| `route_path` | Preserves the complete summary dimensions (`config_revision`, route path, leaf tag/type, and network) while folding `destination_domain`. This is the default. |
| `destination_domain` | Groups only by normalized target domain across configuration revisions, networks, routes, groups, and nodes. |
| `outbound_group` | Groups only by the innermost/leaf-parent group tag across revisions and networks. |
| `actual_outbound` | Groups only by leaf outbound tag across revisions and networks. |

Every response row always contains every dimension field. Fields not applicable
to the selected grouping are empty strings or an empty `group_path`.

### REST API v2

Both supported listeners expose:

| Resource | Method | Purpose |
|----------|--------|---------|
| `/mbox/v2/traffic/capabilities` | `GET` | Discover dimensions, groupings, sort fields, storage parameters, and target availability. |
| `/mbox/v2/traffic/query` | `POST` | Filter, aggregate, search, sort, and page summary rows. |

This build mounts the v2 path only; the incompatible v1 endpoint is not kept.

Authentication is inherited from the listener:

| Listener | Authentication |
|----------|----------------|
| Clash API | `Authorization: Bearer <secret>` when `experimental.clash_api.secret` is non-empty; otherwise none. |
| Native API service | `Authorization: Bearer <secret>` when the service `secret` is non-empty; otherwise none. |

!!! warning

    Traffic history, especially target domains, is sensitive. If the listener
    has an empty secret, restrict it to loopback or a trusted private network.

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
    "destination_domain",
    "outbound_group",
    "actual_outbound_tag",
    "actual_outbound_type",
    "network"
  ],
  "groupings": [
    "route_path",
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
  "target_available_from": "2026-07-25T12:00:00Z"
}
```

`target_available_from` is an empty string before history storage has been
initialized. It moves forward with the retention window after the original
target-collection start ages out.

### Query

```json
{
  "from": "2026-07-25T00:00:00Z",
  "to": "2026-07-26T00:00:00Z",
  "group_by": "destination_domain",
  "route_tags": ["AI"],
  "group_tags": ["AI-Auto"],
  "actual_outbound_tags": ["ai-node"],
  "destination_domains": ["api.openai.com"],
  "networks": ["tcp", "udp"],
  "search": "openai",
  "sort_by": "total_bytes",
  "sort_order": "desc",
  "page": 1,
  "page_size": 50
}
```

`from` and `to` are optional RFC 3339 timestamps and must satisfy `from < to`.
The five list filters are exact-match allowlists and are combined with AND.
`group_tags` matches `outbound_group`, not every ancestor in `group_path`.
Destination-domain filter values are normalized like stored domains; an empty
string explicitly selects target-period flows with no domain. Each filter
accepts at most 256 values, each at most 1024 UTF-8 bytes. `networks` accepts
only `tcp` and `udp`.

`search` is a case-insensitive substring match on the current grouping label:
the displayed route path, destination domain, outbound group, or leaf outbound
tag. It is applied after aggregation and before totals and pagination.

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
  "group_by": "destination_domain",
  "page": 1,
  "page_size": 50,
  "total_rows": 1,
  "target_available_from": "2026-07-25T12:00:00Z",
  "actual_from": "2026-07-25T12:00:00Z",
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
match. For target-backed queries, their range and totals cover only target
details at or after `target_available_from`.
