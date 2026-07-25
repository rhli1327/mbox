# 流量统计

!!! warning "mbox traffic-statistics 构建"

    这是 mbox 扩展，并非上游 sing-box API 的组成部分。

流量统计会持久化已路由连接的上传、下载总量，默认不启用。

在 `experimental.traffic_statistics` 中配置此对象。

### 结构

```json
{
  "enabled": true,
  "path": "traffic.db"
}
```

### 字段

#### enabled

启用流量统计采集与持久化。

启用后，每个已配置的 [Clash API](./clash-api/) 监听器或
[sing-box API 服务](../service/api/)都会挂载 REST 端点。监听器 secret
非空时，端点沿用现有鉴权；secret 为空时不要求鉴权。

#### path

流量统计数据库路径。

留空时使用 `traffic.db`。相对路径遵循 sing-box 的标准基础路径解析规则。

每条路由路径记录都带有不透明的 `config_revision`。它根据解析后的配置序列化
内容计算 keyed HMAC，随机 HMAC 密钥保存在流量数据库中。因此，同一数据库内
相同配置的 revision 保持稳定，不同数据库之间不可比较；它不会泄露配置密钥，
也不是可移植的内容哈希。

### 存储与统计口径

| 属性 | 当前值 |
|------|--------|
| 时间桶间隔 | 1 分钟 |
| 保留期 | 30 天 |
| 活跃流采样与待写数据落盘 tick | 5 秒 |

这些数值是当前实现默认值，并非稳定协议承诺。客户端必须读取 capabilities
响应。

每一笔采样增量都会写入 summary bucket；到达 target 完整采集边界后，同一个
Bolt 事务还会将其写入 target detail bucket：

- 低基数 summary bucket 保存路由路径、节点组、叶子出站、网络与配置 revision。
  旧数据库已经包含该 bucket，因此非域名汇总仍能包含升级前的历史。
- target detail bucket 在相同维度之外保存 `destination_domain`。它从支持域名
  的构建首次打开数据库后的下一个完整分钟边界开始记录，并有意排除升级所在的
  不完整分钟。`target_available_from` 取该完整采集边界与当前保留期边界两者中
  较晚的时间。

系统不会把升级前仅存在于 summary 的流量伪造成空域名或“未知域名”。因此，按
域名聚合以及任何带 `destination_domains` 过滤的查询只覆盖 target detail
可用时段。在该时段内，空 `destination_domain` 表示路由时没有可用的有效逻辑
域名，例如纯 IP 连接；这样该时段内的已知域名与未知域名总量仍可核对。

待写计数会在 5 秒 tick 到达时以及正常关闭时落盘。异常退出可能丢失最近尚未
采样及尚未落盘的增量（按当前实现约两个 tick）。长连接增量归入采样时刻所在的
时间桶，因此时间范围边界可能有约一个采样周期的误差，不适合作为计费级事件
时间戳。

`uplink_bytes` 与 `downlink_bytes` 使用 `logical_payload` 口径，在已路由流的
逻辑边界计量。它们不包含加密链路开销、协议封装、填充或链路层开销。上传字节
表示已从客户端侧读取，不代表已送达目标；下载字节在写向客户端侧时计数。

为保持这一统计边界，启用历史记录后会禁用被跟踪流量的可替换 reader/writer
以及 splice、zero-copy 等快速复制路径，因此吞吐可能降低。持久化统计仍保持
按需启用。

### 维度与聚合方式

假设路由选择 `AI`，随后经过 `AI-Auto`，最终分派到 `ai-node`，相应路由维度为：

```json
{
  "route_tag": "AI",
  "group_path": ["AI", "AI-Auto"],
  "outbound_group": "AI-Auto",
  "actual_outbound_tag": "ai-node"
}
```

| 字段 | 含义 |
|------|------|
| `config_revision` | 本数据库内不透明的配置 revision。 |
| `route_tag` | 路由动作选择的出站标签；没有规则覆盖时为最终路由选择的标签。它不是路由规则本身的可选 tag。 |
| `group_path` | 从路由选中的组到最内层组，按顺序记录实际经过的出站组标签。直接路由到叶子时为空数组。 |
| `outbound_group` | `group_path` 的最后一个元素，即叶子节点的直接父组；直接路由到叶子时为空字符串。 |
| `actual_outbound_tag` | 针对该网络实际选择并分派的叶子出站标签。 |
| `actual_outbound_type` | 叶子出站类型。按 `actual_outbound` 聚合时，若同一 tag 在查询范围内对应多种类型，则为空字符串。 |
| `network` | `tcp` 或 `udp`。 |
| `destination_domain` | 路由完成并交给 outbound 时冻结的最佳有效逻辑目标域名。优先取有效的 `Destination.Fqdn`，否则取有效的嗅探或反向映射 `Metadata.Domain`；统一转小写并去掉尾点。无效名称和纯 IP 目标为空。它不是代理节点服务器域名。 |
| `connections` | 已解析的 TCP 流或 UDP 数据包会话数。已解析但没有字节的分派仍计为一次。 |

v2 查询支持以下 `group_by`：

| 值 | 聚合语义 |
|----|----------|
| `route_path` | 保留完整 summary 维度（`config_revision`、路由路径、叶子 tag/type 与网络），折叠 `destination_domain`。这是默认值。 |
| `destination_domain` | 只按规范化目标域名聚合，跨配置 revision、网络、路由、组和节点汇总。 |
| `outbound_group` | 只按最内层/叶子父组标签聚合，跨 revision 与网络汇总。 |
| `actual_outbound` | 只按叶子出站 tag 聚合，跨 revision 与网络汇总。 |

响应中的每一行始终包含全部维度字段；当前聚合方式不适用的字段为空字符串，
`group_path` 则为空数组。

### REST API v2

两种监听器都提供：

| 资源 | 方法 | 用途 |
|------|------|------|
| `/mbox/v2/traffic/capabilities` | `GET` | 获取维度、聚合方式、排序字段、存储参数与域名明细可用时间。 |
| `/mbox/v2/traffic/query` | `POST` | 过滤、聚合、搜索、排序并分页查询汇总行。 |

当前构建只挂载 v2 路径，不保留不兼容的 v1 端点。

鉴权沿用监听器配置：

| 监听器 | 鉴权 |
|--------|------|
| Clash API | `experimental.clash_api.secret` 非空时使用 `Authorization: Bearer <secret>`；为空时不鉴权。 |
| 原生 API 服务 | 服务的 `secret` 非空时使用 `Authorization: Bearer <secret>`；为空时不鉴权。 |

!!! warning

    流量历史，特别是目标域名，属于敏感信息。监听器 secret 为空时，应只监听
    回环地址或可信私网。

capabilities 响应示例：

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

历史存储尚未初始化时，`target_available_from` 为空字符串。最初的域名采集
起点超出保留期后，该值会随保留期窗口向前移动。

### 查询

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

`from` 与 `to` 是可选的 RFC 3339 时间戳，同时存在时必须满足 `from < to`。
五个列表过滤字段都是精确匹配允许列表，并按 AND 组合。`group_tags` 匹配
`outbound_group`，不会匹配 `group_path` 中的每一级祖先。域名过滤值会按存储
域名的规则规范化；空字符串显式选择 target 可用时段内没有域名的流量。每个过滤
字段最多接受 256 个值，每个值最多 1024 个 UTF-8 字节；`networks` 只接受
`tcp` 与 `udp`。

`search` 对当前聚合标签执行不区分大小写的子串匹配：分别是页面显示的路由路径、
目标域名、节点组或叶子出站 tag。搜索在聚合之后、计算 totals 与分页之前执行。

服务端依次执行过滤、聚合、搜索、稳定排序，最后才分页。`sort_by` 接受
capabilities 声明的字段，`sort_order` 为 `asc` 或 `desc`。默认按
`total_bytes` 降序。数值相同时使用确定性的维度/名称顺序打破平局，因此在同一
数据快照下分页边界稳定。

`page` 从 1 开始，默认为 1。`page_size` 默认为 50，最大为 200。
`total_rows` 是过滤与搜索之后、分页之前的总行数；`totals` 同样覆盖全部这些
行，而不只是当前页。

响应示例：

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

计数器使用十进制 JSON 字符串，避免 JavaScript 客户端丢失 64 位整数精度。
没有行匹配时，`actual_from` 与 `actual_to` 为空字符串。对于依赖 target detail
的查询，它们的范围与 totals 只覆盖 `target_available_from` 之后可用的域名
明细。
