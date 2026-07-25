# 流量统计

!!! warning "mbox traffic-statistics 构建"

    这是 mbox 扩展，并非上游 sing-box API 的组成部分。

流量统计按路由与实际出站持久化上传、下载总量。默认不启用采集。

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

只有启用此选项，并且对应的 [Clash API](./clash-api/) 监听器或
[sing-box API 服务](../service/api/)配置了非空 secret 时，才会挂载 REST
端点。

#### path

流量统计数据库路径。

留空时使用 `traffic.db`。相对路径遵循 sing-box 的标准基础路径解析规则。

修改配置不会把不同配置产生的数据合并。每行都包含不透明的
`config_revision`，从而区分不同配置中复用的相同标签。该值根据解析后的配置
选项序列化内容计算 keyed HMAC；随机 HMAC 密钥存放在流量数据库中。因此，同一
数据库内相同序列化配置的 revision 保持稳定，但不同数据库之间不可比较。它
不会泄露配置密钥，也不能当作可移植的内容哈希；运行效果等价的两份配置不保证
得到相同 revision。

### 当前存储行为

| 属性 | 当前值 |
|------|--------|
| 时间桶间隔 | 1 分钟 |
| 保留期 | 30 天 |
| 活跃流采样与待写数据落盘 tick | 5 秒 |

这些数值只是当前实现的默认值，并非稳定的协议承诺。客户端必须从
capabilities 端点读取 `bucket_seconds` 和 `retention_seconds`，不能硬编码。
采样和刷新周期也可能在不同 mbox 构建之间变化。

当前实现会在 5 秒刷新 tick 到达时把待写数据写入磁盘。

正常关闭时会刷新剩余的待写计数。
进程或机器异常退出时，最近尚未采样及尚未落盘的增量可能丢失（按当前实现，约为
最多两个 5 秒 tick）。

对于活跃的长连接，增量会归入采样时刻所在的时间桶；流关闭时还会采集最后一段
增量。因此，时间范围边界可能有约一个采样周期的近似误差，时间桶时间戳不适合
当作计费级事件时间戳。

### 维度与计数

假设路由选择了 `AI` selector，随后选择 `AI-Auto` URLTest 组，最终选择
`ai-node`，相应维度为：

```json
{
  "route_tag": "AI",
  "group_path": ["AI", "AI-Auto"],
  "actual_outbound_tag": "ai-node"
}
```

| 字段 | 含义 |
|------|------|
| `route_tag` | 匹配的路由动作所选择的出站标签；若没有规则覆盖，则为最终路由选择的标签。它不是路由规则本身的可选 tag，在示例中始终为 `AI`。 |
| `group_path` | 从路由选中的组到最内层组，按实际经过顺序排列的组标签。数组不包含叶子出站；直接路由到叶子时为空数组。 |
| `actual_outbound_tag` | 针对该网络实际选择并分派的叶子出站标签。若流在解析出叶子之前结束，则不会记录该流。 |
| `actual_outbound_type` | 实际叶子出站的类型。 |
| `network` | `tcp` 或 `udp`。URLTest 会按网络分别解析实际选择。 |
| `uplink_bytes` | 从入站/客户端发往目标方向的逻辑负载字节数。 |
| `downlink_bytes` | 从目标返回入站/客户端方向的逻辑负载字节数。 |
| `connections` | 已解析实际叶子的 TCP 路由流或 UDP 数据包会话数。已解析但没有字节的分派（包括之后失败的分派）仍计为一次。 |
| `config_revision` | 序列化配置选项在本数据库中的不透明 revision。 |

这条路径描述逻辑出站组的分派过程。叶子下层使用的出站 detour 或传输 socket
不会作为 `group_path` 的额外元素，也不会取代 `actual_outbound_tag`。

`uplink_bytes` 与 `downlink_bytes` 使用 `logical_payload` 统计口径，计量路由流
逻辑边界观察到的字节，而不是出站链路上的字节。它们不包含加密传输开销、
协议封装、填充或链路层开销。具体而言，上传流量在从入站/客户端方向成功读取后
计数，并不保证同一批字节已成功写到目标；下载流量在写向入站/客户端方向时计数。
这些计数用于运行流量核算，不代表端到端送达证明。

为保持这一计数边界，启用历史统计后，被跟踪流量会禁用可替换 reader/writer
以及 splice、zero-copy 等快速复制路径，因此吞吐可能低于普通构建。这也是持久化
历史保持按需启用、仅用于确有统计需求机器的原因。

目前不采集目标域名或目标 IP 统计，因此 capabilities 响应中的
`features.targets` 为 `false`。

### REST API

两种受支持的监听器都提供相同的 REST 资源：

| 资源 | 方法 | 用途 |
|------|------|------|
| `/mbox/v1/traffic/capabilities` | `GET` | 获取 schema、统计口径、维度与当前存储参数。 |
| `/mbox/v1/traffic/query` | `POST` | 查询汇总行。 |

监听器及其鉴权来源不同：

| 监听器 | 配置 | 鉴权 |
|--------|------|------|
| Clash API | `experimental.clash_api.external_controller` | `Authorization: Bearer <secret>`，密钥取自 `experimental.clash_api.secret`。 |
| 原生 sing-box API | 顶层 `"type": "api"` 服务 | `Authorization: Bearer <secret>`，密钥取自该 API 服务的 `secret`。 |

对应监听器的 secret 为空时，不会在该监听器上挂载流量统计资源；监听器上的其他
资源仍沿用原有鉴权行为。

例如：

```bash
curl \
  -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:9090/mbox/v1/traffic/capabilities
```

当前 capabilities 响应类似：

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

### 查询

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

`from` 和 `to` 是可选的 RFC 3339 时间戳；两者同时存在时必须满足
`from < to`。每个过滤字段都是精确匹配的允许列表。省略或传入空列表表示接受
全部值；不同过滤字段之间按 AND 组合。每个过滤字段最多接受 256 个值，每个值
最多 1024 个 UTF-8 字节；`networks` 只接受 `tcp` 和 `udp`。

查询结果会跨所有重叠的 1 分钟时间桶按维度汇总，而不返回逐桶时间序列。
`actual_from` 和 `actual_to` 表示实际参与匹配结果的完整时间桶范围，因此可能
超出请求时间戳的精确边界。

结果按 `uplink_bytes + downlink_bytes` 从大到小排列。默认限制为 500 行，
允许的最大限制为 5000 行。省略 limit 或传入 0 时使用默认值；负数或超过
5000 的 limit 会被拒绝。

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

计数器使用十进制 JSON 字符串，避免 JavaScript 客户端丢失 64 位整数精度。

空结果为：

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

没有行匹配时会省略 `actual_from` 与 `actual_to`。当匹配行数超过生效的 limit
时，只返回流量最大的若干行，并将 `truncated` 设为 `true`。`totals` 在应用
limit 之前根据全部匹配行计算，因此即使明细行被截断，它仍是完整汇总。目前没有
分页；需要每一行明细时，请提高 limit 或缩小过滤范围。
