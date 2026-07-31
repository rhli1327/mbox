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
内容计算 keyed HMAC，随机 HMAC 密钥随流量状态持久化。因此，同一存储身份内
相同配置的 revision 保持稳定，不同的独立存储身份之间不可比较；它不会泄露
配置密钥，也不是可移植的内容哈希。

### 配置示例与存储选择

不配置 `storage` 的旧格式仍选择 Bolt：

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

`path` 默认是 `traffic.db`，相对路径使用 mbox 的标准基础目录解析。
旧式 `path` 与 `storage` 不能同时设置。显式配置存储时，
`storage.type` 只能是 `bolt` 或 `postgres`。

下面的直连 PostgreSQL 示例特意使用 `observability` 数据库。DSN 可以选择
任意数据库，不要求数据库名为 `traffic`：

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

最小 PostgreSQL 配置会解析为 schema `public`、`schema_management: auto`、
最多 4 个连接、最少 1 个空闲连接、`connect_timeout: 10s`、
`statement_timeout: 30s`、身份文件 `traffic-instance.json`、spool 文件
`traffic-spool.db`、spool 容量 256 MiB、溢出策略 `drop_oldest` 和启动策略
`degraded`。

下面的示例显式展示所有 PostgreSQL 运维控制，并通过 `hy2-out` 传输逻辑
数据库 TCP 流：

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

PostgreSQL 存储的 dialer 只接受 `dialer.detour`。配置文件沿用根命令的标准
加载与合并机制：`-c/--config`、`-C/--config-directory` 和
`-D/--directory`。系统没有额外的 DSN 环境变量展开层。

| 字段 | 存储 | 含义与默认值 |
|------|------|--------------|
| `enabled` | 两者 | 启用采集、持久化和 REST 资源。默认 `false`。 |
| `path` | 旧式 Bolt | Bolt 路径，默认 `traffic.db`，与 `storage` 互斥。 |
| `instance_id` | PostgreSQL | 稳定实例身份；为空时读取或创建身份文件。 |
| `identity_path` | PostgreSQL | 身份文件，默认 `traffic-instance.json`。 |
| `storage.type` | 显式存储 | `bolt` 或 `postgres`；出现 `storage` 时必填。 |
| `storage.path` | 显式 Bolt | Bolt 路径，默认 `traffic.db`。 |
| `storage.dsn` | PostgreSQL | 必填的 pgx 连接串，通过 URI 的 database component 或 pgx `dbname` 关键字选择已存在的数据库。不要求数据库名为 `traffic`。 |
| `storage.schema` | PostgreSQL | 有效标识符，默认 `public`。 |
| `storage.schema_management` | PostgreSQL | `auto` 或 `validate`，默认 `auto`。 |
| `storage.dialer.detour` | PostgreSQL | 逻辑数据库 TCP 流使用的可选出站 tag。 |
| `storage.max_open_connections` | PostgreSQL | 最大连接数，默认 `4`，最小 `1`。 |
| `storage.min_idle_connections` | PostgreSQL | 最少空闲连接，默认 `1`，不能大于最大值。 |
| `storage.connect_timeout` | PostgreSQL | 正数连接超时，默认 `10s`。 |
| `storage.statement_timeout` | PostgreSQL | 正数操作超时，默认 `30s`。 |
| `spool.path` | PostgreSQL | 本地持久队列，默认 `traffic-spool.db`。 |
| `spool.max_size` | PostgreSQL | 逻辑容量，默认 256 MiB。 |
| `spool.overflow` | PostgreSQL | 仅支持 `drop_oldest`。 |
| `startup_policy` | PostgreSQL | `degraded` 或 `strict`，默认 `degraded`。 |

### PostgreSQL 所有权与 schema 生命周期

支持 PostgreSQL 14 及更高版本。服务器、DSN 选择的数据库、登录角色和授权都
必须预先存在。数据库名完全来自 `storage.dsn`；mbox 从不执行
`CREATE DATABASE`，schema 角色和运行时角色都不需要 superuser 或
`CREATEDB`。

使用 `schema_management: auto` 时，mbox 可以创建配置的 schema 和自身对象，
取得 schema advisory lock，校验内嵌迁移 checksum，并在 PostgreSQL 允许的
范围内以事务应用迁移；它不会创建数据库或角色。使用
`schema_management: validate` 时不执行 DDL，并要求精确兼容的 schema 版本，
当前为版本 3。缺失、不兼容、未来版本、checksum、鉴权和权限错误都会分类，
不会返回原始连接密钥。

管理员可以应用相同的内嵌迁移：

```bash
mbox tools traffic-statistics schema migrate
```

该命令读取标准配置的 PostgreSQL 连接并执行 schema 自动迁移，目前只支持
直连。若配置的存储使用 detour，会返回稳定的 detour-not-ready 错误。可在
出站启动后使用生产 `auto`，或给管理命令提供单独的直连管理配置。

以数据库 `observability`、自定义 schema `mbox_traffic`、schema 角色
`mbox_schema_admin` 和运行时角色 `mbox_data` 为例，最小权限部署通常授予：

- schema 角色对 `observability` 的 `CONNECT`，创建或升级 `mbox_traffic`
  所需的数据库/schema DDL 权限，以及 mbox 表、索引、约束和迁移 ledger 的
  所有权或等效权限；
- validate-only 运行时角色对 `observability` 的 `CONNECT`、对
  `mbox_traffic` 的 `USAGE`、对 mbox 表的 `SELECT`、`INSERT`、`UPDATE`、
  `DELETE`，以及验证迁移 ledger 所需的读取权限。

schema 升级新增表后应更新运行时授权。版本 3 包含：

| 对象 | 用途 |
|------|------|
| `mbox_traffic_instances` | 每实例身份和可用性元数据。 |
| `mbox_traffic_config_revisions` | 不透明路由配置 revision。 |
| `mbox_traffic_minute_summary` | 分钟级路由路径汇总。 |
| `mbox_traffic_minute_targets` | 分钟级目标明细。 |
| `mbox_traffic_ingest_batches` | 用于远端恰好一次效果的已接受持久批次身份。 |
| `mbox_traffic_migration_jobs` | 离线迁移身份、选择范围、状态和 fingerprint。 |
| `mbox_traffic_migration_revisions` | 已迁移 revision 元数据。 |
| `mbox_traffic_migration_batches` | 持久迁移游标和 marker 链。 |

### 首次运行、启动与本地持久性

- `Box.New` 验证选项，读取或原子创建身份，派生 keyed revision，打开并验证
  本地持久 spool，在不连接的情况下构造 PostgreSQL pool factory，并注册历史
  服务。
- 第一次远端 PostgreSQL 连接发生在 `Box.Start`，且在所选出站已经可以拨号
  之后。此时初始化或验证 schema、协调远端身份/revision 状态，并启动有序恢复
  与投递。

本地身份或 spool 失败在两种策略下都会使构造失败。`strict` 会返回首次远端错误
并关闭内部存储。`degraded` 会保留本地持久 spool 和重试 worker，允许代理流量
继续工作，并在已提交查询边界可用前返回 HTTP 503。永久错误需要运维人员修正。

生产身份的选择顺序是显式 `instance_id`、持久身份、生成的 UUID。默认身份文件
以 0600 模式原子写入。身份独立于 spool。写入会先在本地持久化再唤醒投递，
批次 ID 跨重启保持稳定，并按旧批次优先排空。PostgreSQL 已接受批次身份使每个
持久批次在远端产生恰好一次效果。

唯一的溢出行为是 `drop_oldest`。丢弃数据是允许的遥测损失，不是计费语义。
不要用其他 writer 检查、复制、替换或打开正在使用的 spool。身份与 spool 是
敏感备份状态；只恢复其中一部分而不匹配数据库状态可能导致身份或 revision
冲突。

### TLS、detour、一致性与可用性

直连不需要 detour。支持 `sslmode=disable`，也支持带 `sslrootcert` 的
`sslmode=verify-full`。pgx 在逻辑连接之上负责 PostgreSQL TLS。TLS 与所选
detour 相互独立：detour 既不会禁用也不会强制 TLS。即使出站使用 UDP/QUIC，
数据库流仍是逻辑 TCP；Hysteria2 是已测试的 logical-TCP-over-QUIC 路径。
缺失出站或只支持 UDP、不能承载 stream 的出站会明确失败。数据库自身流量不会
递归计入用户流量。

Bolt 查询保持现有的内存 pending overlay。PostgreSQL 查询会建立持久化的已提交
边界，再打开远端 snapshot。过滤、聚合、排序、totals 和分页使用同一个一致性
snapshot；系统不会用成功但过期的远端结果替代。超时或不可用映射为稳定、脱敏
的 HTTP 503；客户端显式取消仍保持取消语义。

### 离线 Bolt 迁移

```bash
mbox tools traffic-statistics migrate
```

该命令使用标准根配置加载。目标必须解析为已配置的 PostgreSQL；DSN、schema 和
detour 都来自该配置。

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `--source` | `traffic.db` | Bolt 源数据库。 |
| `--instance-id` | 空 | 目标实例身份。 |
| `--batch-size` | `500` | 每个事务的源记录数。 |
| `--from` | 空 | 包含的 RFC 3339 分钟下界。 |
| `--to` | 空 | 不包含的 RFC 3339 分钟上界。 |
| `--resume` | 空 | 预期的确定性迁移 ID。 |
| `--active-config-revision` | 空 | 恢复状态的活跃历史 revision。 |
| `--dry-run` | `false` | 验证、比较且不执行持久写入。 |

Bolt 源必须离线。锁竞争会明确失败；源字节和元数据保持不变。在构造 runtime 或
访问网络之前会验证完整的原始 Bolt 数据域。summary/target 记录、可用性、
revision、revision key 和无符号计数器都会保留。

安全恢复模式插入缺失行、跳过完全相同行并在冲突时停止。迁移 ID 和游标范围是
确定性的。中断后从持久 marker 链继续；不确定的提交会按恰好一次语义协调。
`--from` 包含下界，`--to` 不包含上界。`--dry-run` 执行零持久化写入并强制
validate-only。进度 JSON 写入 stderr，最终唯一结果 JSON 对象写入 stdout。

正常生产清理保留 30 天，但离线迁移不应用该保留期截止条件：它会恢复每一条选中
的历史源记录，包括早于 30 天的记录，除非运维人员使用 `--from` 或 `--to`
缩小恢复范围。正常生产清理随后可能删除超出保留窗口的已恢复记录。

迁移没有 `--dsn`、`--schema`、`--merge` 或 `--delete-source`；继承的
`--outbound` 会被拒绝。直连模式不构造出站 runtime。detour 模式构造有作用域的
network-namespace、DNS transport/router、network、connection/router、
outbound、endpoint 和 certificate 依赖图，并且仅在 Hysteria2 出站 realm
需要时才构造并启动 HTTP-client manager。注册的 inbound manager 创建并启动
零个入站对象或监听器。不会启动 API、traffic collector、NTP、cache-file
service 或 debug server。迁移不会删除 Bolt 源，也不支持在线 snapshot。

### 监控、恢复与安全

成功查询证明请求的已提交边界当时可用。脱敏的 HTTP 503 表示该边界无法在请求
context 内提交并读取。启动错误会区分本地持久性、可重试远端和永久分类错误。
管理员可以查看实例、已接受批次和迁移元数据；运维人员可以从外部监控 spool
文件大小与文件系统健康。

不存在新的公开流量统计 readiness 或 status 端点。内部队列、丢弃、重试、
flush 和错误类别字段不属于公开 API。`degraded` 会自动恢复瞬时故障；鉴权、
数据库缺失、权限、不兼容/未来 schema、身份、revision 和碰撞错误需要人工修正。
磁盘满、权限、损坏和锁错误属于本地持久性故障，不应盲目删除 spool。

DSN 和密码属于敏感信息，系统没有命令行 DSN override。应限制配置、身份和
spool 的权限。错误和 HTTP 响应会脱敏，但流量历史包含敏感目标，因此 API
监听器应启用鉴权并只绑定可信网络。行和 REST 视图按实例隔离，不提供跨实例
REST 聚合。计数器继续使用十进制 JSON 字符串。正常保留期为 30 天
（30 days）。

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
- target detail bucket 在相同维度之外保存纯 `destination_domain`，当前构建
  还会保存作为 fallback 的目标 IP。域名明细从支持域名的构建首次打开数据库后
  的下一个完整分钟边界开始记录，并有意排除升级所在的不完整分钟。
  `target_available_from` 取该域名明细边界与当前保留期边界两者中较晚的时间。

系统不会把升级前仅存在于 summary 的流量伪造成空域名或“未知域名”。因此，按
域名聚合以及任何带 `destination_domains` 过滤的查询只覆盖 target detail
可用时段。在该时段内，空 `destination_domain` 表示路由时没有可用的有效逻辑
域名，其中也包括纯 IP 连接。

通用的 `destination` 维度晚于纯域名明细引入。旧数据库中空域名记录所对应的
IP 无法恢复，因此系统不会把这些旧记录伪造成完整目标数据。
`destination_available_from` 表示开始完整采集“域名或 IP”目标的第一个完整
分钟，并同样受当前保留期边界约束。按 `destination` 聚合以及任何带
`destinations` 的查询只覆盖这个较晚的时段。新建数据库通常会把两个可用边界
初始化为同一分钟。

同一数据库的目标完整性边界假设统计构建只做单向升级。若临时降级到不写
fallback IP 的旧构建后再升级，系统无法区分降级期间的缺失 IP 与真正未知目标，
已有的 `destination_available_from` 将不再可靠。此时应恢复升级前的数据库
备份，或使用新的流量数据库重新建立完整性边界。

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
| `destination` | 路由完成并交给 outbound 时冻结的首选逻辑目标：有有效逻辑域名时使用域名，否则使用 `Destination.Addr`。域名转为小写并去掉一个尾点；IP 使用规范文本，IPv4-mapped IPv6 会还原为 IPv4，IPv6 zone 会移除。它既不是代理节点服务器地址，也不是域名解析后实际拨号的 IP。 |
| `destination_type` | 与 `destination` 对应的 `domain` 或 `ip`；两者都不可用时为空。 |
| `destination_domain` | 为兼容保留的纯域名维度。优先取有效的 `Destination.Fqdn`，否则取有效的嗅探或反向映射 `Metadata.Domain`。无效名称和纯 IP 目标为空，绝不会在该字段中放入 IP。 |
| `connections` | 已解析的 TCP 流或 UDP 数据包会话数。已解析但没有字节的分派仍计为一次。 |

v2 查询支持以下 `group_by`：

| 值 | 聚合语义 |
|----|----------|
| `route_path` | 保留完整 summary 维度（`config_revision`、路由路径、叶子 tag/type 与网络），折叠目标维度。这是默认值。 |
| `destination` | 只按首选的规范化域名或 IP 目标聚合，跨配置 revision、网络、路由、组和节点汇总。新版目标视图应使用该聚合。 |
| `destination_domain` | 为兼容保留的纯域名聚合；纯 IP 与其他无域名流量会进入同一个空域名行。 |
| `outbound_group` | 只按最内层/叶子父组标签聚合，跨 revision 与网络汇总。 |
| `actual_outbound` | 只按叶子出站 tag 聚合，跨 revision 与网络汇总。 |

响应中的每一行始终包含全部维度字段；当前聚合方式不适用的字段为空字符串，
`group_path` 则为空数组。

### REST API v2

两种监听器都提供：

| 资源 | 方法 | 用途 |
|------|------|------|
| `/mbox/v2/traffic/capabilities` | `GET` | 获取维度、聚合方式、排序字段、存储参数以及域名/目标明细可用时间。 |
| `/mbox/v2/traffic/query` | `POST` | 过滤、聚合、搜索、排序并分页查询汇总行。 |

当前构建只挂载 v2 路径，不保留不兼容的 v1 端点。

鉴权沿用监听器配置：

| 监听器 | 鉴权 |
|--------|------|
| Clash API | `experimental.clash_api.secret` 非空时使用 `Authorization: Bearer <secret>`；为空时不鉴权。 |
| 原生 API 服务 | 服务的 `secret` 非空时使用 `Authorization: Bearer <secret>`；为空时不鉴权。 |

!!! warning

    流量历史，特别是目标域名与 IP，属于敏感信息。监听器 secret 为空时，应只
    监听回环地址或可信私网。

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

历史存储尚未初始化时，`target_available_from` 为空字符串。最初的域名采集
起点超出保留期后，该值会随保留期窗口向前移动。
`destination_available_from` 对完整的“域名或 IP”明细遵循同样规则；升级
已有数据库时，它可能晚于 `target_available_from`。

### 查询

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

`from` 与 `to` 是可选的 RFC 3339 时间戳，同时存在时必须满足 `from < to`。
六个列表过滤字段都是精确匹配允许列表；同一过滤字段内按 OR 组合，不同维度之间
按 AND 组合。`group_tags` 匹配 `outbound_group`，不会匹配 `group_path` 中的
每一级祖先。`destinations` 接受规范化域名或 IP，IPv4 与 IPv6 输入都会转为
规范形式；空字符串显式选择既没有有效域名、也没有有效 fallback IP 的流量。
`destination_domains` 保持纯域名语义，其空字符串会选择没有有效域名的流量，
其中包括已知的纯 IP 流量。每个过滤字段最多接受 256 个值，每个值最多 1024 个
UTF-8 字节；`networks` 只接受 `tcp` 与 `udp`。

`search` 对当前聚合标签执行不区分大小写的子串匹配：分别是页面显示的路由路径、
首选目标、兼容域名、节点组或叶子出站 tag。搜索在聚合之后、计算 totals 与
分页之前执行。

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

计数器使用十进制 JSON 字符串，避免 JavaScript 客户端丢失 64 位整数精度。
没有行匹配时，`actual_from` 与 `actual_to` 为空字符串。对于依赖 target detail
的查询，纯域名查询只覆盖 `target_available_from` 之后的明细；按首选目标聚合
或过滤时，只覆盖 `destination_available_from` 之后完整的域名或 IP 明细。
