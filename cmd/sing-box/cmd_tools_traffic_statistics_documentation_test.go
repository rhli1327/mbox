package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/trafficcontrol"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/schema"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
)

const (
	trafficStatisticsBoltExampleMarker   = "<!-- mbox-test:traffic-statistics-bolt -->"
	trafficStatisticsDirectExampleMarker = "<!-- mbox-test:traffic-statistics-postgres-direct -->"
	trafficStatisticsDetourExampleMarker = "<!-- mbox-test:traffic-statistics-postgres-detour -->"
	trafficStatisticsExampleEndMarker    = "<!-- mbox-test:end -->"
)

type trafficStatisticsDocumentation struct {
	name    string
	content string
}

func TestTrafficStatisticsGeneratedSchemaIsCurrent(t *testing.T) {
	generatedContent, err := schema.Generate(
		include.Context(context.Background()),
		reflect.TypeFor[option.Options](),
	)
	if err != nil {
		t.Fatal("generate current schema:", err)
	}
	committedContent, err := os.ReadFile(
		filepath.Join("..", "..", "docs", "schema.json"),
	)
	if err != nil {
		t.Fatal("read committed schema:", err)
	}
	generated := decodeTrafficStatisticsSchema(t, generatedContent)
	committed := decodeTrafficStatisticsSchema(t, committedContent)

	experimental := trafficStatisticsSchemaObject(
		t,
		committed,
		"$defs",
		"ExperimentalOptions",
		"properties",
		"traffic_statistics",
	)
	if reference, _ := experimental["$ref"].(string); reference != "#/$defs/TrafficStatisticsOptions" {
		t.Fatalf(
			"ExperimentalOptions traffic_statistics reference = %q, want %q",
			reference,
			"#/$defs/TrafficStatisticsOptions",
		)
	}

	expectedProperties := map[string][]string{
		"TrafficStatisticsOptions": {
			"enabled",
			"identity_path",
			"instance_id",
			"path",
			"spool",
			"startup_policy",
			"storage",
		},
		"TrafficStatisticsStorageOptions": {
			"connect_timeout",
			"dialer",
			"dsn",
			"max_open_connections",
			"min_idle_connections",
			"path",
			"schema",
			"schema_management",
			"statement_timeout",
			"type",
		},
		"TrafficStatisticsSpoolOptions": {
			"max_size",
			"overflow",
			"path",
		},
	}
	for definition, expected := range expectedProperties {
		generatedDefinition := trafficStatisticsSchemaObject(
			t,
			generated,
			"$defs",
			definition,
		)
		committedDefinition := trafficStatisticsSchemaObject(
			t,
			committed,
			"$defs",
			definition,
		)
		if !reflect.DeepEqual(generatedDefinition, committedDefinition) {
			t.Errorf("%s in docs/schema.json does not match generated schema", definition)
		}
		if additionalProperties, loaded := committedDefinition["additionalProperties"].(bool); !loaded || additionalProperties {
			t.Errorf("%s additionalProperties = %v, want false", definition, committedDefinition["additionalProperties"])
		}
		properties := trafficStatisticsSchemaObject(
			t,
			committedDefinition,
			"properties",
		)
		actual := make([]string, 0, len(properties))
		for property := range properties {
			actual = append(actual, property)
		}
		sort.Strings(actual)
		sort.Strings(expected)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%s properties = %v, want %v", definition, actual, expected)
		}
	}
}

func TestTrafficStatisticsDocumentationExamplesResolve(t *testing.T) {
	documents := readTrafficStatisticsDocumentation(t)
	type resolvedExamples struct {
		bolt   option.ResolvedTrafficStatisticsOptions
		direct option.ResolvedTrafficStatisticsOptions
		detour option.ResolvedTrafficStatisticsOptions
	}
	resolvedByDocument := make(map[string]resolvedExamples, len(documents))
	for _, document := range documents {
		bolt := resolveTrafficStatisticsDocumentationExample(
			t,
			document,
			trafficStatisticsBoltExampleMarker,
		)
		direct := resolveTrafficStatisticsDocumentationExample(
			t,
			document,
			trafficStatisticsDirectExampleMarker,
		)
		detour := resolveTrafficStatisticsDocumentationExample(
			t,
			document,
			trafficStatisticsDetourExampleMarker,
		)
		resolvedByDocument[document.name] = resolvedExamples{
			bolt:   bolt,
			direct: direct,
			detour: detour,
		}

		if !bolt.Enabled ||
			bolt.StorageType != option.TrafficStatisticsStorageTypeBolt ||
			bolt.Path != "traffic.db" ||
			bolt.InstanceID != "" ||
			bolt.IdentityPath != "" ||
			bolt.SpoolPath != "" {
			t.Errorf("%s Bolt example resolved unexpectedly: %#v", document.name, bolt)
		}
		assertTrafficStatisticsExampleDatabase(t, document.name+" direct", direct.DSN, "observability")
		if direct.StorageType != option.TrafficStatisticsStorageTypePostgres ||
			direct.Dialer.Detour != "" ||
			direct.Schema != "public" ||
			direct.SchemaManagement != option.TrafficStatisticsSchemaManagementAuto ||
			direct.MaxOpenConnections != 4 ||
			direct.MinIdleConnections != 1 ||
			direct.ConnectTimeout != 10*time.Second ||
			direct.StatementTimeout != 30*time.Second ||
			direct.IdentityPath != "traffic-instance.json" ||
			direct.SpoolPath != "traffic-spool.db" ||
			direct.SpoolMaxSize != 256<<20 ||
			direct.SpoolOverflow != option.TrafficStatisticsSpoolOverflowDropOldest ||
			direct.StartupPolicy != option.TrafficStatisticsStartupPolicyDegraded {
			t.Errorf("%s direct PostgreSQL example resolved unexpectedly: %#v", document.name, direct)
		}
		assertTrafficStatisticsExampleDatabase(t, document.name+" detour", detour.DSN, "observability")
		if detour.StorageType != option.TrafficStatisticsStorageTypePostgres ||
			detour.Schema != "mbox_traffic" ||
			detour.SchemaManagement == "" ||
			detour.Dialer.Detour != "hy2-out" ||
			detour.MaxOpenConnections == 4 ||
			detour.MinIdleConnections == 1 ||
			detour.ConnectTimeout == 10*time.Second ||
			detour.StatementTimeout == 30*time.Second ||
			detour.IdentityPath == "" ||
			detour.SpoolPath == "" ||
			detour.SpoolMaxSize == 256<<20 ||
			detour.SpoolOverflow != option.TrafficStatisticsSpoolOverflowDropOldest ||
			detour.StartupPolicy != option.TrafficStatisticsStartupPolicyStrict {
			t.Errorf("%s detoured PostgreSQL example does not demonstrate explicit options: %#v", document.name, detour)
		}
	}
	if !reflect.DeepEqual(resolvedByDocument["English"], resolvedByDocument["Chinese"]) {
		t.Fatal("English and Chinese traffic-statistics examples resolve differently")
	}
}

func TestTrafficStatisticsDocumentationMatchesCLI(t *testing.T) {
	if actual := commandToolsTrafficStatisticsSchemaMigrate.CommandPath(); actual != "mbox tools traffic-statistics schema migrate" {
		t.Fatalf("schema command path = %q", actual)
	}
	if actual := commandToolsTrafficStatisticsMigrate.CommandPath(); actual != "mbox tools traffic-statistics migrate" {
		t.Fatalf("migration command path = %q", actual)
	}

	migrationCommand := newTrafficStatisticsMigrateCommand()
	var flags []string
	migrationCommand.Flags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, "--"+flag.Name)
	})
	sort.Strings(flags)
	expectedFlags := []string{
		"--active-config-revision",
		"--batch-size",
		"--dry-run",
		"--from",
		"--instance-id",
		"--resume",
		"--source",
		"--to",
	}
	if !reflect.DeepEqual(flags, expectedFlags) {
		t.Fatalf("migration flags = %v, want %v", flags, expectedFlags)
	}

	for _, document := range readTrafficStatisticsDocumentation(t) {
		for _, commandPath := range []string{
			"mbox tools traffic-statistics schema migrate",
			"mbox tools traffic-statistics migrate",
		} {
			requireTrafficStatisticsDocumentationText(t, document, commandPath)
		}
		for _, flag := range expectedFlags {
			requireTrafficStatisticsDocumentationText(t, document, flag)
		}
		for _, text := range []string{
			"-c/--config",
			"-C/--config-directory",
			"-D/--directory",
			"--outbound",
			"--dsn",
			"--schema",
			"--merge",
			"--delete-source",
		} {
			requireTrafficStatisticsDocumentationText(t, document, text)
		}
	}
}

func TestTrafficStatisticsDocumentationCoversPostgresOperations(t *testing.T) {
	commonLiterals := []string{
		"PostgreSQL 14",
		"CREATE DATABASE",
		"schema_management",
		"auto",
		"validate",
		"public",
		"mbox_traffic",
		"sslmode=disable",
		"sslmode=verify-full",
		"traffic-instance.json",
		"traffic-spool.db",
		"256 MiB",
		"drop_oldest",
		"degraded",
		"strict",
		"HTTP 503",
		"30 days",
		"mbox_traffic_instances",
		"mbox_traffic_config_revisions",
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_targets",
		"mbox_traffic_ingest_batches",
		"mbox_traffic_migration_jobs",
		"mbox_traffic_migration_revisions",
		"mbox_traffic_migration_batches",
		"/mbox/v2/traffic/capabilities",
		"/mbox/v2/traffic/query",
		"logical_payload",
	}
	semanticRequirements := map[string][]string{
		"English": {
			"arbitrary database",
			"mbox never executes `CREATE DATABASE`",
			"The first remote PostgreSQL connection is made during `Box.Start`",
			"`strict` returns the initial remote failure",
			"`degraded` keeps the local durable spool",
			"PostgreSQL queries establish a durable committed boundary",
			"TLS is independent of the selected detour",
			"The Bolt source must be offline",
			"`--dry-run` performs zero persistent writes",
			"Errors and HTTP responses are redacted",
			"No new public traffic-statistics readiness or status endpoint exists",
			"decimal JSON strings",
		},
		"Chinese": {
			"任意数据库",
			"mbox 从不执行 `CREATE DATABASE`",
			"第一次远端 PostgreSQL 连接发生在 `Box.Start`",
			"`strict` 会返回首次远端错误",
			"`degraded` 会保留本地持久 spool",
			"PostgreSQL 查询会建立持久化的已提交边界",
			"TLS 与所选 detour 相互独立",
			"Bolt 源必须离线",
			"`--dry-run` 执行零持久化写入",
			"错误和 HTTP 响应会脱敏",
			"不存在新的公开流量统计 readiness 或 status 端点",
			"十进制 JSON 字符串",
		},
	}
	for _, document := range readTrafficStatisticsDocumentation(t) {
		for _, literal := range commonLiterals {
			requireTrafficStatisticsDocumentationText(t, document, literal)
		}
		for _, requirement := range semanticRequirements[document.name] {
			requireTrafficStatisticsDocumentationText(t, document, requirement)
		}
	}
}

func TestTrafficStatisticsDocumentationMatchesMigrationRuntimeScope(t *testing.T) {
	ordinaryHysteria := option.Options{
		Outbounds: []option.Outbound{
			{
				Type:    C.TypeHysteria2,
				Options: &option.Hysteria2OutboundOptions{},
			},
		},
	}
	if trafficStatisticsMigrationNeedsHTTPClient(ordinaryHysteria) {
		t.Fatal("ordinary Hysteria2 outbound unexpectedly requires a migration HTTP-client manager")
	}
	realmHysteria := option.Options{
		Outbounds: []option.Outbound{
			{
				Type: C.TypeHysteria2,
				Options: &option.Hysteria2OutboundOptions{
					Realm: &option.Hysteria2Realm{},
				},
			},
		},
	}
	if !trafficStatisticsMigrationNeedsHTTPClient(realmHysteria) {
		t.Fatal("Hysteria2 realm did not require a migration HTTP-client manager")
	}

	requirements := map[string][]string{
		"English": {
			"scoped network-namespace, DNS transport/router, network, connection/router, outbound, endpoint, and certificate dependency graph",
			"HTTP-client manager only when a Hysteria2 outbound realm requires it",
			"registered inbound manager creates and starts zero inbound objects or listeners",
			"No API, traffic collector, NTP, cache-file service, or debug server is started",
		},
		"Chinese": {
			"有作用域的 network-namespace、DNS transport/router、network、connection/router、outbound、endpoint 和 certificate 依赖图",
			"仅在 Hysteria2 出站 realm 需要时才构造并启动 HTTP-client manager",
			"注册的 inbound manager 创建并启动零个入站对象或监听器",
			"不会启动 API、traffic collector、NTP、cache-file service 或 debug server",
		},
	}
	for _, document := range readTrafficStatisticsDocumentation(t) {
		for _, requirement := range requirements[document.name] {
			requireTrafficStatisticsDocumentationText(t, document, requirement)
		}
	}
}

func TestTrafficStatisticsDocumentationCoversMigrationRetentionSemantics(t *testing.T) {
	if trafficcontrol.HistoryRetention != 30*24*time.Hour {
		t.Fatalf("production retention = %v, want 30 days", trafficcontrol.HistoryRetention)
	}

	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationTestConfig(t, configPath, `{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "storage": {
        "type": "postgres",
        "dsn": "host=invalid.invalid user=example password=example dbname=observability",
        "schema_management": "validate"
      }
    }
  }
}`)
	previousMigrate := migrateBoltTrafficStatisticsToPostgres
	defer func() {
		migrateBoltTrafficStatisticsToPostgres = previousMigrate
	}()
	var captured []trafficcontrol.TrafficStatisticsMigrationOptions
	migrateBoltTrafficStatisticsToPostgres = func(
		_ context.Context,
		_ log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		captured = append(captured, options)
		return trafficcontrol.TrafficStatisticsMigrationResult{
			Completed: true,
		}, nil
	}
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:    "unused.db",
			BatchSize: 500,
		},
	)
	if err != nil {
		t.Fatal("run unbounded migration:", err)
	}
	const (
		fromText = "2025-01-02T03:05:00Z"
		toText   = "2025-01-02T03:06:00Z"
	)
	_, _, err = runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:    "unused.db",
			BatchSize: 500,
			From:      fromText,
			To:        toText,
		},
	)
	if err != nil {
		t.Fatal("run bounded migration:", err)
	}
	if len(captured) != 2 {
		t.Fatalf("captured migration calls = %d, want 2", len(captured))
	}
	if !captured[0].From.IsZero() || !captured[0].To.IsZero() {
		t.Fatalf(
			"unbounded migration gained an implicit retention cutoff: from=%v to=%v",
			captured[0].From,
			captured[0].To,
		)
	}
	expectedFrom, err := time.Parse(time.RFC3339, fromText)
	if err != nil {
		t.Fatal(err)
	}
	expectedTo, err := time.Parse(time.RFC3339, toText)
	if err != nil {
		t.Fatal(err)
	}
	if !captured[1].From.Equal(expectedFrom) || !captured[1].To.Equal(expectedTo) {
		t.Fatalf(
			"migration did not preserve explicit bounds: from=%v to=%v",
			captured[1].From,
			captured[1].To,
		)
	}

	requirements := map[string][]string{
		"English": {
			"Normal production cleanup retains 30 days, but offline migration does not apply that retention cutoff",
			"restores every selected historical source row, including rows older than 30 days, unless the operator narrows the restore with `--from` or `--to`",
			"Normal production cleanup may later remove restored rows outside its retention window",
		},
		"Chinese": {
			"正常生产清理保留 30 天，但离线迁移不应用该保留期截止条件",
			"会恢复每一条选中的历史源记录，包括早于 30 天的记录，除非运维人员使用 `--from` 或 `--to` 缩小恢复范围",
			"正常生产清理随后可能删除超出保留窗口的已恢复记录",
		},
	}
	for _, document := range readTrafficStatisticsDocumentation(t) {
		for _, requirement := range requirements[document.name] {
			requireTrafficStatisticsDocumentationText(t, document, requirement)
		}
	}
}

func TestTrafficStatisticsDocumentationCoversPostgresDSNForms(t *testing.T) {
	for name, dsn := range map[string]string{
		"URI":           "postgres://example:example@localhost/observability?sslmode=disable",
		"keyword/value": "host=localhost user=example password=example dbname=observability sslmode=disable",
	} {
		config, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("parse %s pgx DSN: %v", name, err)
		}
		if config.ConnConfig.Database != "observability" {
			t.Fatalf(
				"%s pgx DSN database = %q, want observability",
				name,
				config.ConnConfig.Database,
			)
		}
	}

	requirements := map[string][]string{
		"English": {
			"selects the existing database through either the URI database component or the pgx `dbname` keyword",
			"No database named `traffic` is required",
		},
		"Chinese": {
			"通过 URI 的 database component 或 pgx `dbname` 关键字选择已存在的数据库",
			"不要求数据库名为 `traffic`",
		},
	}
	for _, document := range readTrafficStatisticsDocumentation(t) {
		for _, requirement := range requirements[document.name] {
			requireTrafficStatisticsDocumentationText(t, document, requirement)
		}
	}
}

func decodeTrafficStatisticsSchema(t *testing.T, content []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatal("decode schema:", err)
	}
	return decoded
}

func trafficStatisticsSchemaObject(
	t *testing.T,
	root map[string]any,
	path ...string,
) map[string]any {
	t.Helper()
	current := root
	for _, part := range path {
		next, loaded := current[part].(map[string]any)
		if !loaded {
			t.Fatalf("schema path %s is missing or is not an object", strings.Join(path, "."))
		}
		current = next
	}
	return current
}

func readTrafficStatisticsDocumentation(t *testing.T) []trafficStatisticsDocumentation {
	t.Helper()
	documents := []trafficStatisticsDocumentation{
		{name: "English"},
		{name: "Chinese"},
	}
	for index := range documents {
		name := "traffic-statistics.md"
		if documents[index].name == "Chinese" {
			name = "traffic-statistics.zh.md"
		}
		content, err := os.ReadFile(filepath.Join("..", "..", "docs", "configuration", "experimental", name))
		if err != nil {
			t.Fatalf("read %s documentation: %v", documents[index].name, err)
		}
		documents[index].content = string(content)
	}
	return documents
}

func resolveTrafficStatisticsDocumentationExample(
	t *testing.T,
	document trafficStatisticsDocumentation,
	marker string,
) option.ResolvedTrafficStatisticsOptions {
	t.Helper()
	if count := strings.Count(document.content, marker); count != 1 {
		t.Fatalf("%s marker %q count = %d, want 1", document.name, marker, count)
	}
	if count := strings.Count(document.content, trafficStatisticsExampleEndMarker); count != 3 {
		t.Fatalf("%s example end marker count = %d, want 3", document.name, count)
	}
	expression := regexp.MustCompile(
		`(?s)` +
			regexp.QuoteMeta(marker) +
			`[ \t]*\n+` +
			"```json[ \t]*\n" +
			`(.*?)` +
			"\n```[ \t]*\n+" +
			regexp.QuoteMeta(trafficStatisticsExampleEndMarker),
	)
	matches := expression.FindStringSubmatch(document.content)
	if len(matches) != 2 {
		t.Fatalf("%s marker %q does not contain exactly one JSON block", document.name, marker)
	}
	if strings.Count(matches[0], "```json") != 1 || strings.Count(matches[0], "```") != 2 {
		t.Fatalf("%s marker %q contains multiple fenced blocks", document.name, marker)
	}
	var options option.Options
	if err := options.UnmarshalJSONContext(
		include.Context(context.Background()),
		[]byte(matches[1]),
	); err != nil {
		t.Fatalf("%s marker %q does not decode: %v", document.name, marker, err)
	}
	if options.Experimental == nil || options.Experimental.TrafficStatistics == nil {
		t.Fatalf("%s marker %q does not configure traffic statistics", document.name, marker)
	}
	resolved, err := option.ResolveTrafficStatisticsOptions(options.Experimental.TrafficStatistics)
	if err != nil {
		t.Fatalf("%s marker %q does not resolve: %v", document.name, marker, err)
	}
	return resolved
}

func assertTrafficStatisticsExampleDatabase(
	t *testing.T,
	name string,
	dsn string,
	expected string,
) {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("%s DSN does not parse: %v", name, err)
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	if database != expected {
		t.Errorf("%s database = %q, want %q", name, database, expected)
	}
	if database == "traffic" {
		t.Errorf("%s incorrectly requires database traffic", name)
	}
}

func requireTrafficStatisticsDocumentationText(
	t *testing.T,
	document trafficStatisticsDocumentation,
	text string,
) {
	t.Helper()
	normalizedDocument := strings.Join(strings.Fields(document.content), "")
	normalizedText := strings.Join(strings.Fields(text), "")
	if !strings.Contains(normalizedDocument, normalizedText) {
		t.Errorf("%s documentation does not contain %q", document.name, text)
	}
}
