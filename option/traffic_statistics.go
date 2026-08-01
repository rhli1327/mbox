package option

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json/badoption"
)

const (
	TrafficStatisticsStorageTypeBolt     = "bolt"
	TrafficStatisticsStorageTypePostgres = "postgres"

	TrafficStatisticsSchemaManagementAuto     = "auto"
	TrafficStatisticsSchemaManagementValidate = "validate"

	TrafficStatisticsStartupPolicyDegraded = "degraded"
	TrafficStatisticsStartupPolicyStrict   = "strict"

	TrafficStatisticsSpoolOverflowDropOldest = "drop_oldest"
)

const (
	defaultTrafficStatisticsBoltPath         = "traffic.db"
	defaultTrafficStatisticsIdentityPath     = "traffic-instance.json"
	defaultTrafficStatisticsSpoolPath        = "traffic-spool.db"
	defaultTrafficStatisticsPostgresSchema   = "public"
	defaultTrafficStatisticsMaxOpen          = 4
	defaultTrafficStatisticsMinIdle          = 1
	defaultTrafficStatisticsConnectTimeout   = 10 * time.Second
	defaultTrafficStatisticsStatementTimeout = 30 * time.Second
	defaultTrafficStatisticsSpoolMaxSize     = 256 * byteformats.MiByte
)

var trafficStatisticsSchemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type TrafficStatisticsOptions struct {
	Enabled       bool                             `json:"enabled,omitempty"`
	Path          string                           `json:"path,omitempty"`
	InstanceID    string                           `json:"instance_id,omitempty"`
	IdentityPath  string                           `json:"identity_path,omitempty"`
	Storage       *TrafficStatisticsStorageOptions `json:"storage,omitempty"`
	Spool         *TrafficStatisticsSpoolOptions   `json:"spool,omitempty"`
	StartupPolicy string                           `json:"startup_policy,omitempty"`
}

type TrafficStatisticsStorageOptions struct {
	Type               string              `json:"type,omitempty"`
	Path               string              `json:"path,omitempty"`
	DSN                string              `json:"dsn,omitempty"`
	Schema             string              `json:"schema,omitempty"`
	SchemaManagement   string              `json:"schema_management,omitempty"`
	Dialer             DialerOptions       `json:"dialer,omitempty"`
	MaxOpenConnections *int                `json:"max_open_connections,omitempty"`
	MinIdleConnections *int                `json:"min_idle_connections,omitempty"`
	ConnectTimeout     *badoption.Duration `json:"connect_timeout,omitempty"`
	StatementTimeout   *badoption.Duration `json:"statement_timeout,omitempty"`
}

type TrafficStatisticsSpoolOptions struct {
	Path     string                   `json:"path,omitempty"`
	MaxSize  *byteformats.MemoryBytes `json:"max_size,omitempty"`
	Overflow string                   `json:"overflow,omitempty"`
}

type ResolvedTrafficStatisticsOptions struct {
	Enabled            bool
	StorageType        string
	Path               string
	DSN                string
	Schema             string
	SchemaManagement   string
	Dialer             DialerOptions
	MaxOpenConnections int
	MinIdleConnections int
	ConnectTimeout     time.Duration
	StatementTimeout   time.Duration
	InstanceID         string
	IdentityPath       string
	SpoolPath          string
	SpoolMaxSize       uint64
	SpoolOverflow      string
	StartupPolicy      string
}

func ResolveTrafficStatisticsOptions(
	options *TrafficStatisticsOptions,
) (ResolvedTrafficStatisticsOptions, error) {
	if options == nil {
		return ResolvedTrafficStatisticsOptions{}, nil
	}
	resolved := ResolvedTrafficStatisticsOptions{Enabled: options.Enabled}
	if options.Storage == nil {
		if options.InstanceID != "" ||
			options.IdentityPath != "" ||
			options.Spool != nil ||
			options.StartupPolicy != "" {
			return ResolvedTrafficStatisticsOptions{},
				fmt.Errorf("traffic statistics Bolt storage does not accept PostgreSQL options")
		}
		resolved.StorageType = TrafficStatisticsStorageTypeBolt
		resolved.Path = options.Path
		if resolved.Path == "" {
			resolved.Path = defaultTrafficStatisticsBoltPath
		}
		return resolved, nil
	}
	if options.Path != "" {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics legacy path and storage are mutually exclusive")
	}

	storage := options.Storage
	if storage.Type == "" {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics storage type is required")
	}
	switch storage.Type {
	case TrafficStatisticsStorageTypeBolt:
		if hasTrafficStatisticsPostgresOptions(options, storage) {
			return ResolvedTrafficStatisticsOptions{},
				fmt.Errorf("traffic statistics Bolt storage does not accept PostgreSQL options")
		}
		resolved.StorageType = TrafficStatisticsStorageTypeBolt
		resolved.Path = storage.Path
		if resolved.Path == "" {
			resolved.Path = defaultTrafficStatisticsBoltPath
		}
		return resolved, nil
	case TrafficStatisticsStorageTypePostgres:
		return resolvePostgresTrafficStatisticsOptions(options, storage)
	default:
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("unsupported traffic statistics storage type: %s", storage.Type)
	}
}

func resolvePostgresTrafficStatisticsOptions(
	options *TrafficStatisticsOptions,
	storage *TrafficStatisticsStorageOptions,
) (ResolvedTrafficStatisticsOptions, error) {
	if storage.Path != "" {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics PostgreSQL storage does not accept a Bolt path")
	}
	if strings.TrimSpace(storage.DSN) == "" {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics PostgreSQL DSN is required")
	}
	if !ValidateTrafficStatisticsInstanceID(options.InstanceID) {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("invalid traffic statistics instance ID")
	}
	if hasUnsupportedTrafficStatisticsDialerOptions(storage.Dialer) {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics PostgreSQL dialer only supports detour")
	}

	resolved := ResolvedTrafficStatisticsOptions{
		Enabled:            options.Enabled,
		StorageType:        TrafficStatisticsStorageTypePostgres,
		DSN:                storage.DSN,
		Schema:             storage.Schema,
		SchemaManagement:   storage.SchemaManagement,
		Dialer:             storage.Dialer,
		MaxOpenConnections: defaultTrafficStatisticsMaxOpen,
		MinIdleConnections: defaultTrafficStatisticsMinIdle,
		ConnectTimeout:     defaultTrafficStatisticsConnectTimeout,
		StatementTimeout:   defaultTrafficStatisticsStatementTimeout,
		InstanceID:         options.InstanceID,
		IdentityPath:       options.IdentityPath,
		SpoolPath:          defaultTrafficStatisticsSpoolPath,
		SpoolMaxSize:       defaultTrafficStatisticsSpoolMaxSize,
		SpoolOverflow:      TrafficStatisticsSpoolOverflowDropOldest,
		StartupPolicy:      options.StartupPolicy,
	}
	if resolved.Schema == "" {
		resolved.Schema = defaultTrafficStatisticsPostgresSchema
	}
	if err := ValidateTrafficStatisticsSchema(resolved.Schema); err != nil {
		return ResolvedTrafficStatisticsOptions{}, err
	}
	if resolved.SchemaManagement == "" {
		resolved.SchemaManagement = TrafficStatisticsSchemaManagementAuto
	}
	switch resolved.SchemaManagement {
	case TrafficStatisticsSchemaManagementAuto, TrafficStatisticsSchemaManagementValidate:
	default:
		return ResolvedTrafficStatisticsOptions{}, fmt.Errorf(
			"unsupported traffic statistics schema management mode: %s",
			resolved.SchemaManagement,
		)
	}
	if storage.MaxOpenConnections != nil {
		resolved.MaxOpenConnections = *storage.MaxOpenConnections
	}
	if resolved.MaxOpenConnections < 1 {
		return ResolvedTrafficStatisticsOptions{},
			fmt.Errorf("traffic statistics max_open_connections must be at least 1")
	}
	if storage.MinIdleConnections != nil {
		resolved.MinIdleConnections = *storage.MinIdleConnections
	}
	if resolved.MinIdleConnections < 0 ||
		resolved.MinIdleConnections > resolved.MaxOpenConnections {
		return ResolvedTrafficStatisticsOptions{}, fmt.Errorf(
			"traffic statistics min_idle_connections must be between 0 and max_open_connections",
		)
	}
	if storage.ConnectTimeout != nil {
		resolved.ConnectTimeout = time.Duration(*storage.ConnectTimeout)
		if resolved.ConnectTimeout <= 0 {
			return ResolvedTrafficStatisticsOptions{},
				fmt.Errorf("traffic statistics connect_timeout must be positive")
		}
	}
	if storage.StatementTimeout != nil {
		resolved.StatementTimeout = time.Duration(*storage.StatementTimeout)
		if resolved.StatementTimeout <= 0 {
			return ResolvedTrafficStatisticsOptions{},
				fmt.Errorf("traffic statistics statement_timeout must be positive")
		}
	}
	if resolved.IdentityPath == "" {
		resolved.IdentityPath = defaultTrafficStatisticsIdentityPath
	}
	if options.Spool != nil {
		if options.Spool.Path != "" {
			resolved.SpoolPath = options.Spool.Path
		}
		if options.Spool.MaxSize != nil {
			resolved.SpoolMaxSize = options.Spool.MaxSize.Value()
			if resolved.SpoolMaxSize == 0 {
				return ResolvedTrafficStatisticsOptions{},
					fmt.Errorf("traffic statistics spool max_size must be positive")
			}
		}
		if options.Spool.Overflow != "" {
			resolved.SpoolOverflow = options.Spool.Overflow
		}
	}
	if resolved.SpoolOverflow != TrafficStatisticsSpoolOverflowDropOldest {
		return ResolvedTrafficStatisticsOptions{}, fmt.Errorf(
			"unsupported traffic statistics spool overflow policy: %s",
			resolved.SpoolOverflow,
		)
	}
	if resolved.StartupPolicy == "" {
		resolved.StartupPolicy = TrafficStatisticsStartupPolicyDegraded
	}
	switch resolved.StartupPolicy {
	case TrafficStatisticsStartupPolicyDegraded, TrafficStatisticsStartupPolicyStrict:
	default:
		return ResolvedTrafficStatisticsOptions{}, fmt.Errorf(
			"unsupported traffic statistics startup policy: %s",
			resolved.StartupPolicy,
		)
	}
	return resolved, nil
}

func ValidateTrafficStatisticsSchema(schema string) error {
	if len(schema) == 0 || len(schema) > 63 ||
		!utf8.ValidString(schema) ||
		!trafficStatisticsSchemaPattern.MatchString(schema) {
		return fmt.Errorf("invalid traffic statistics PostgreSQL schema: %s", schema)
	}
	return nil
}

func ValidateTrafficStatisticsInstanceID(instanceID string) bool {
	if instanceID == "" {
		return true
	}
	if len(instanceID) > 128 || !utf8.ValidString(instanceID) ||
		strings.TrimSpace(instanceID) != instanceID {
		return false
	}
	for _, character := range instanceID {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func hasTrafficStatisticsPostgresOptions(
	options *TrafficStatisticsOptions,
	storage *TrafficStatisticsStorageOptions,
) bool {
	return storage.DSN != "" ||
		storage.Schema != "" ||
		storage.SchemaManagement != "" ||
		!reflect.DeepEqual(storage.Dialer, DialerOptions{}) ||
		storage.MaxOpenConnections != nil ||
		storage.MinIdleConnections != nil ||
		storage.ConnectTimeout != nil ||
		storage.StatementTimeout != nil ||
		options.InstanceID != "" ||
		options.IdentityPath != "" ||
		options.Spool != nil ||
		options.StartupPolicy != ""
}

func hasUnsupportedTrafficStatisticsDialerOptions(dialerOptions DialerOptions) bool {
	dialerOptions.Detour = ""
	return !reflect.DeepEqual(dialerOptions, DialerOptions{})
}
