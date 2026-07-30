package main

import (
	"context"
	"errors"

	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/spf13/cobra"
)

var commandToolsTrafficStatistics = &cobra.Command{
	Use:   "traffic-statistics",
	Short: "Manage traffic statistics",
}

var commandToolsTrafficStatisticsSchema = &cobra.Command{
	Use:   "schema",
	Short: "Manage the traffic statistics database schema",
}

var commandToolsTrafficStatisticsSchemaMigrate = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate the configured PostgreSQL schema",
	Args:  cobra.NoArgs,
	RunE: func(*cobra.Command, []string) error {
		return runTrafficStatisticsSchemaMigrate(globalCtx)
	},
}

var migrateTrafficStatisticsSchema = trafficcontrol.MigratePostgresSchema

func init() {
	commandTools.AddCommand(commandToolsTrafficStatistics)
	commandToolsTrafficStatistics.AddCommand(commandToolsTrafficStatisticsSchema)
	commandToolsTrafficStatisticsSchema.AddCommand(
		commandToolsTrafficStatisticsSchemaMigrate,
	)
}

func runTrafficStatisticsSchemaMigrate(ctx context.Context) error {
	options, err := readConfigAndMerge()
	if err != nil {
		return err
	}
	if options.Experimental == nil ||
		options.Experimental.TrafficStatistics == nil {
		return errors.New("traffic statistics are not configured")
	}
	resolved, err := option.ResolveTrafficStatisticsOptions(
		options.Experimental.TrafficStatistics,
	)
	if err != nil {
		return err
	}
	if !resolved.Enabled {
		return errors.New("traffic statistics are not enabled")
	}
	if resolved.StorageType != option.TrafficStatisticsStorageTypePostgres {
		return errors.New(
			"traffic statistics schema migration requires PostgreSQL storage",
		)
	}
	return migrateTrafficStatisticsSchema(
		ctx,
		log.StdLogger(),
		trafficcontrol.PostgresSchemaOptions{
			DSN:                resolved.DSN,
			Schema:             resolved.Schema,
			Dialer:             resolved.Dialer,
			MaxOpenConnections: resolved.MaxOpenConnections,
			MinIdleConnections: resolved.MinIdleConnections,
			ConnectTimeout:     resolved.ConnectTimeout,
			StatementTimeout:   resolved.StatementTimeout,
		},
	)
}
