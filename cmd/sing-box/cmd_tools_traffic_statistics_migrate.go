package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"

	"github.com/spf13/cobra"
)

type trafficStatisticsMigrateCLIOptions struct {
	Source               string
	InstanceID           string
	BatchSize            int
	From                 string
	To                   string
	Resume               string
	ActiveConfigRevision string
	DryRun               bool
}

var commandToolsTrafficStatisticsMigrate *cobra.Command

var migrateBoltTrafficStatisticsToPostgres = trafficcontrol.MigrateBoltTrafficStatisticsToPostgres

func init() {
	commandToolsTrafficStatisticsMigrate = newTrafficStatisticsMigrateCommand()
	commandToolsTrafficStatistics.AddCommand(commandToolsTrafficStatisticsMigrate)
}

func newTrafficStatisticsMigrateCommand() *cobra.Command {
	cliOptions := trafficStatisticsMigrateCLIOptions{}
	command := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate Bolt traffic statistics to PostgreSQL",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runTrafficStatisticsMigrate(globalCtx, command, cliOptions)
		},
	}
	command.Flags().StringVar(
		&cliOptions.Source,
		"source",
		"traffic.db",
		"Source Bolt traffic statistics database",
	)
	command.Flags().StringVar(
		&cliOptions.InstanceID,
		"instance-id",
		"",
		"Target traffic statistics instance ID",
	)
	command.Flags().IntVar(
		&cliOptions.BatchSize,
		"batch-size",
		500,
		"Number of source records per transaction",
	)
	command.Flags().StringVar(
		&cliOptions.From,
		"from",
		"",
		"Inclusive RFC3339 minute boundary",
	)
	command.Flags().StringVar(
		&cliOptions.To,
		"to",
		"",
		"Exclusive RFC3339 minute boundary",
	)
	command.Flags().StringVar(
		&cliOptions.Resume,
		"resume",
		"",
		"Expected deterministic migration ID",
	)
	command.Flags().StringVar(
		&cliOptions.ActiveConfigRevision,
		"active-config-revision",
		"",
		"Active historical config revision",
	)
	command.Flags().BoolVar(
		&cliOptions.DryRun,
		"dry-run",
		false,
		"Validate and compare without persistent writes",
	)
	return command
}

func runTrafficStatisticsMigrate(
	ctx context.Context,
	command *cobra.Command,
	cliOptions trafficStatisticsMigrateCLIOptions,
) error {
	if commandToolsFlagOutbound != "" ||
		command.Flags().Changed("outbound") ||
		command.InheritedFlags().Changed("outbound") {
		return errors.New("traffic statistics migration does not accept --outbound")
	}
	from, err := parseTrafficStatisticsMigrationBound(cliOptions.From)
	if err != nil {
		return err
	}
	to, err := parseTrafficStatisticsMigrationBound(cliOptions.To)
	if err != nil {
		return err
	}
	if cliOptions.BatchSize < 1 || cliOptions.BatchSize > 10000 {
		return errors.New("traffic statistics migration --batch-size must be between 1 and 10000")
	}
	if cliOptions.Resume != "" &&
		!trafficStatisticsMigrationLowerHex(cliOptions.Resume, 64) {
		return errors.New("traffic statistics migration --resume must contain 64 lowercase hexadecimal characters")
	}
	if cliOptions.ActiveConfigRevision != "" &&
		!trafficStatisticsMigrationLowerHex(cliOptions.ActiveConfigRevision, 32) {
		return errors.New("traffic statistics migration --active-config-revision must contain 32 lowercase hexadecimal characters")
	}
	if !from.IsZero() && !to.IsZero() && !from.Before(to) {
		return errors.New("traffic statistics migration --from must be before --to")
	}
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
		return errors.New("traffic statistics migration requires PostgreSQL storage")
	}

	if ctx == nil {
		ctx = context.Background()
	}
	joinedCtx, cancel := context.WithCancel(ctx)
	if command.Context() != nil {
		stopCommand := context.AfterFunc(command.Context(), cancel)
		defer stopCommand()
	}
	signalCtx, stopSignals := signal.NotifyContext(
		joinedCtx,
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()
	defer cancel()

	migrationCtx := signalCtx
	logger := log.StdLogger()
	if resolved.Dialer.Detour != "" {
		runtimeParent := service.ExtendContext(signalCtx)
		startRuntime := func() (
			context.Context,
			log.ContextLogger,
			func() error,
			error,
		) {
			runtime, startErr := newTrafficStatisticsMigrationRuntime(
				runtimeParent,
				options,
				command.ErrOrStderr(),
			)
			if startErr != nil {
				return nil, nil, nil, startErr
			}
			if startErr = runtime.Start(); startErr != nil {
				_ = runtime.Close()
				return nil, nil, nil, startErr
			}
			return runtime.Context(), runtime.Logger(), runtime.Close, nil
		}
		migrationCtx = service.ContextWith(
			runtimeParent,
			startRuntime,
		)
	}
	progressEncoder := json.NewEncoder(command.ErrOrStderr())
	result, err := migrateBoltTrafficStatisticsToPostgres(
		migrationCtx,
		logger,
		trafficcontrol.TrafficStatisticsMigrationOptions{
			SourcePath:           cliOptions.Source,
			Configuration:        options,
			Traffic:              resolved,
			InstanceID:           cliOptions.InstanceID,
			BatchSize:            cliOptions.BatchSize,
			From:                 from,
			To:                   to,
			ResumeMigrationID:    cliOptions.Resume,
			ActiveConfigRevision: cliOptions.ActiveConfigRevision,
			DryRun:               cliOptions.DryRun,
			Progress: func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
				return progressEncoder.Encode(event)
			},
		},
	)
	if err != nil {
		return err
	}
	return json.NewEncoder(command.OutOrStdout()).Encode(result)
}

func parseTrafficStatisticsMigrationBound(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil ||
		parsed.Second() != 0 ||
		parsed.Nanosecond() != 0 {
		return time.Time{}, errors.New(
			"traffic statistics migration bounds must be minute-aligned RFC3339",
		)
	}
	return parsed.UTC(), nil
}

func trafficStatisticsMigrationLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}
