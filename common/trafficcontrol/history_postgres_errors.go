package trafficcontrol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrPostgresConfiguration = errors.New(
		"traffic statistics PostgreSQL configuration is invalid",
	)
	ErrPostgresAuthentication = errors.New(
		"traffic statistics PostgreSQL authentication failed",
	)
	ErrPostgresDatabaseMissing = errors.New(
		"traffic statistics PostgreSQL database does not exist",
	)
	ErrPostgresPermission = errors.New(
		"traffic statistics PostgreSQL permission denied",
	)
	ErrPostgresSchemaMissing = errors.New(
		"traffic statistics PostgreSQL schema is missing",
	)
	ErrPostgresSchemaIncompatible = errors.New(
		"traffic statistics PostgreSQL schema is incompatible",
	)
	ErrPostgresSchemaFuture = errors.New(
		"traffic statistics PostgreSQL schema version is newer than this binary",
	)
	ErrPostgresTransientNetwork = errors.New(
		"traffic statistics PostgreSQL network operation failed",
	)
	ErrPostgresTimeout = errors.New(
		"traffic statistics PostgreSQL operation timed out",
	)
	ErrPostgresBatchCollision = errors.New(
		"traffic statistics PostgreSQL batch ID has different content",
	)
	ErrPostgresDetourUnavailable = errors.New(
		"traffic statistics PostgreSQL detour outbound is unavailable",
	)
	ErrPostgresDetourNoStream = errors.New(
		"traffic statistics PostgreSQL detour outbound does not support TCP streams",
	)
)

func classifyPostgresError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, ErrPostgresTimeout)
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "28P01", "28000":
			return fmt.Errorf("%s: %w", operation, ErrPostgresAuthentication)
		case "3D000":
			return fmt.Errorf("%s: %w", operation, ErrPostgresDatabaseMissing)
		case "42501":
			return fmt.Errorf("%s: %w", operation, ErrPostgresPermission)
		case "3F000":
			return fmt.Errorf("%s: %w", operation, ErrPostgresSchemaMissing)
		case "57014":
			return fmt.Errorf("%s: %w", operation, ErrPostgresTimeout)
		case "40001", "40P01", "53300", "57P01", "57P02", "57P03":
			return fmt.Errorf("%s: %w", operation, ErrPostgresTransientNetwork)
		default:
			if strings.HasPrefix(postgresError.Code, "08") {
				return fmt.Errorf("%s: %w", operation, ErrPostgresTransientNetwork)
			}
			return fmt.Errorf("%s: PostgreSQL server rejected the operation", operation)
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return fmt.Errorf("%s: %w", operation, ErrPostgresTimeout)
		}
		return fmt.Errorf("%s: %w", operation, ErrPostgresTransientNetwork)
	}
	return fmt.Errorf("%s: PostgreSQL operation failed", operation)
}
