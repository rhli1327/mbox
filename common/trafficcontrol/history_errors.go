package trafficcontrol

import "errors"

var (
	ErrPostgresRequiresDurableSpool = errors.New(
		"traffic statistics PostgreSQL recording requires durable spool support",
	)
	ErrPostgresDetourNotReady = errors.New(
		"traffic statistics PostgreSQL detour support is not ready",
	)
	ErrPostgresIdentityConflict = errors.New(
		"traffic statistics PostgreSQL identity conflicts with existing state",
	)
)
