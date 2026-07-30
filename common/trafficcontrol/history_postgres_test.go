//go:build with_postgres

package trafficcontrol

import (
	"fmt"
	"os"
	"testing"
)

var (
	testPostgresAdminDSN    string
	testPostgresSchemaDSN   string
	testPostgresDSN         string
	testPostgresValidateDSN string
	testPostgresTLSDSN      string
	testPostgresRootCert    string
)

func TestMain(m *testing.M) {
	required := map[string]*string{
		"MBOX_TEST_POSTGRES_ADMIN_DSN":    &testPostgresAdminDSN,
		"MBOX_TEST_POSTGRES_SCHEMA_DSN":   &testPostgresSchemaDSN,
		"MBOX_TEST_POSTGRES_DSN":          &testPostgresDSN,
		"MBOX_TEST_POSTGRES_VALIDATE_DSN": &testPostgresValidateDSN,
		"MBOX_TEST_POSTGRES_TLS_DSN":      &testPostgresTLSDSN,
		"MBOX_TEST_POSTGRES_SSLROOTCERT":  &testPostgresRootCert,
	}
	for name, target := range required {
		*target = os.Getenv(name)
		if *target == "" {
			_, _ = fmt.Fprintf(
				os.Stderr,
				"with_postgres requires %s\n",
				name,
			)
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}
