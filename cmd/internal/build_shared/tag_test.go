package build_shared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHighestVersionTag(t *testing.T) {
	t.Parallel()
	require.Equal(t, "v1.14.0-beta.2", highestVersionTag([]string{
		"v1.13.18",
		"traffic-statistics-latest",
		"v1.14.0-alpha.50",
		"v1.14.0-beta.2",
		"v1.14.0-beta.2+mbox.100",
		"v1.14.0-beta.2-mbox.99",
	}))
	require.Equal(t, "v1.14.0", highestVersionTag([]string{
		"v1.14.0-beta.2",
		"v1.14.0-rc.1",
		"v1.14.0",
	}))
	require.Empty(t, highestVersionTag([]string{
		"traffic-statistics-latest",
		"latest",
		"v1.14.0-beta.2+mbox.100",
	}))
}
