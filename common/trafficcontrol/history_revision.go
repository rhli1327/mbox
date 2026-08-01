package trafficcontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

type postgresRevisionInput struct {
	CanonicalContent   []byte
	RoutingFingerprint [sha256.Size]byte
	ConfigRevision     string
}

func buildPostgresRevisionInput(
	ctx context.Context,
	options option.Options,
	revisionKey []byte,
) (postgresRevisionInput, error) {
	if len(revisionKey) != sha256.Size {
		return postgresRevisionInput{}, fmt.Errorf(
			"traffic statistics revision key must contain 32 bytes",
		)
	}
	canonicalOptions := options
	if options.Experimental != nil {
		experimentalOptions := *options.Experimental
		experimentalOptions.TrafficStatistics = nil
		canonicalOptions.Experimental = &experimentalOptions
	}
	canonicalContent, err := json.MarshalContext(ctx, canonicalOptions)
	if err != nil {
		return postgresRevisionInput{}, fmt.Errorf(
			"marshal traffic attribution configuration: %w",
			err,
		)
	}
	fingerprint := sha256.Sum256(canonicalContent)
	revisionMAC := hmac.New(sha256.New, revisionKey)
	_, _ = revisionMAC.Write(canonicalContent)
	revision := revisionMAC.Sum(nil)
	return postgresRevisionInput{
		CanonicalContent:   canonicalContent,
		RoutingFingerprint: fingerprint,
		ConfigRevision:     hex.EncodeToString(revision[:historyRevisionSize]),
	}, nil
}
