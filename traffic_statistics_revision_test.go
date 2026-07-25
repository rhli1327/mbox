package box

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

func TestTrafficStatisticsConfigRevisionIncludesOutboundOptions(t *testing.T) {
	base := trafficStatisticsRevisionTestOptions([]string{"node-a", "node-b"}, 1080)
	baseContent := marshalTrafficStatisticsConfigForTest(t, base)
	if !bytes.Contains(baseContent, []byte(`"outbounds":["node-a","node-b"]`)) {
		t.Fatalf("serialized configuration omitted selector members: %s", baseContent)
	}
	baseRevision := sha256.Sum256(baseContent)

	sameRevision := sha256.Sum256(marshalTrafficStatisticsConfigForTest(t, base))
	if baseRevision != sameRevision {
		t.Fatal("identical outbound configuration produced a different revision input")
	}

	changedMembers := trafficStatisticsRevisionTestOptions([]string{"node-a", "node-c"}, 1080)
	changedMembersRevision := sha256.Sum256(marshalTrafficStatisticsConfigForTest(t, changedMembers))
	if baseRevision == changedMembersRevision {
		t.Fatal("changing selector members did not change the configuration revision")
	}

	changedServer := trafficStatisticsRevisionTestOptions([]string{"node-a", "node-b"}, 2080)
	changedServerRevision := sha256.Sum256(marshalTrafficStatisticsConfigForTest(t, changedServer))
	if baseRevision == changedServerRevision {
		t.Fatal("changing an outbound option did not change the configuration revision")
	}
}

func marshalTrafficStatisticsConfigForTest(t *testing.T, options option.Options) []byte {
	t.Helper()
	content, err := marshalTrafficStatisticsConfig(context.Background(), options)
	if err != nil {
		t.Fatal("marshal traffic statistics configuration:", err)
	}
	return content
}

func trafficStatisticsRevisionTestOptions(groupMembers []string, serverPort uint16) option.Options {
	return option.Options{
		Outbounds: []option.Outbound{
			{
				Type: C.TypeSelector,
				Tag:  "Proxy",
				Options: &option.SelectorOutboundOptions{
					Outbounds: append([]string(nil), groupMembers...),
				},
			},
			{
				Type: C.TypeSOCKS,
				Tag:  "node-a",
				Options: &option.SOCKSOutboundOptions{
					ServerOptions: option.ServerOptions{
						Server:     "127.0.0.1",
						ServerPort: serverPort,
					},
					Version: "5",
				},
			},
		},
	}
}
