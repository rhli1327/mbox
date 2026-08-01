package api

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"
)

func TestAuthenticateHTTPBearerSecret(t *testing.T) {
	var calls int
	handler := authenticateHTTP("traffic-secret", http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls++
		writer.WriteHeader(http.StatusNoContent)
	}))

	for name, authorization := range map[string]string{
		"missing":   "",
		"wrong":     "Bearer wrong-secret",
		"malformed": "Basic traffic-secret",
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/mbox/v2/traffic/capabilities", nil)
			if authorization != "" {
				request.Header.Set("Authorization", authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
			}
		})
	}
	if calls != 0 {
		t.Fatalf("unauthorized requests reached the handler %d times", calls)
	}

	request := httptest.NewRequest(http.MethodGet, "/mbox/v2/traffic/capabilities", nil)
	request.Header.Set("Authorization", "Bearer traffic-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("unexpected authorized response status %d", response.Code)
	}
	if calls != 1 {
		t.Fatalf("authorized request reached the handler %d times", calls)
	}
}

func TestAuthenticateHTTPWithoutSecret(t *testing.T) {
	var calls int
	handler := authenticateHTTP("", http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls++
		writer.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/mbox/v2/traffic/capabilities", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	if calls != 1 {
		t.Fatalf("request reached the handler %d times", calls)
	}
}

func TestTrafficHistoryHTTPHandlerAuthentication(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		history       trafficcontrol.HistoryReader
		secret        string
		authorization string
		statusCode    int
	}{
		{
			name:       "history disabled",
			statusCode: http.StatusNotFound,
		},
		{
			name:       "empty secret",
			history:    new(trafficcontrol.History),
			statusCode: http.StatusOK,
		},
		{
			name:       "missing bearer",
			history:    new(trafficcontrol.History),
			secret:     "traffic-secret",
			statusCode: http.StatusUnauthorized,
		},
		{
			name:          "wrong bearer",
			history:       new(trafficcontrol.History),
			secret:        "traffic-secret",
			authorization: "Bearer wrong-secret",
			statusCode:    http.StatusUnauthorized,
		},
		{
			name:          "valid bearer",
			history:       new(trafficcontrol.History),
			secret:        "traffic-secret",
			authorization: "Bearer traffic-secret",
			statusCode:    http.StatusOK,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			bridge := &webBridge{
				trafficHandler: newTrafficHistoryHTTPHandler(testCase.secret, testCase.history),
			}
			request := httptest.NewRequest(http.MethodGet, "/mbox/v2/traffic/capabilities", nil)
			if testCase.authorization != "" {
				request.Header.Set("Authorization", testCase.authorization)
			}
			response := httptest.NewRecorder()
			bridge.ServeHTTP(response, request)
			if response.Code != testCase.statusCode {
				t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestTrafficHistoryHTTPMountContract(t *testing.T) {
	history, availableFrom := newAPIServiceTestHistory(t)
	bridge := &webBridge{
		trafficHandler: newTrafficHistoryHTTPHandler("traffic-secret", history),
	}

	unauthorized := httptest.NewRecorder()
	bridge.ServeHTTP(
		unauthorized,
		httptest.NewRequest(http.MethodGet, "/mbox/v2/traffic/capabilities", nil),
	)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected unauthorized status %d: %s", unauthorized.Code, unauthorized.Body.String())
	}
	var unauthorizedBody map[string]json.RawMessage
	if err := json.Unmarshal(unauthorized.Body.Bytes(), &unauthorizedBody); err != nil {
		t.Fatal("decode unauthorized response:", err)
	}
	assertAPIServiceJSONString(t, unauthorizedBody, "error", "unauthorized")

	capabilities := performAPIServiceTrafficRequest(
		t,
		bridge,
		http.MethodGet,
		"/mbox/v2/traffic/capabilities",
		"",
	)
	var capabilitiesBody struct {
		APIVersion               string          `json:"api_version"`
		MetricScope              string          `json:"metric_scope"`
		Features                 map[string]bool `json:"features"`
		Dimensions               []string        `json:"dimensions"`
		Groupings                []string        `json:"groupings"`
		MaxPageSize              int             `json:"max_page_size"`
		BucketSeconds            int64           `json:"bucket_seconds"`
		RetentionSeconds         int64           `json:"retention_seconds"`
		TargetAvailableFrom      string          `json:"target_available_from"`
		DestinationAvailableFrom string          `json:"destination_available_from"`
	}
	if err := json.Unmarshal(capabilities.Body.Bytes(), &capabilitiesBody); err != nil {
		t.Fatal("decode capabilities response:", err)
	}
	if capabilitiesBody.APIVersion != "2" ||
		capabilitiesBody.MetricScope != "logical_payload" ||
		!reflect.DeepEqual(capabilitiesBody.Features, map[string]bool{
			"summary":    true,
			"series":     false,
			"targets":    true,
			"pagination": true,
			"sorting":    true,
			"filtering":  true,
		}) ||
		capabilitiesBody.MaxPageSize != trafficcontrol.HistoryPageSizeMax ||
		capabilitiesBody.BucketSeconds != int64(trafficcontrol.HistoryBucketInterval/time.Second) ||
		capabilitiesBody.RetentionSeconds != int64(trafficcontrol.HistoryRetention/time.Second) ||
		capabilitiesBody.TargetAvailableFrom != availableFrom.Format(time.RFC3339) ||
		capabilitiesBody.DestinationAvailableFrom != availableFrom.Format(time.RFC3339) {
		t.Fatalf("unexpected capabilities response: %#v", capabilitiesBody)
	}
	if !reflect.DeepEqual(capabilitiesBody.Groupings, []string{
		trafficcontrol.HistoryGroupByRoutePath,
		trafficcontrol.HistoryGroupByDestination,
		trafficcontrol.HistoryGroupByDestinationDomain,
		trafficcontrol.HistoryGroupByOutboundGroup,
		trafficcontrol.HistoryGroupByActualOutbound,
	}) {
		t.Fatalf("unexpected capabilities groupings: %#v", capabilitiesBody.Groupings)
	}
	if len(capabilitiesBody.Dimensions) != 10 {
		t.Fatalf("unexpected capabilities dimensions: %#v", capabilitiesBody.Dimensions)
	}

	query := performAPIServiceTrafficRequest(
		t,
		bridge,
		http.MethodPost,
		"/mbox/v2/traffic/query",
		`{"route_tags":["Proxy"],"group_tags":["Auto"],"actual_outbound_tags":["node-a"],"networks":["tcp"],"page":1,"page_size":1}`,
	)
	assertAPIServiceMountedQuery(t, query.Body.Bytes(), false)

	targetQuery := performAPIServiceTrafficRequest(
		t,
		bridge,
		http.MethodPost,
		"/mbox/v2/traffic/query",
		`{"group_by":"destination","destinations":["api.example"]}`,
	)
	assertAPIServiceMountedQuery(t, targetQuery.Body.Bytes(), true)

	invalid := performAPIServiceTrafficRequest(
		t,
		bridge,
		http.MethodPost,
		"/mbox/v2/traffic/query",
		`{"page":0}`,
	)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("unexpected invalid-query status %d: %s", invalid.Code, invalid.Body.String())
	}
	var invalidBody map[string]json.RawMessage
	if err := json.Unmarshal(invalid.Body.Bytes(), &invalidBody); err != nil {
		t.Fatal("decode invalid-query response:", err)
	}
	assertAPIServiceJSONString(t, invalidBody, "error", "page must be at least 1")
}

func newAPIServiceTestHistory(t *testing.T) (*trafficcontrol.History, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "traffic.db")
	availableFrom := time.Now().UTC().Truncate(trafficcontrol.HistoryBucketInterval)
	seedAPIServiceTestAvailability(t, path, availableFrom)
	history := trafficcontrol.NewHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("traffic-test"),
		trafficcontrol.HistoryOptions{
			Path:          path,
			ConfigContent: []byte("mounted-api-contract"),
		},
	)
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("open traffic history:", err)
	}
	t.Cleanup(func() {
		if err := history.Close(); err != nil {
			t.Error("close traffic history:", err)
		}
	})
	trace := trafficcontrol.NewRouteTrace("Proxy", "selector", true)
	trace.RecordSelection("Proxy", "Auto", "urltest", true)
	trace.RecordSelection("Auto", "node-a", "vmess", false)
	history.RecordDelta(&trafficcontrol.TrackerMetadata{
		Metadata:          adapter.InboundContext{Network: N.NetworkTCP},
		Trace:             trace,
		DestinationDomain: "api.example",
	}, 9_007_199_254_740_993, 9_007_199_254_740_995, true)
	return history, availableFrom
}

func seedAPIServiceTestAvailability(t *testing.T, path string, availableFrom time.Time) {
	t.Helper()
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal("create traffic database:", err)
	}
	err = database.Update(func(transaction *bbolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists([]byte("traffic_statistics_metadata_v1"))
		if err != nil {
			return err
		}
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(availableFrom.Unix()))
		if err = bucket.Put([]byte("target_available_from"), value); err != nil {
			return err
		}
		return bucket.Put([]byte("destination_available_from"), value)
	})
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("seed traffic database: update=%v close=%v", err, closeErr)
	}
}

func performAPIServiceTrafficRequest(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer traffic-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK && path != "/mbox/v2/traffic/query" {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	return response
}

func assertAPIServiceMountedQuery(t *testing.T, content []byte, target bool) {
	t.Helper()
	var body struct {
		GroupBy   string                       `json:"group_by"`
		Page      int                          `json:"page"`
		PageSize  int                          `json:"page_size"`
		TotalRows int                          `json:"total_rows"`
		Totals    map[string]json.RawMessage   `json:"totals"`
		Rows      []map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(content, &body); err != nil {
		t.Fatal("decode mounted query response:", err)
	}
	if body.Page != 1 || body.TotalRows != 1 || len(body.Rows) != 1 {
		t.Fatalf("unexpected mounted query metadata: %#v", body)
	}
	if target {
		if body.GroupBy != trafficcontrol.HistoryGroupByDestination ||
			body.PageSize != trafficcontrol.HistoryPageSizeDefault {
			t.Fatalf("unexpected mounted target query metadata: %#v", body)
		}
		assertAPIServiceJSONString(t, body.Rows[0], "destination", "api.example")
		assertAPIServiceJSONString(t, body.Rows[0], "destination_type", trafficcontrol.HistoryDestinationTypeDomain)
		assertAPIServiceJSONString(t, body.Rows[0], "destination_domain", "api.example")
	} else {
		if body.GroupBy != trafficcontrol.HistoryGroupByRoutePath || body.PageSize != 1 {
			t.Fatalf("unexpected mounted route query metadata: %#v", body)
		}
		assertAPIServiceJSONString(t, body.Rows[0], "route_tag", "Proxy")
		assertAPIServiceStringArray(t, body.Rows[0], "group_path", []string{"Proxy", "Auto"})
		assertAPIServiceJSONString(t, body.Rows[0], "outbound_group", "Auto")
		assertAPIServiceJSONString(t, body.Rows[0], "actual_outbound_tag", "node-a")
		assertAPIServiceJSONString(t, body.Rows[0], "actual_outbound_type", "vmess")
		assertAPIServiceJSONString(t, body.Rows[0], "network", N.NetworkTCP)
		assertAPIServiceNonEmptyJSONString(t, body.Rows[0], "config_revision")
	}
	for _, fields := range []map[string]json.RawMessage{body.Totals, body.Rows[0]} {
		assertAPIServiceJSONString(t, fields, "uplink_bytes", "9007199254740993")
		assertAPIServiceJSONString(t, fields, "downlink_bytes", "9007199254740995")
		assertAPIServiceJSONString(t, fields, "connections", "1")
	}
}

func assertAPIServiceJSONString(t *testing.T, object map[string]json.RawMessage, key string, expected string) {
	t.Helper()
	content, loaded := object[key]
	if !loaded {
		t.Fatalf("missing JSON field %q in %#v", key, object)
	}
	var actual string
	if err := json.Unmarshal(content, &actual); err != nil {
		t.Fatalf("field %q is not a JSON string: %s", key, content)
	}
	if actual != expected {
		t.Fatalf("unexpected JSON field %q: got %q, want %q", key, actual, expected)
	}
}

func assertAPIServiceNonEmptyJSONString(t *testing.T, object map[string]json.RawMessage, key string) {
	t.Helper()
	content, loaded := object[key]
	if !loaded {
		t.Fatalf("missing JSON field %q in %#v", key, object)
	}
	var actual string
	if err := json.Unmarshal(content, &actual); err != nil {
		t.Fatalf("field %q is not a JSON string: %s", key, content)
	}
	if actual == "" {
		t.Fatalf("JSON field %q is empty", key)
	}
}

func assertAPIServiceStringArray(t *testing.T, object map[string]json.RawMessage, key string, expected []string) {
	t.Helper()
	content, loaded := object[key]
	if !loaded {
		t.Fatalf("missing JSON field %q in %#v", key, object)
	}
	var actual []string
	if err := json.Unmarshal(content, &actual); err != nil {
		t.Fatalf("field %q is not a JSON string array: %s", key, content)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected JSON field %q: got %#v, want %#v", key, actual, expected)
	}
}
