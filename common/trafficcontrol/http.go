package trafficcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	historyQueryBodyLimit       = 1 << 20
	historyQueryFilterLimit     = 256
	historyQueryFilterValueSize = 1024
)

type capabilitiesResponse struct {
	APIVersion       string              `json:"api_version"`
	MetricScope      string              `json:"metric_scope"`
	Features         capabilitiesFeature `json:"features"`
	Dimensions       []string            `json:"dimensions"`
	BucketSeconds    int64               `json:"bucket_seconds"`
	RetentionSeconds int64               `json:"retention_seconds"`
}

type capabilitiesFeature struct {
	Summary bool `json:"summary"`
	Series  bool `json:"series"`
	Targets bool `json:"targets"`
}

type queryRequest struct {
	From               string   `json:"from,omitempty"`
	To                 string   `json:"to,omitempty"`
	RouteTags          []string `json:"route_tags,omitempty"`
	ActualOutboundTags []string `json:"actual_outbound_tags,omitempty"`
	Networks           []string `json:"networks,omitempty"`
	Limit              int      `json:"limit,omitempty"`
}

type queryResponse struct {
	ActualFrom string              `json:"actual_from,omitempty"`
	ActualTo   string              `json:"actual_to,omitempty"`
	Totals     queryResponseTotals `json:"totals"`
	Rows       []queryResponseRow  `json:"rows"`
	Truncated  bool                `json:"truncated"`
}

type queryResponseTotals struct {
	UplinkBytes   string `json:"uplink_bytes"`
	DownlinkBytes string `json:"downlink_bytes"`
	Connections   string `json:"connections"`
}

type queryResponseRow struct {
	ConfigRevision     string   `json:"config_revision"`
	RouteTag           string   `json:"route_tag"`
	GroupPath          []string `json:"group_path"`
	ActualOutboundTag  string   `json:"actual_outbound_tag"`
	ActualOutboundType string   `json:"actual_outbound_type"`
	Network            string   `json:"network"`
	UplinkBytes        string   `json:"uplink_bytes"`
	DownlinkBytes      string   `json:"downlink_bytes"`
	Connections        string   `json:"connections"`
}

func NewHistoryHTTPHandler(history *History) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /capabilities", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, capabilitiesResponse{
			APIVersion:  "1",
			MetricScope: "logical_payload",
			Features: capabilitiesFeature{
				Summary: true,
				Series:  false,
				Targets: false,
			},
			Dimensions: []string{
				"config_revision",
				"route_tag",
				"group_path",
				"actual_outbound_tag",
				"actual_outbound_type",
				"network",
			},
			BucketSeconds:    int64(HistoryBucketInterval / time.Second),
			RetentionSeconds: int64(HistoryRetention / time.Second),
		})
	})
	mux.HandleFunc("POST /query", func(writer http.ResponseWriter, request *http.Request) {
		request.Body = http.MaxBytesReader(writer, request.Body, historyQueryBodyLimit)
		var body queryRequest
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		err := decoder.Decode(&body)
		if err != nil && err != io.EOF {
			writeAPIError(writer, http.StatusBadRequest, "invalid request")
			return
		}
		if err == nil {
			var trailing json.RawMessage
			err = decoder.Decode(&trailing)
			if err != io.EOF {
				writeAPIError(writer, http.StatusBadRequest, "invalid request")
				return
			}
		}
		if err = validateQueryRequest(body); err != nil {
			writeAPIError(writer, http.StatusBadRequest, err.Error())
			return
		}
		query := HistoryQuery{
			RouteTags:          body.RouteTags,
			ActualOutboundTags: body.ActualOutboundTags,
			Networks:           body.Networks,
			Limit:              body.Limit,
		}
		if body.From != "" {
			query.From, err = time.Parse(time.RFC3339, body.From)
			if err != nil {
				writeAPIError(writer, http.StatusBadRequest, "invalid from time")
				return
			}
		}
		if body.To != "" {
			query.To, err = time.Parse(time.RFC3339, body.To)
			if err != nil {
				writeAPIError(writer, http.StatusBadRequest, "invalid to time")
				return
			}
		}
		if !query.From.IsZero() && !query.To.IsZero() && !query.From.Before(query.To) {
			writeAPIError(writer, http.StatusBadRequest, "from must be before to")
			return
		}
		result, err := history.Query(request.Context(), query)
		if err != nil {
			if request.Context().Err() != nil ||
				errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return
			}
			writeAPIError(writer, http.StatusInternalServerError, "query failed")
			return
		}
		response := queryResponse{
			Rows:      make([]queryResponseRow, 0, len(result.Rows)),
			Truncated: result.Truncated,
			Totals: queryResponseTotals{
				UplinkBytes:   strconv.FormatUint(result.Totals.UplinkBytes, 10),
				DownlinkBytes: strconv.FormatUint(result.Totals.DownlinkBytes, 10),
				Connections:   strconv.FormatUint(result.Totals.Connections, 10),
			},
		}
		if !result.ActualFrom.IsZero() {
			response.ActualFrom = result.ActualFrom.Format(time.RFC3339)
			response.ActualTo = result.ActualTo.Format(time.RFC3339)
		}
		for _, row := range result.Rows {
			response.Rows = append(response.Rows, queryResponseRow{
				ConfigRevision:     row.ConfigRevision,
				RouteTag:           row.RouteTag,
				GroupPath:          row.GroupPath,
				ActualOutboundTag:  row.ActualOutboundTag,
				ActualOutboundType: row.ActualOutboundType,
				Network:            row.Network,
				UplinkBytes:        strconv.FormatUint(row.UplinkBytes, 10),
				DownlinkBytes:      strconv.FormatUint(row.DownlinkBytes, 10),
				Connections:        strconv.FormatUint(row.Connections, 10),
			})
		}
		writeJSON(writer, http.StatusOK, response)
	})
	return mux
}

func validateQueryRequest(request queryRequest) error {
	if request.Limit < 0 || request.Limit > historyQueryLimitMax {
		return errors.New("limit must be between 0 and 5000")
	}
	for _, filter := range [][]string{
		request.RouteTags,
		request.ActualOutboundTags,
		request.Networks,
	} {
		if len(filter) > historyQueryFilterLimit {
			return errors.New("too many filter values")
		}
		for _, value := range filter {
			if len(value) > historyQueryFilterValueSize {
				return errors.New("filter value is too long")
			}
		}
	}
	for _, network := range request.Networks {
		if network != "tcp" && network != "udp" {
			return errors.New("network must be tcp or udp")
		}
	}
	return nil
}

func writeAPIError(writer http.ResponseWriter, statusCode int, message string) {
	writeJSON(writer, statusCode, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, statusCode int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(value)
}
