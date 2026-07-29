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
	historyQuerySearchSize      = 1024
)

type capabilitiesResponse struct {
	APIVersion               string              `json:"api_version"`
	MetricScope              string              `json:"metric_scope"`
	Features                 capabilitiesFeature `json:"features"`
	Dimensions               []string            `json:"dimensions"`
	Groupings                []string            `json:"groupings"`
	SortFields               []string            `json:"sort_fields"`
	MaxPageSize              int                 `json:"max_page_size"`
	BucketSeconds            int64               `json:"bucket_seconds"`
	RetentionSeconds         int64               `json:"retention_seconds"`
	TargetAvailableFrom      string              `json:"target_available_from"`
	DestinationAvailableFrom string              `json:"destination_available_from"`
}

type capabilitiesFeature struct {
	Summary    bool `json:"summary"`
	Series     bool `json:"series"`
	Targets    bool `json:"targets"`
	Pagination bool `json:"pagination"`
	Sorting    bool `json:"sorting"`
	Filtering  bool `json:"filtering"`
}

type queryRequest struct {
	From               string   `json:"from,omitempty"`
	To                 string   `json:"to,omitempty"`
	RouteTags          []string `json:"route_tags,omitempty"`
	GroupTags          []string `json:"group_tags,omitempty"`
	ActualOutboundTags []string `json:"actual_outbound_tags,omitempty"`
	Destinations       []string `json:"destinations,omitempty"`
	DestinationDomains []string `json:"destination_domains,omitempty"`
	Networks           []string `json:"networks,omitempty"`
	GroupBy            string   `json:"group_by,omitempty"`
	Page               *int     `json:"page,omitempty"`
	PageSize           *int     `json:"page_size,omitempty"`
	SortBy             string   `json:"sort_by,omitempty"`
	SortOrder          string   `json:"sort_order,omitempty"`
	Search             string   `json:"search,omitempty"`
}

type queryResponse struct {
	GroupBy                  string              `json:"group_by"`
	Page                     int                 `json:"page"`
	PageSize                 int                 `json:"page_size"`
	TotalRows                int                 `json:"total_rows"`
	TargetAvailableFrom      string              `json:"target_available_from"`
	DestinationAvailableFrom string              `json:"destination_available_from"`
	ActualFrom               string              `json:"actual_from"`
	ActualTo                 string              `json:"actual_to"`
	Totals                   queryResponseTotals `json:"totals"`
	Rows                     []queryResponseRow  `json:"rows"`
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
	Destination        string   `json:"destination"`
	DestinationType    string   `json:"destination_type"`
	DestinationDomain  string   `json:"destination_domain"`
	OutboundGroup      string   `json:"outbound_group"`
	ActualOutboundTag  string   `json:"actual_outbound_tag"`
	ActualOutboundType string   `json:"actual_outbound_type"`
	Network            string   `json:"network"`
	UplinkBytes        string   `json:"uplink_bytes"`
	DownlinkBytes      string   `json:"downlink_bytes"`
	Connections        string   `json:"connections"`
}

func NewHistoryHTTPHandler(history HistoryReader) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /capabilities", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, capabilitiesResponse{
			APIVersion:  "2",
			MetricScope: "logical_payload",
			Features: capabilitiesFeature{
				Summary:    true,
				Series:     false,
				Targets:    true,
				Pagination: true,
				Sorting:    true,
				Filtering:  true,
			},
			Dimensions: []string{
				"config_revision",
				"route_tag",
				"group_path",
				"destination",
				"destination_type",
				"destination_domain",
				"outbound_group",
				"actual_outbound_tag",
				"actual_outbound_type",
				"network",
			},
			Groupings: []string{
				HistoryGroupByRoutePath,
				HistoryGroupByDestination,
				HistoryGroupByDestinationDomain,
				HistoryGroupByOutboundGroup,
				HistoryGroupByActualOutbound,
			},
			SortFields: []string{
				HistorySortByName,
				HistorySortByTotalBytes,
				HistorySortByUplinkBytes,
				HistorySortByDownlinkBytes,
				HistorySortByConnections,
			},
			MaxPageSize:              HistoryPageSizeMax,
			BucketSeconds:            int64(HistoryBucketInterval / time.Second),
			RetentionSeconds:         int64(HistoryRetention / time.Second),
			TargetAvailableFrom:      formatOptionalTime(history.TargetAvailableFrom()),
			DestinationAvailableFrom: formatOptionalTime(history.DestinationAvailableFrom()),
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
			GroupTags:          body.GroupTags,
			ActualOutboundTags: body.ActualOutboundTags,
			Destinations:       body.Destinations,
			DestinationDomains: body.DestinationDomains,
			Networks:           body.Networks,
			GroupBy:            body.GroupBy,
			SortBy:             body.SortBy,
			SortOrder:          body.SortOrder,
			Search:             body.Search,
		}
		if body.Page != nil {
			query.Page = *body.Page
		}
		if body.PageSize != nil {
			query.PageSize = *body.PageSize
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
		query, err = normalizeHistoryQuery(query)
		if err != nil {
			writeAPIError(writer, http.StatusBadRequest, err.Error())
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
			GroupBy:                  result.GroupBy,
			Page:                     result.Page,
			PageSize:                 result.PageSize,
			TotalRows:                result.TotalRows,
			TargetAvailableFrom:      formatOptionalTime(result.TargetAvailableFrom),
			DestinationAvailableFrom: formatOptionalTime(result.DestinationAvailableFrom),
			Rows:                     make([]queryResponseRow, 0, len(result.Rows)),
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
				Destination:        row.Destination,
				DestinationType:    row.DestinationType,
				DestinationDomain:  row.DestinationDomain,
				OutboundGroup:      row.OutboundGroup,
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

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339)
}

func validateQueryRequest(request queryRequest) error {
	if request.Page != nil && *request.Page < 1 {
		return errors.New("page must be at least 1")
	}
	if request.PageSize != nil && (*request.PageSize < 1 || *request.PageSize > HistoryPageSizeMax) {
		return errors.New("page_size must be between 1 and 200")
	}
	if len(request.Search) > historyQuerySearchSize {
		return errors.New("search is too long")
	}
	for _, filter := range [][]string{
		request.RouteTags,
		request.GroupTags,
		request.ActualOutboundTags,
		request.Destinations,
		request.DestinationDomains,
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
	_, err := normalizeHistoryQuery(HistoryQuery{
		GroupBy:            request.GroupBy,
		Page:               valueOrZero(request.Page),
		PageSize:           valueOrZero(request.PageSize),
		SortBy:             request.SortBy,
		SortOrder:          request.SortOrder,
		Networks:           request.Networks,
		Destinations:       request.Destinations,
		DestinationDomains: request.DestinationDomains,
	})
	return err
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func writeAPIError(writer http.ResponseWriter, statusCode int, message string) {
	writeJSON(writer, statusCode, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, statusCode int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(value)
}
