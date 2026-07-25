package clashapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/common/trafficcontrol"

	"github.com/go-chi/chi/v5"
)

func TestTrafficHistoryAPIAuthentication(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		history       *trafficcontrol.History
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
			router := chi.NewRouter()
			router.Group(func(router chi.Router) {
				router.Use(authentication(testCase.secret))
				mountTrafficHistoryAPI(router, testCase.history)
			})
			request := httptest.NewRequest(http.MethodGet, "/mbox/v1/traffic/capabilities", nil)
			if testCase.authorization != "" {
				request.Header.Set("Authorization", testCase.authorization)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != testCase.statusCode {
				t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
			}
		})
	}
}
