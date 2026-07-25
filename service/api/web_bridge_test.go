package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/common/trafficcontrol"
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
			request := httptest.NewRequest(http.MethodGet, "/mbox/v1/traffic/capabilities", nil)
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

	request := httptest.NewRequest(http.MethodGet, "/mbox/v1/traffic/capabilities", nil)
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

	request := httptest.NewRequest(http.MethodGet, "/mbox/v1/traffic/capabilities", nil)
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
			bridge := &webBridge{
				trafficHandler: newTrafficHistoryHTTPHandler(testCase.secret, testCase.history),
			}
			request := httptest.NewRequest(http.MethodGet, "/mbox/v1/traffic/capabilities", nil)
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
