package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
