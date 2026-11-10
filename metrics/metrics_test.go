package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthAndMetrics(t *testing.T) {
	m := New()
	m.Sent.Inc()

	health := httptest.NewRecorder()
	m.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), "ok") {
		t.Fatalf("health = %d %q", health.Code, health.Body.String())
	}

	metrics := httptest.NewRecorder()
	m.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", metrics.Code)
	}
	if !strings.Contains(metrics.Body.String(), "mailer_emails_sent_total") {
		t.Fatalf("metrics body missing sent counter:\n%s", metrics.Body.String())
	}
}
