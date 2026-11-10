package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics are the service counters and the histogram scraped at /metrics.
type Metrics struct {
	Registry   *prometheus.Registry
	Sent       prometheus.Counter
	Failed     prometheus.Counter
	Duplicate  prometheus.Counter
	Attempts   prometheus.Counter
	Acked      prometheus.Counter
	Nacked     prometheus.Counter
	Processing prometheus.Histogram
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		Sent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_emails_sent_total",
			Help: "Emails accepted by the provider and recorded.",
		}),
		Failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_emails_failed_total",
			Help: "Emails that reached a terminal failure and were dead-lettered or dropped.",
		}),
		Duplicate: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_emails_duplicate_total",
			Help: "Requests skipped because the id was already recorded as sent.",
		}),
		Attempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_send_attempts_total",
			Help: "Provider send attempts, including retries.",
		}),
		Acked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_messages_acked_total",
			Help: "JetStream messages acknowledged after terminal processing.",
		}),
		Nacked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailer_messages_nacked_total",
			Help: "JetStream messages negatively acknowledged so they are redelivered.",
		}),
		Processing: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailer_processing_seconds",
			Help:    "Time spent handling one email request.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.Sent, m.Failed, m.Duplicate, m.Attempts, m.Acked, m.Nacked, m.Processing)
	return m
}

// Handler serves liveness at /healthz and Prometheus metrics at /metrics.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	return mux
}
