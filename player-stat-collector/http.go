package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func normalizePathForMetrics(r *http.Request) string {
	p := r.URL.Path
	switch {
	case p == "/log":
		return "/log"
	case strings.HasPrefix(p, "/e/"):
		return "/e/*" // ВАЖНО: не /e/<event>
	case p == "/healthz":
		return "/healthz"
	case p == "/readyz":
		return "/readyz"
	case p == "/debug/wal":
		return "/debug/wal"
	case p == "/debug/domain-cache":
		return "/debug/domain-cache"
	case p == "/debug/country-cache":
		return "/debug/country-cache"
	case strings.HasPrefix(p, "/metrics/"):
		return "/metrics" // скрытый путь
	default:
		return "other"
	}
}

func httpMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}

		defer func() {
			path := normalizePathForMetrics(r)
			code := fmt.Sprintf("%d", sw.status)

			mReqTotal.WithLabelValues(path, code).Inc()
			mReqDur.WithLabelValues(path).Observe(time.Since(start).Seconds())
		}()

		next.ServeHTTP(sw, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
