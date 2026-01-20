package main

import "net/http"

/* ---------------- http + limiter ---------------- */

type limiter struct{ ch chan struct{} }

func newLimiter(n int) *limiter { return &limiter{ch: make(chan struct{}, n)} }
func (l *limiter) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.ch <- struct{}{}:
			defer func() { <-l.ch }()
			next.ServeHTTP(w, r)
		default:
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		}
	})
}
