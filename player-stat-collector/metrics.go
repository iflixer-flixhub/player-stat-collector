package main

import "github.com/prometheus/client_golang/prometheus"

/* ---------------- metrics ---------------- */

var (
	mReqTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "ingest_http_requests_total", Help: "Total HTTP requests"},
		[]string{"path", "code"},
	)
	mReqDur = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "ingest_http_request_duration_seconds", Help: "Request latency (s)"},
		[]string{"path"},
	)

	mEnqueued = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_events_enqueued_total", Help: "Events enqueued"})
	mDropped  = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_events_dropped_total", Help: "Events dropped (queue full/bad/wal error)"})
	mFlushed  = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_events_flushed_total", Help: "Events flushed to MySQL"})
	mFlushErr = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_flush_errors_total", Help: "Flush errors"})

	mQueueLen = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ingest_queue_length", Help: "In-memory queue length"})
	mBufLen   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ingest_batch_buffer_length", Help: "Current batch buffer length"})

	mWALBytes     = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ingest_wal_size_bytes", Help: "Approx WAL size on disk"})
	mWALSegs      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ingest_wal_segments", Help: "Number of WAL segments"})
	mWALReplay    = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_wal_replay_total", Help: "Events replayed from WAL at startup"})
	mWALAppendErr = prometheus.NewCounter(prometheus.CounterOpts{Name: "ingest_wal_append_errors_total", Help: "WAL append errors"})
)
