package main

import "time"

/* ---------------- config ---------------- */

type Config struct {
	ListenAddr string
	MySQLDSN   string

	CorsAllowedHost string

	FlushEvery time.Duration
	BatchMax   int
	QueueSize  int

	DomainReloadEvery time.Duration
	GeoReloadEvery    time.Duration

	ReqMaxInFlight int

	WALDir          string
	WALSegmentMaxMB int
	WALFsyncEvery   time.Duration
	WALCompactEvery time.Duration
}

func loadConfig() Config {
	return Config{
		ListenAddr: env("LISTEN", ":8080"),
		MySQLDSN:   mustEnv("MYSQL_DSN"),

		CorsAllowedHost: mustEnv("CORS_ALLOWED_HOST"),

		FlushEvery: envDur("FLUSH_EVERY", 5*time.Minute),
		BatchMax:   envInt("BATCH_MAX", 2000),
		QueueSize:  envInt("QUEUE_SIZE", 200_000),

		DomainReloadEvery: envDur("DOMAIN_RELOAD_EVERY", 1*time.Hour),
		GeoReloadEvery:    envDur("GEO_RELOAD_EVERY", 1*time.Hour),

		ReqMaxInFlight: envInt("REQ_MAX_INFLIGHT", 2000),

		WALDir:          env("WAL_DIR", "/var/lib/ingest-wal"),
		WALSegmentMaxMB: envInt("WAL_SEGMENT_MAX_MB", 256),
		WALFsyncEvery:   envDur("WAL_FSYNC_EVERY", 1*time.Second),
		WALCompactEvery: envDur("WAL_COMPACT_EVERY", 1*time.Minute),
	}
}
