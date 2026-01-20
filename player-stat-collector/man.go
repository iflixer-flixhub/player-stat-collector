package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	prometheus.MustRegister(
		mReqTotal, mReqDur,
		mEnqueued, mDropped, mFlushed, mFlushErr,
		mQueueLen, mBufLen,
		mWALBytes, mWALSegs, mWALReplay, mWALAppendErr,
	)
}

var walNotify = make(chan struct{}, 1) // глобально/в main

func main() {
	cfg := loadConfig()

	db := mustDB(cfg.MySQLDSN)
	defer db.Close()

	wal, err := NewWAL(cfg.WALDir, cfg.WALSegmentMaxMB, cfg.WALFsyncEvery)
	if err != nil {
		log.Fatalf("wal init: %v", err)
	}

	events := make(chan Event, cfg.QueueSize)
	dc := NewDomainCache(db, cfg.DomainReloadEvery)
	geo := NewGeoMapper(db, cfg.GeoReloadEvery)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// background geo refresh
	go geo.Run(ctx)
	// background domain refresh
	go dc.Run(ctx)

	// wal tail reader
	tailer := NewWALTailer(wal)
	go tailer.Run(ctx, events, walNotify)

	// background WAL compact + stats gauges
	go func() {
		t := time.NewTicker(cfg.WALCompactEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = wal.Compact()
				cp, segs, bytes, err := wal.Stats()
				if err == nil {
					mWALSegs.Set(float64(segs))
					mWALBytes.Set(float64(bytes))
					_ = cp // used in /stats
				}
			}
		}
	}()

	// start flusher
	go flusher(ctx, db, wal, events, cfg.FlushEvery, cfg.BatchMax)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctxTO, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctxTO); err != nil {
			http.Error(w, "mysql not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	// /log?event=...&domain=...&file_id=...
	handleLog := func(w http.ResponseWriter, r *http.Request) {
		ev, err := buildEvent(r, dc, geo)
		if err != nil {
			mDropped.Inc()
			http.Error(w, fmt.Errorf("bad request: %v", err).Error(), 400)
			return
		}

		if _, err := wal.Append(ev); err != nil {
			mWALAppendErr.Inc()
			mDropped.Inc()
			http.Error(w, "wal write failed", 500)
			return
		}

		// разбудить tailer (non-blocking)
		select {
		case walNotify <- struct{}{}:
		default:
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok\n"))

	}

	mux.HandleFunc("/log", handleLog)

	mux.HandleFunc("/e/", func(w http.ResponseWriter, r *http.Request) {
		evName := strings.TrimPrefix(r.URL.Path, "/e/")
		evName = strings.SplitN(evName, "/", 2)[0]
		if evName == "" {
			mDropped.Inc()
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		q := r.URL.Query()
		q.Set("event", evName)
		r.URL.RawQuery = q.Encode()

		// метрики можно пометить как /e/* отдельно, если хочешь:
		handleLog(w, r)
	})

	mux.HandleFunc("/debug/domain-cache", func(w http.ResponseWriter, r *http.Request) {
		snap := dc.Snapshot()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"items": snap,
			"count": len(snap),
		})
	})
	mux.HandleFunc("/debug/country-cache", func(w http.ResponseWriter, r *http.Request) {
		snap := geo.Snapshot()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"items": snap,
			"count": len(snap),
		})
	})

	// human-friendly stats
	mux.HandleFunc("/debug/wal", func(w http.ResponseWriter, r *http.Request) {
		cp, segs, bytes, _ := wal.Stats()

		wal.readMu.Lock()
		rp := wal.readPos
		wal.readMu.Unlock()

		resp := map[string]any{
			"queue_len":      len(events),
			"wal_segments":   segs,
			"wal_size_bytes": bytes,
			"commit":         cp,
			"read_pos":       rp,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	lim := newLimiter(cfg.ReqMaxInFlight)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           lim.Wrap(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("listening on %s, wal=%s", cfg.ListenAddr, cfg.WALDir)
	log.Fatal(srv.ListenAndServe())
}
