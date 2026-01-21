package main

import (
	"bufio"
	"database/sql"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func mustDB(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("sql open: %v", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db
}

func originDomain(r *http.Request) string {
	orig := r.Header.Get("Origin")
	if orig == "" {
		return ""
	}

	u, err := url.Parse(orig)
	if err != nil || u.Host == "" {
		return ""
	}

	host := strings.ToLower(u.Hostname()) // без порта
	host = strings.TrimSuffix(host, ".")
	if orig == "null" { // бывает
		return ""
	}
	return normalizeHost(host)
}

func refererDomain(r *http.Request) string {
	ref := r.Referer() // то же самое, что Header.Get("Referer")
	if ref == "" {
		return ""
	}

	u, err := url.Parse(ref)
	if err != nil || u.Host == "" {
		return ""
	}

	host := strings.ToLower(u.Hostname()) // без порта
	// опционально убрать точку в конце (бывает в DNS-стиле)
	host = strings.TrimSuffix(host, ".")
	return normalizeHost(host)
}

func normalizeHost(h string) string {
	h = strings.TrimSpace(h)
	if h == "" {
		return ""
	}

	// если пришло как URL (на всякий случай), вытащим host
	if strings.Contains(h, "://") {
		if u, err := url.Parse(h); err == nil && u.Host != "" {
			h = u.Host
		}
	}

	// убрать порт, если есть
	if strings.Contains(h, ":") {
		// net.SplitHostPort требует порт, поэтому сначала попробуем аккуратно
		if host, _, err := net.SplitHostPort(h); err == nil {
			h = host
		} else {
			// возможно это IPv6 без порта или что-то странное — попробуем Hostname() через url
			if u, err2 := url.Parse("http://" + h); err2 == nil {
				if hn := u.Hostname(); hn != "" {
					h = hn
				}
			}
		}
	}

	h = strings.ToLower(strings.TrimSuffix(h, "."))

	// отсекаем IP (если вдруг)
	if ip := net.ParseIP(h); ip != nil {
		return ""
	}

	// грубая фильтрация мусора
	if strings.ContainsAny(h, " \t\r\n/") || h == "" {
		return ""
	}

	return h
}

func countLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 512*1024)

	lines := 0
	for sc.Scan() {
		lines++
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return lines, err
	}
	return lines, nil
}

func clientIP16(r *http.Request) []byte {
	if ip := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ipTo16(ip)
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return ipTo16(strings.TrimSpace(parts[0]))
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ipTo16(r.RemoteAddr)
	}
	return ipTo16(host)
}

func ipTo16(s string) []byte {
	ip := net.ParseIP(s)
	if ip == nil {
		return nil
	}
	ip = ip.To16()
	if ip == nil {
		return nil
	}
	return []byte(ip)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env %s", k)
	}
	return v
}
func env(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}
func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return def
	}
	return n
}
func envDur(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return d
}

func nullIntTo0(v sql.NullInt64) int {
	if v.Valid {
		return int(v.Int64)
	}
	return 0
}
