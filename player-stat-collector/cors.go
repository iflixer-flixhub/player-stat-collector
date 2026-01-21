package main

import (
	"net/http"
	"net/url"
	"strings"
)

// Options на случай, если захочешь подкрутить поведение.
type CorsOptions struct {
	// Domain: "example.com"
	Domain string

	// Разрешать ли сам корневой домен "example.com" (без поддомена)
	AllowApex bool

	// Разрешать ли credentials (cookies/Authorization). Если true — нельзя Allow-Origin="*"
	AllowCredentials bool

	// Разрешённые методы/заголовки
	AllowMethods []string
	AllowHeaders []string

	// Заголовки, которые можно читать из JS
	ExposeHeaders []string
	MaxAge        int // seconds
}

func Middleware(opts CorsOptions) func(http.Handler) http.Handler {
	domain := strings.ToLower(strings.TrimSpace(opts.Domain))
	if domain == "" {
		panic("cors: Domain is required")
	}

	allowMethods := joinOrDefault(opts.AllowMethods, "GET,POST,PUT,PATCH,DELETE,OPTIONS")
	allowHeaders := joinOrDefault(opts.AllowHeaders, "Content-Type,Authorization")
	exposeHeaders := strings.Join(opts.ExposeHeaders, ", ")

	maxAge := opts.MaxAge
	if maxAge <= 0 {
		maxAge = 86400
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Не CORS-запрос (или запрос из curl/postman без Origin)
				next.ServeHTTP(w, r)
				return
			}
			if isAllowedOrigin(origin, domain, opts.AllowApex) {
				// Важно: Vary, чтобы кеши не смешивали ответы для разных Origin
				w.Header().Add("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", allowMethods)
				w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
				w.Header().Set("Access-Control-Max-Age", itoa(maxAge))

				if opts.AllowCredentials {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				if exposeHeaders != "" {
					w.Header().Set("Access-Control-Expose-Headers", exposeHeaders)
				}

				// Preflight
				if r.Method == http.MethodOptions {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			} else {
				// Origin не разрешён: на OPTIONS можно быстро вернуть 403/204.
				// Я предпочитаю 403, чтобы было видно причину в девтулзах.
				if r.Method == http.MethodOptions {
					http.Error(w, "CORS origin denied", http.StatusForbidden)
					return
				}
				// Для обычных запросов можно просто продолжить без CORS-заголовков:
				// браузер сам заблокирует доступ к ответу JS-коду.
			}

			next.ServeHTTP(w, r)
		})
	}
}

func isAllowedOrigin(origin, domain string, allowApex bool) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	// Обычно CORS работает для http/https. Если нужно разрешить extension:// или null — это отдельная история.
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}

	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}

	// exact apex
	if allowApex && host == domain {
		return true
	}

	// subdomain: endswith ".domain" и при этом есть что-то слева
	suffix := "." + domain
	return strings.HasSuffix(host, suffix) && host != suffix
}

func joinOrDefault(v []string, def string) string {
	if len(v) == 0 {
		return def
	}
	return strings.Join(v, ",")
}

func itoa(n int) string {
	// без strconv, чтобы код был компактнее; если хочешь — замени на strconv.Itoa
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
