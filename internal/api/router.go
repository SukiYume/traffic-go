package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"traffic-go/internal/config"
	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type runtimeProvider interface {
	ActiveProcesses() []model.ProcessListItem
	ActiveStats() model.ActiveStats
	ReadyStatus() model.CollectorReadiness
	Diagnostics() model.CollectorDiagnostics
}

type shadowsocksJournalReader func(context.Context, time.Time, time.Time) ([]string, error)

type BasicAuthConfig struct {
	Username string
	Password string
}

type Server struct {
	store                   *store.Store
	runtime                 runtimeProvider
	logger                  *log.Logger
	static                  fs.FS
	processLogDirs          map[string]string
	auth                    BasicAuthConfig
	enableSSJournalFallback bool
	readShadowsocksJournal  shadowsocksJournalReader
	countCache              *lruCache[string, int]
	processCache            *lruCache[string, cachedTop[model.ProcessSummary]]
	remoteCache             *lruCache[string, cachedTop[model.RemoteSummary]]
	dashboardCache          *lruCache[string, model.DashboardSnapshot]
	countSF                 singleflight.Group
	processSF               singleflight.Group
	remoteSF                singleflight.Group
	dashboardSF             singleflight.Group
	slidingCacheTTL         time.Duration
	archivedCacheTTL        time.Duration
	prefetchCursorMu        sync.Mutex
	prefetchCursors         map[string]evidence.PrefetchCursor
	scanGate                chan struct{}
}

func NewServer(
	trafficStore *store.Store,
	runtime runtimeProvider,
	logger *log.Logger,
	static fs.FS,
	processLogDirs map[string]string,
	auth BasicAuthConfig,
	enableSSJournalFallback bool,
	cacheCfg config.Cache,
) *Server {
	return &Server{
		store:                   trafficStore,
		runtime:                 runtime,
		logger:                  logger,
		static:                  static,
		processLogDirs:          cloneStringMap(processLogDirs),
		auth:                    auth,
		enableSSJournalFallback: enableSSJournalFallback,
		readShadowsocksJournal:  defaultShadowsocksJournalReader,
		countCache:              newLRUCache[string, int](cacheCfg.CountCacheSize),
		processCache:            newWeightedLRUCache[string, cachedTop[model.ProcessSummary]](cacheCfg.ResultCacheSize, newCacheBudget(maxCachedTopBytes), cachedTopWeight[model.ProcessSummary]),
		remoteCache:             newWeightedLRUCache[string, cachedTop[model.RemoteSummary]](cacheCfg.ResultCacheSize, newCacheBudget(maxCachedTopBytes), cachedTopWeight[model.RemoteSummary]),
		dashboardCache:          newLRUCache[string, model.DashboardSnapshot](cacheCfg.ResultCacheSize),
		slidingCacheTTL:         cacheCfg.SlidingTTL,
		archivedCacheTTL:        cacheCfg.ArchivedTTL,
		scanGate:                make(chan struct{}, 1),
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	registerMethods(mux, "/api/v1/healthz", s.handleHealthz, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/readyz", s.handleReadyz, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/dashboard", s.handleDashboard, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/processes", s.handleProcesses, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/stats/overview", s.handleOverview, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/stats/monthly", s.handleMonthlyUsage, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/stats/timeseries", s.handleTimeseries, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/stats/interfaces/timeseries", s.handleInterfaceTimeseries, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/usage", s.handleUsage, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/usage/explain", s.handleUsageExplain, http.MethodGet, http.MethodPost, http.MethodHead)
	registerMethods(mux, "/api/v1/top/processes", s.handleTopProcesses, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/top/remotes", s.handleTopRemotes, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/top/ports", s.handleTopPorts, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/forward/usage", s.handleForwardUsage, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/diagnostics/collector", s.handleCollectorDiagnostics, http.MethodGet, http.MethodHead)
	registerMethods(mux, "/api/v1/diagnostics/store", s.handleStoreDiagnostics, http.MethodGet, http.MethodHead)
	registerHandlerMethods(mux, "/", s.spaHandler(), http.MethodGet, http.MethodHead)
	var handler http.Handler = mux
	if s.auth.enabled() {
		handler = s.basicAuthMiddleware(handler)
	}
	handler = securityHeadersMiddleware(handler)
	handler = requestIDMiddleware(handler)
	return loggingMiddleware(s.logger, handler)
}

func registerMethods(mux *http.ServeMux, path string, handler http.HandlerFunc, methods ...string) {
	registerHandlerMethods(mux, path, handler, methods...)
}

func registerHandlerMethods(mux *http.ServeMux, path string, handler http.Handler, methods ...string) {
	allowed := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		allowed[method] = struct{}{}
	}
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := allowed[r.Method]; !ok {
			w.Header().Set("Allow", strings.Join(methods, ", "))
			writeJSON(w, http.StatusMethodNotAllowed, envelope{"error": "method_not_allowed", "message": "method not allowed"})
			return
		}
		handler.ServeHTTP(w, r)
	}))
}

func (a BasicAuthConfig) enabled() bool {
	return strings.TrimSpace(a.Username) != "" || strings.TrimSpace(a.Password) != ""
}

func (s *Server) basicAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/healthz" || r.URL.Path == "/api/v1/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		username, password, ok := r.BasicAuth()
		if ok &&
			subtle.ConstantTimeCompare([]byte(username), []byte(s.auth.Username)) == 1 &&
			subtle.ConstantTimeCompare([]byte(password), []byte(s.auth.Password)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="traffic-go"`)
		writeJSON(w, http.StatusUnauthorized, envelope{
			"error":   "unauthorized",
			"message": "valid basic authentication credentials are required",
		})
	})
}

func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'; object-src 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
		}
		next.ServeHTTP(w, r)
	})
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if !validRequestID(requestID) {
			var raw [12]byte
			if _, err := rand.Read(raw[:]); err == nil {
				requestID = hex.EncodeToString(raw[:])
			} else {
				requestID = "unavailable"
			}
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r)
	})
}

func validRequestID(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func (s *Server) spaHandler() http.Handler {
	fileServer := http.FileServer(http.FS(s.static))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if _, err := fs.Stat(s.static, path); err == nil {
			if strings.HasPrefix(path, "assets/") {
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			} else {
				w.Header().Set("Cache-Control", "no-cache")
			}
			fileServer.ServeHTTP(w, r)
			return
		}
		// Asset URLs are not client-side routes. Returning index.html here masks a
		// missing/corrupt bundle as a successful JavaScript response and turns a
		// diagnosable 404 into a browser MIME/module-load failure.
		if strings.HasPrefix(path, "assets/") {
			http.NotFound(w, r)
			return
		}

		index, err := fs.ReadFile(s.static, "index.html")
		if err != nil {
			http.Error(w, "frontend not available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(index)
	})
}

func loggingMiddleware(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		if logger != nil {
			path := r.URL.EscapedPath()
			if len(path) > 256 {
				path = path[:256]
			}
			logger.Printf(
				"%s %s status=%d bytes=%d duration_ms=%d request_id=%s",
				r.Method,
				path,
				recorder.status,
				recorder.bytes,
				time.Since(start).Milliseconds(),
				recorder.Header().Get("X-Request-ID"),
			)
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(value []byte) (int, error) {
	written, err := r.ResponseWriter.Write(value)
	r.bytes += written
	return written, err
}
