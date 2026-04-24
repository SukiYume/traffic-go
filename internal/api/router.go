package api

import (
	"context"
	"crypto/subtle"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type runtimeProvider interface {
	ActiveProcesses() []model.ProcessListItem
	ActiveStats() model.ActiveStats
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
}

func NewServer(
	trafficStore *store.Store,
	runtime runtimeProvider,
	logger *log.Logger,
	static fs.FS,
	processLogDirs map[string]string,
	auth BasicAuthConfig,
	enableSSJournalFallback bool,
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
	mux.HandleFunc("/api/v1/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/processes", s.handleProcesses)
	mux.HandleFunc("/api/v1/stats/overview", s.handleOverview)
	mux.HandleFunc("/api/v1/stats/monthly", s.handleMonthlyUsage)
	mux.HandleFunc("/api/v1/stats/timeseries", s.handleTimeseries)
	mux.HandleFunc("/api/v1/usage", s.handleUsage)
	mux.HandleFunc("/api/v1/usage/explain", s.handleUsageExplain)
	mux.HandleFunc("/api/v1/top/processes", s.handleTopProcesses)
	mux.HandleFunc("/api/v1/top/remotes", s.handleTopRemotes)
	mux.HandleFunc("/api/v1/top/ports", s.handleTopPorts)
	mux.HandleFunc("/api/v1/forward/usage", s.handleForwardUsage)
	mux.HandleFunc("/api/v1/diagnostics/collector", s.handleCollectorDiagnostics)
	mux.Handle("/", s.spaHandler())
	var handler http.Handler = mux
	if s.auth.enabled() {
		handler = s.basicAuthMiddleware(handler)
	}
	return loggingMiddleware(s.logger, handler)
}

func (a BasicAuthConfig) enabled() bool {
	return strings.TrimSpace(a.Username) != "" || strings.TrimSpace(a.Password) != ""
}

func (s *Server) basicAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/healthz" {
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
			fileServer.ServeHTTP(w, r)
			return
		}

		index, err := fs.ReadFile(s.static, "index.html")
		if err != nil {
			http.Error(w, "frontend not available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
}

func loggingMiddleware(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if logger != nil {
			logger.Printf("%s %s", r.Method, r.URL.RequestURI())
		}
		next.ServeHTTP(w, r)
	})
}
