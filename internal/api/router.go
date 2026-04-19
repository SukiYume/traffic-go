package api

import (
	"io/fs"
	"log"
	"net/http"
	"strings"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type runtimeProvider interface {
	ActiveProcesses() []model.ProcessListItem
	ActiveStats() model.ActiveStats
}

type Server struct {
	store          *store.Store
	runtime        runtimeProvider
	logger         *log.Logger
	static         fs.FS
	processLogDirs map[string]string
}

func NewServer(trafficStore *store.Store, runtime runtimeProvider, logger *log.Logger, static fs.FS, processLogDirs map[string]string) *Server {
	return &Server{
		store:          trafficStore,
		runtime:        runtime,
		logger:         logger,
		static:         static,
		processLogDirs: cloneStringMap(processLogDirs),
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
	mux.HandleFunc("/api/v1/stats/timeseries", s.handleTimeseries)
	mux.HandleFunc("/api/v1/usage", s.handleUsage)
	mux.HandleFunc("/api/v1/usage/explain", s.handleUsageExplain)
	mux.HandleFunc("/api/v1/top/processes", s.handleTopProcesses)
	mux.HandleFunc("/api/v1/top/remotes", s.handleTopRemotes)
	mux.HandleFunc("/api/v1/top/ports", s.handleTopPorts)
	mux.HandleFunc("/api/v1/forward/usage", s.handleForwardUsage)
	mux.Handle("/", s.spaHandler())
	return loggingMiddleware(s.logger, mux)
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
