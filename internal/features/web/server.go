package web

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/catalog"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

// ServerOptions configures the embedded web studio.
type ServerOptions struct {
	TunnelID    string
	PublicURL   string
	LocalTarget string
	Store       inspector.EventStore
	Bus         *inspector.EventBus
	ReplaySvc   *replay.Service
}

// Server serves the local developer dashboard and API.
type Server struct {
	opts ServerOptions
	mux  *http.ServeMux
}

// NewServer initializes the embedded Web Studio HTTP handler.
func NewServer(opts ServerOptions) *Server {
	s := &Server{
		opts: opts,
		mux:  http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	// 1. Static asset file server from embed.FS
	subFS, err := fs.Sub(AssetsFS, "assets")
	if err == nil {
		fileServer := http.FileServer(http.FS(subFS))
		s.mux.Handle("/", fileServer)
	}

	// 2. API endpoints
	s.mux.HandleFunc("/api/status", s.handleStatus)
	s.mux.HandleFunc("/api/events", s.handleEvents)
	s.mux.HandleFunc("/api/events/stream", s.handleStream)
	s.mux.HandleFunc("/api/events/clear", s.handleClear)
	s.mux.HandleFunc("/api/replay", s.handleReplay)
	s.mux.HandleFunc("/api/catalog", s.handleCatalog)
	s.mux.HandleFunc("/api/catalog/trigger", s.handleCatalogTrigger)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Start binds an HTTP server on addr with automatic port fallback if the initial port is in use.
// For example, if ":4040" is busy, it sequentially tries 4041..4060.
// It returns the active net.Listener, the resolved URL (e.g. "http://localhost:4041"), and any error.
func (s *Server) Start(addr string) (net.Listener, string, error) {
	if addr == "" {
		addr = ":4040"
	}

	var host string
	var basePort int

	if strings.HasPrefix(addr, ":") {
		host = ""
		p, err := strconv.Atoi(addr[1:])
		if err != nil {
			return nil, "", fmt.Errorf("invalid port in address %q: %w", addr, err)
		}
		basePort = p
	} else if !strings.Contains(addr, ":") {
		p, err := strconv.Atoi(addr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid port %q: %w", addr, err)
		}
		basePort = p
	} else {
		h, pStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, "", fmt.Errorf("parsing address %q: %w", addr, err)
		}
		host = h
		p, err := strconv.Atoi(pStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid port in %q: %w", addr, err)
		}
		basePort = p
	}

	var ln net.Listener
	var finalPort int
	var lastErr error

	for port := basePort; port <= basePort+20; port++ {
		target := fmt.Sprintf("%s:%d", host, port)
		l, err := net.Listen("tcp", target)
		if err == nil {
			ln = l
			finalPort = port
			break
		}
		lastErr = err
	}

	if ln == nil {
		return nil, "", fmt.Errorf("unable to bind Web Studio on ports %d-%d: %w", basePort, basePort+20, lastErr)
	}

	displayHost := host
	if displayHost == "" || displayHost == "0.0.0.0" {
		displayHost = "127.0.0.1"
	}
	webURL := fmt.Sprintf("http://%s:%d", displayHost, finalPort)

	srv := &http.Server{
		Handler: s,
	}

	go func() {
		_ = srv.Serve(ln)
	}()

	return ln, webURL, nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"tunnel_id":    s.opts.TunnelID,
		"public_url":   s.opts.PublicURL,
		"local_target": s.opts.LocalTarget,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.opts.Store == nil {
		_ = json.NewEncoder(w).Encode([]interface{}{})
		return
	}
	list, err := s.opts.Store.List(100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(list)
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.Store != nil {
		_ = s.opts.Store.Clear()
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"cleared":true}`))
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	if s.opts.Bus == nil {
		return
	}

	eventCh, unsub := s.opts.Bus.Subscribe(50)
	defer unsub()

	// Initial ping
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	// Periodic keepalive ping to prevent browser/proxy connection drop
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			data, err := json.Marshal(evt)
			if err == nil {
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}

type replayPayload struct {
	ID            string `json:"id"`
	Body          string `json:"body,omitempty"` // base64 encoded
	SigningSecret string `json:"signing_secret,omitempty"`
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req replayPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if s.opts.ReplaySvc == nil {
		http.Error(w, "Replay service not configured", http.StatusInternalServerError)
		return
	}

	var bodyBytes []byte
	if req.Body != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.Body)
		if err == nil {
			bodyBytes = decoded
		} else {
			bodyBytes = []byte(req.Body)
		}
	}

	opts := replay.MutationOptions{
		Body:          bodyBytes,
		SigningSecret: req.SigningSecret,
	}

	replayed, err := s.opts.ReplaySvc.ReplayByID(r.Context(), req.ID, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(replayed)
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	cat := catalog.NewCatalog()
	_ = json.NewEncoder(w).Encode(cat.List())
}

type triggerPayload struct {
	PresetID      string `json:"preset_id"`
	SigningSecret string `json:"signing_secret,omitempty"`
	Duplicate     bool   `json:"duplicate,omitempty"`
}

func (s *Server) handleCatalogTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req triggerPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	cat := catalog.NewCatalog()
	reqData, err := cat.BuildRequest(req.PresetID, req.SigningSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if s.opts.ReplaySvc == nil {
		http.Error(w, "Replay service not configured", http.StatusInternalServerError)
		return
	}

	// Create a synthetic WebhookEvent from preset
	event := inspector.NewWebhookEvent(s.opts.TunnelID, *reqData)

	opts := replay.MutationOptions{
		Chaos: replay.ChaosOptions{
			Duplicate: req.Duplicate,
		},
	}

	replayed, err := s.opts.ReplaySvc.Replay(r.Context(), event, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(replayed)
}
