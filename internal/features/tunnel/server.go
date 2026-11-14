package tunnel

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

var (
	ErrTunnelNotFound       = errors.New("tunnel not found or disconnected")
	ErrStreamFailed         = errors.New("failed to open multiplexed stream to tunnel")
	ErrTunnelAlreadyClaimed = errors.New("subdomain is already claimed by another active tunnel")
	ErrInvalidSubdomain     = errors.New("subdomain must be 3-32 alphanumeric characters and hyphens")
)

var subdomainRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$`)

// TokenBucket provides a thread-safe token bucket rate limiter for inbound tunnel requests.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	rate       float64 // tokens per second
	lastRefill time.Time
}

func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{
		tokens:     capacity,
		capacity:   capacity,
		rate:       rate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	return false
}

// TunnelSession holds active connection state and ownership metadata.
type TunnelSession struct {
	ID          string
	Token       string
	Session     *yamux.Session
	ConnectedAt time.Time
}

// Server coordinates incoming public HTTP webhook traffic and multiplexed client connections.
type Server struct {
	BaseDomain string
	AuthToken  string
	StartTime  time.Time
	RateLimit  float64 // default 60.0 req/sec
	RateBurst  float64 // default 100.0

	mu       sync.RWMutex
	tunnels  map[string]*TunnelSession
	limiters map[string]*TokenBucket
	upgrader websocket.Upgrader
}

// NewServer initializes a new tunnel relay server.
func NewServer(baseDomain string, authToken string) *Server {
	return &Server{
		BaseDomain: baseDomain,
		AuthToken:  authToken,
		StartTime:  time.Now(),
		RateLimit:  60.0,
		RateBurst:  100.0,
		tunnels:    make(map[string]*TunnelSession),
		limiters:   make(map[string]*TokenBucket),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  32 * 1024,
			WriteBufferSize: 32 * 1024,
		},
	}
}

// AllowRequest checks whether a tunnel subdomain is within its allowed rate limit.
func (s *Server) AllowRequest(tunnelID string) bool {
	if s.RateLimit <= 0 {
		return true
	}

	s.mu.Lock()
	limiter, exists := s.limiters[tunnelID]
	if !exists {
		limiter = NewTokenBucket(s.RateLimit, s.RateBurst)
		s.limiters[tunnelID] = limiter
	}
	s.mu.Unlock()

	return limiter.Allow()
}

// RegisterTunnel safely binds a tunnel ID to an active yamux session with ownership verification.
func (s *Server) RegisterTunnel(id string, token string, session *yamux.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.tunnels[id]; exists {
		if !existing.Session.IsClosed() {
			// Subdomain collision check: only allow takeover if matching token
			if s.AuthToken != "" && existing.Token != token {
				return ErrTunnelAlreadyClaimed
			}
			// If reconnecting from same client or token, close stale previous session
			_ = existing.Session.Close()
		}
	}

	s.tunnels[id] = &TunnelSession{
		ID:          id,
		Token:       token,
		Session:     session,
		ConnectedAt: time.Now(),
	}
	return nil
}

// UnregisterTunnel removes a tunnel ID only if the active session matches the caller's session.
func (s *Server) UnregisterTunnel(id string, session *yamux.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.tunnels[id]; exists {
		// Only delete if it's the exact same session instance, preventing ghost cleanup races
		if session == nil || existing.Session == session {
			_ = existing.Session.Close()
			delete(s.tunnels, id)
			delete(s.limiters, id)
		}
	}
}

// GetTunnel retrieves the yamux session for a given tunnel ID.
func (s *Server) GetTunnel(id string) (*yamux.Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.tunnels[id]
	if !ok || sess.Session.IsClosed() {
		return nil, false
	}
	return sess.Session, true
}

// ActiveCount returns the number of currently connected tunnels.
func (s *Server) ActiveCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, t := range s.tunnels {
		if !t.Session.IsClosed() {
			count++
		}
	}
	return count
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	uptime := time.Since(s.StartTime).Round(time.Second).String()
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "healthy",
		"uptime":         uptime,
		"active_tunnels": s.ActiveCount(),
		"version":        "0.1.0",
	})
}

// HandleConnect upgrades incoming client connections to WebSocket and initializes yamux server.
func (s *Server) HandleConnect(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	corrID := logger.GetCorrelationID(ctx)
	if corrID == "" {
		corrID = GenerateTunnelID()
	}
	start := time.Now()
	logger.LogOperationStart(ctx, "tunnel_connect", corrID)

	// 1. Auth check
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if s.AuthToken != "" {
		if token != "Bearer "+s.AuthToken && token != s.AuthToken {
			logger.LogOperationFailure(ctx, "tunnel_connect", corrID, time.Since(start), errors.New("unauthorized"))
			http.Error(w, "Unauthorized tunnel client", http.StatusUnauthorized)
			return
		}
	}

	// 2. Validate tunnel subdomain identifier
	tunnelID := r.URL.Query().Get("id")
	if tunnelID == "" {
		tunnelID = GenerateTunnelID()
	}
	tunnelID = strings.ToLower(tunnelID)
	if !subdomainRegex.MatchString(tunnelID) {
		http.Error(w, ErrInvalidSubdomain.Error(), http.StatusBadRequest)
		return
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.LogOperationFailure(ctx, "tunnel_connect", corrID, time.Since(start), err)
		return
	}

	netConn := WrapWS(ws)
	session, err := yamux.Server(netConn, DefaultYamuxConfig())
	if err != nil {
		_ = ws.Close()
		logger.LogOperationFailure(ctx, "tunnel_connect", corrID, time.Since(start), err)
		return
	}

	if err := s.RegisterTunnel(tunnelID, token, session); err != nil {
		_ = session.Close()
		_ = ws.Close()
		logger.LogOperationFailure(ctx, "tunnel_connect", corrID, time.Since(start), err)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	logger.LogOperationSuccess(ctx, "tunnel_connect", corrID, time.Since(start))

	// Block until session closes, then cleanup with exact session reference
	go func() {
		<-session.CloseChan()
		s.UnregisterTunnel(tunnelID, session)
	}()
}

// ServeHTTP handles both client connection upgrades and public inbound webhook traffic.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Healthcheck / Status endpoints (active on root domain, IP, or direct host without subdomain)
	if r.URL.Path == "/healthz" || r.URL.Path == "/status" {
		sub := ExtractSubdomain(r, s.BaseDomain)
		if sub == "" || sub == "healthz" || sub == "status" {
			s.handleHealthz(w, r)
			return
		}
	}

	// 2. Check if this is a tunnel client connecting
	if r.URL.Path == ConnectPath {
		s.HandleConnect(w, r)
		return
	}

	// 3. Identify destination tunnel ID
	tunnelID := ExtractSubdomain(r, s.BaseDomain)
	if tunnelID == "" {
		http.Error(w, "AudienceShift: Missing or invalid tunnel identifier", http.StatusBadRequest)
		return
	}

	session, ok := s.GetTunnel(tunnelID)
	if !ok || session.IsClosed() {
		http.Error(w, fmt.Sprintf("AudienceShift: Tunnel %q is offline", tunnelID), http.StatusBadGateway)
		return
	}

	// Guard against memory exhaustion from oversized payloads
	r.Body = http.MaxBytesReader(w, r.Body, DefaultMaxBodySize)

	// 4. Rate limiting guard per tunnel subdomain (anti-DDoS / anti-flood)
	if !s.AllowRequest(tunnelID) {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", s.RateLimit))
		http.Error(w, "AudienceShift: Ingress rate limit exceeded (429 Too Many Requests)", http.StatusTooManyRequests)
		return
	}

	// 5. Open multiplexed stream to connected client
	stream, err := session.OpenStream()
	if err != nil {
		http.Error(w, "AudienceShift: Failed to open stream to local tunnel client", http.StatusBadGateway)
		return
	}
	defer stream.Close()

	// 4. Enforce strict I/O deadline on stream to prevent goroutine leaks on stalled clients
	_ = stream.SetDeadline(time.Now().Add(30 * time.Second))

	// 5. Forward incoming HTTP request across yamux stream
	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-Proto", "https")
	if err := r.Write(stream); err != nil {
		http.Error(w, "AudienceShift: Failed to write request to tunnel stream", http.StatusBadGateway)
		return
	}

	// 6. Read response back from local tunnel client
	resp, err := http.ReadResponse(bufio.NewReader(stream), r)
	if err != nil {
		http.Error(w, "AudienceShift: Failed to read response from local tunnel client", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// 7. Relay response back to external webhook caller
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
