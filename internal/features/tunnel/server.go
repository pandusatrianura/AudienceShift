package tunnel

import (
	"bufio"
	"bytes"
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
	tunnels  map[string][]*TunnelSession
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
		tunnels:    make(map[string][]*TunnelSession),
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
// If multiple clients connect with the same valid token/subdomain, they are joined into a fanout pool.
func (s *Server) RegisterTunnel(id string, token string, session *yamux.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessions := s.tunnels[id]
	active := make([]*TunnelSession, 0, len(sessions)+1)
	for _, sess := range sessions {
		if !sess.Session.IsClosed() {
			if s.AuthToken != "" && sess.Token != token {
				return ErrTunnelAlreadyClaimed
			}
			active = append(active, sess)
		}
	}

	active = append(active, &TunnelSession{
		ID:          id,
		Token:       token,
		Session:     session,
		ConnectedAt: time.Now(),
	})
	s.tunnels[id] = active
	return nil
}

// UnregisterTunnel removes a specific session from a tunnel ID's fanout pool.
func (s *Server) UnregisterTunnel(id string, session *yamux.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessions, exists := s.tunnels[id]
	if !exists {
		return
	}

	active := make([]*TunnelSession, 0, len(sessions))
	for _, sess := range sessions {
		if session == nil || sess.Session == session {
			_ = sess.Session.Close()
			continue
		}
		if !sess.Session.IsClosed() {
			active = append(active, sess)
		}
	}

	if len(active) == 0 {
		delete(s.tunnels, id)
		delete(s.limiters, id)
	} else {
		s.tunnels[id] = active
	}
}

// GetTunnels retrieves all active yamux sessions for a given tunnel ID.
func (s *Server) GetTunnels(id string) []*yamux.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessions, ok := s.tunnels[id]
	if !ok {
		return nil
	}

	var active []*yamux.Session
	for _, sess := range sessions {
		if !sess.Session.IsClosed() {
			active = append(active, sess.Session)
		}
	}
	return active
}

// GetTunnel retrieves the primary yamux session for a given tunnel ID.
func (s *Server) GetTunnel(id string) (*yamux.Session, bool) {
	active := s.GetTunnels(id)
	if len(active) == 0 {
		return nil, false
	}
	return active[0], true
}

// ActiveCount returns the number of currently connected tunnels across all sessions.
func (s *Server) ActiveCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, sessions := range s.tunnels {
		for _, sess := range sessions {
			if !sess.Session.IsClosed() {
				count++
			}
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

	sessions := s.GetTunnels(tunnelID)
	if len(sessions) == 0 {
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

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "AudienceShift: Failed to read incoming request body", http.StatusBadRequest)
		return
	}

	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-Proto", "https")

	type clientResult struct {
		statusCode int
		header     http.Header
		body       []byte
		err        error
	}

	resultCh := make(chan clientResult, len(sessions))

	// Fanout concurrently to all connected client sessions on this tunnel
	for _, sess := range sessions {
		go func(s *yamux.Session) {
			stream, err := s.OpenStream()
			if err != nil {
				resultCh <- clientResult{err: err}
				return
			}
			defer stream.Close()

			_ = stream.SetDeadline(time.Now().Add(30 * time.Second))

			// Clone request with independent body reader for each client
			clonedReq := r.Clone(r.Context())
			clonedReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			clonedReq.ContentLength = int64(len(bodyBytes))

			if err := clonedReq.Write(stream); err != nil {
				resultCh <- clientResult{err: err}
				return
			}

			resp, err := http.ReadResponse(bufio.NewReader(stream), clonedReq)
			if err != nil {
				resultCh <- clientResult{err: err}
				return
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, DefaultMaxBodySize))
			resultCh <- clientResult{
				statusCode: resp.StatusCode,
				header:     resp.Header,
				body:       respBody,
			}
		}(sess)
	}

	// Capture the first successful client response to return to the public webhook provider
	var primaryResp *clientResult
	var lastErr error
	for i := 0; i < len(sessions); i++ {
		res := <-resultCh
		if res.err != nil {
			lastErr = res.err
			continue
		}
		if primaryResp == nil {
			primaryResp = &res
			break
		}
	}

	if primaryResp == nil {
		errMsg := "AudienceShift: All connected local tunnel clients failed to process request"
		if lastErr != nil {
			errMsg = fmt.Sprintf("AudienceShift: Failed to reach local tunnel client: %v", lastErr)
		}
		http.Error(w, errMsg, http.StatusBadGateway)
		return
	}

	for k, vv := range primaryResp.header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(primaryResp.statusCode)
	_, _ = w.Write(primaryResp.body)
}
