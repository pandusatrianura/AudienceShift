package tunnel

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

// ClientOptions configures a tunnel client session.
type ClientOptions struct {
	ServerURL   string // e.g. "ws://example.com/tunnel/connect" or "wss://..."
	TunnelID    string // Requested subdomain or ID
	AuthToken   string
	LocalTarget string // e.g. "http://localhost:8080"
	Store       inspector.EventStore
	Bus         *inspector.EventBus
}

// Client establishes and maintains a multiplexed reverse tunnel session to the server.
type Client struct {
	opts       ClientOptions
	httpClient *http.Client

	mu        sync.Mutex
	session   *yamux.Session
	running   bool
	stopChan  chan struct{}
	closeOnce sync.Once
}

// NewClient initializes a Client with sensible defaults.
func NewClient(opts ClientOptions) *Client {
	if opts.LocalTarget == "" {
		opts.LocalTarget = "http://localhost:8080"
	}
	if !strings.HasPrefix(opts.LocalTarget, "http://") && !strings.HasPrefix(opts.LocalTarget, "https://") {
		opts.LocalTarget = "http://" + opts.LocalTarget
	}

	return &Client{
		opts: opts,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // Don't auto-follow redirects for webhooks
			},
		},
		stopChan: make(chan struct{}),
	}
}

// Start connects to the server and begins forwarding streams. It blocks until ctx is canceled or Stop() is called.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	backoff := 1 * time.Second
	const maxBackoff = 15 * time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		default:
		}

		err := c.connectAndServe(ctx)
		if err != nil {
			logger.LogOperationFailure(ctx, "client_tunnel_loop", c.opts.TunnelID, 0, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// connectAndServe handles a single connection lifecycle.
func (c *Client) connectAndServe(ctx context.Context) error {
	u, err := url.Parse(c.opts.ServerURL)
	if err != nil {
		return fmt.Errorf("parsing server url: %w", err)
	}

	// Build query params
	q := u.Query()
	q.Set("id", c.opts.TunnelID)
	if c.opts.AuthToken != "" {
		q.Set("token", c.opts.AuthToken)
	}
	u.RawQuery = q.Encode()

	dialer := websocket.DefaultDialer
	ws, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dialing tunnel server: %w", err)
	}

	netConn := WrapWS(ws)
	session, err := yamux.Client(netConn, DefaultYamuxConfig())
	if err != nil {
		_ = ws.Close()
		return fmt.Errorf("initializing yamux client: %w", err)
	}

	c.mu.Lock()
	c.session = session
	c.mu.Unlock()

	logger.LogOperationSuccess(ctx, "client_tunnel_connected", c.opts.TunnelID, 0)

	// Stream accept loop
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			if session.IsClosed() {
				return nil
			}
			return fmt.Errorf("accepting stream: %w", err)
		}

		go c.handleStream(ctx, stream)
	}
}

// handleStream processes an incoming HTTP request from a yamux stream and proxies to localhost.
func (c *Client) handleStream(ctx context.Context, stream *yamux.Stream) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	defer req.Body.Close()

	limitReader := io.LimitReader(req.Body, DefaultMaxBodySize+1)
	bodyBytes, err := io.ReadAll(limitReader)
	if err != nil {
		c.writeErrorResponse(stream, http.StatusBadRequest, "Failed to read request body")
		return
	}
	if len(bodyBytes) > DefaultMaxBodySize {
		c.writeErrorResponse(stream, http.StatusRequestEntityTooLarge, "Payload Too Large: exceeds 10MB limit")
		return
	}

	// Prepare captured inspector request
	capturedReq := inspector.HTTPRequest{
		Method:   req.Method,
		Path:     req.URL.Path,
		Query:    req.URL.RawQuery,
		Headers:  req.Header.Clone(),
		Body:     bodyBytes,
		ClientIP: req.RemoteAddr,
	}

	event := inspector.NewWebhookEvent(c.opts.TunnelID, capturedReq)

	// Prepare request to local server with smart path prefix normalization
	targetBase, err := url.Parse(c.opts.LocalTarget)
	if err != nil {
		c.writeErrorResponse(stream, http.StatusInternalServerError, "Invalid local target URL configuration")
		return
	}

	reqURI := req.URL.RequestURI()
	if targetBase.Path != "" && targetBase.Path != "/" {
		basePath := strings.TrimRight(targetBase.Path, "/")
		if !strings.HasPrefix(req.URL.Path, basePath) {
			destPath := basePath + req.URL.Path
			if req.URL.RawQuery != "" {
				reqURI = destPath + "?" + req.URL.RawQuery
			} else {
				reqURI = destPath
			}
		}
	}

	targetURL := fmt.Sprintf("%s://%s%s", targetBase.Scheme, targetBase.Host, reqURI)
	proxyReq, err := http.NewRequestWithContext(ctx, req.Method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		c.writeErrorResponse(stream, http.StatusInternalServerError, "Failed to create local request")
		return
	}

	// Copy headers
	proxyReq.Header = req.Header.Clone()
	// Set host to local destination
	proxyReq.Host = proxyReq.URL.Host

	start := time.Now()
	resp, err := c.httpClient.Do(proxyReq)
	duration := time.Since(start)

	if err != nil {
		event.Response = inspector.HTTPResponse{
			StatusCode: http.StatusBadGateway,
			Error:      fmt.Sprintf("Failed to reach local target at %s: %v", targetURL, err),
		}
		event.DurationMs = duration.Milliseconds()

		if c.opts.Store != nil {
			_ = c.opts.Store.Save(event)
		}
		if c.opts.Bus != nil {
			c.opts.Bus.Publish(event)
		}

		c.writeErrorResponse(stream, http.StatusBadGateway, event.Response.Error)
		return
	}
	defer resp.Body.Close()

	// Guard against memory exhaustion from unbounded local backend response bodies
	limitedRespReader := io.LimitReader(resp.Body, DefaultMaxBodySize+1)
	respBody, _ := io.ReadAll(limitedRespReader)
	if len(respBody) > DefaultMaxBodySize {
		event.Response = inspector.HTTPResponse{
			StatusCode: http.StatusBadGateway,
			Error:      "Local backend response exceeded 10MB limit",
		}
		event.DurationMs = duration.Milliseconds()

		if c.opts.Store != nil {
			_ = c.opts.Store.Save(event)
		}
		if c.opts.Bus != nil {
			c.opts.Bus.Publish(event)
		}

		c.writeErrorResponse(stream, http.StatusBadGateway, "Local backend response exceeded 10MB limit")
		return
	}

	event.Response = inspector.HTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       respBody,
	}
	event.DurationMs = duration.Milliseconds()

	if c.opts.Store != nil {
		_ = c.opts.Store.Save(event)
	}
	if c.opts.Bus != nil {
		c.opts.Bus.Publish(event)
	}

	// Write response back into the yamux stream
	tunnelResp := &http.Response{
		StatusCode:    resp.StatusCode,
		Status:        resp.Status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        resp.Header.Clone(),
		Body:          io.NopCloser(bytes.NewReader(respBody)),
		ContentLength: int64(len(respBody)),
	}

	_ = tunnelResp.Write(stream)
}

func (c *Client) writeErrorResponse(stream io.Writer, status int, msg string) {
	resp := &http.Response{
		StatusCode:    status,
		Status:        http.StatusText(status),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(msg)),
		ContentLength: int64(len(msg)),
	}
	resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
	_ = resp.Write(stream)
}

// Stop gracefully terminates the tunnel client.
func (c *Client) Stop() {
	c.closeOnce.Do(func() {
		close(c.stopChan)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.session != nil {
			_ = c.session.Close()
		}
		c.running = false
	})
}
