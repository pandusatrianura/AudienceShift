package tunnel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

const (
	// DefaultTunnelHeader contains the target tunnel identifier.
	HeaderTunnelID = "X-AudienceShift-Tunnel-ID"

	// ConnectPath is the WebSocket endpoint used to establish the client tunnel.
	ConnectPath = "/tunnel/connect"

	// DefaultMaxBodySize limits payload sizes to 10MB to prevent memory exhaustion.
	DefaultMaxBodySize = 10 * 1024 * 1024
)

// GenerateTunnelID produces a clean random subdomain/tunnel identifier.
func GenerateTunnelID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("tun_%d", time.Now().UnixNano()%1000000)
	}
	return hex.EncodeToString(b)
}

// DefaultYamuxConfig returns a low-latency, robust yamux configuration.
func DefaultYamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = 10 * time.Second
	cfg.ConnectionWriteTimeout = 10 * time.Second
	// Suppress noisy internal log lines
	cfg.LogOutput = io.Discard
	return cfg
}

// wsConn adapts a Gorilla WebSocket connection to a net.Conn suitable for Yamux multiplexing.
type wsConn struct {
	conn    *websocket.Conn
	reader  io.Reader
	writeMu sync.Mutex
}

// WrapWS creates a net.Conn compatible stream from a Gorilla WebSocket connection.
func WrapWS(ws *websocket.Conn) net.Conn {
	return &wsConn{conn: ws}
}

func (c *wsConn) Read(b []byte) (int, error) {
	for {
		if c.reader == nil {
			msgType, r, err := c.conn.NextReader()
			if err != nil {
				return 0, err
			}
			if msgType != websocket.BinaryMessage && msgType != websocket.TextMessage {
				continue
			}
			c.reader = r
		}
		n, err := c.reader.Read(b)
		if err == io.EOF {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	w, err := c.conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	if err != nil {
		_ = w.Close()
		return n, err
	}
	return n, w.Close()
}

func (c *wsConn) Close() error {
	return c.conn.Close()
}

func (c *wsConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *wsConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}

func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *wsConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// ExtractSubdomain parses the subdomain from Host header, or fallback to query/header.
func ExtractSubdomain(r *http.Request, baseDomain string) string {
	// First check explicit header
	if h := r.Header.Get(HeaderTunnelID); h != "" {
		return strings.ToLower(h)
	}

	// Next check query param (?tunnel=xyz)
	if q := r.URL.Query().Get("tunnel"); q != "" {
		return strings.ToLower(q)
	}

	// Next check subdomain: "xyz.example.com"
	host := r.Host
	if colon := strings.Index(host, ":"); colon != -1 {
		host = host[:colon]
	}
	host = strings.ToLower(host)
	base := strings.ToLower(baseDomain)

	if base != "" && strings.HasSuffix(host, "."+base) {
		sub := strings.TrimSuffix(host, "."+base)
		if sub != "" {
			return sub
		}
	}

	return ""
}
