package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

// MaxFrameSize limits the maximum allowable Kafka request frame size (16 MB)
// to prevent denial-of-service and out-of-memory crashes on malformed packets.
const MaxFrameSize = 16 * 1024 * 1024

// ErrPortInUse indicates the configured network port is already occupied.
var ErrPortInUse = errors.New("port already in use")

func isPortInUse(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			if errno, ok := sysErr.Err.(syscall.Errno); ok {
				if errno == 10048 || errno == 98 || errno == 48 {
					return true
				}
			}
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "address already in use") ||
		strings.Contains(errStr, "wsaeaddrinuse") ||
		strings.Contains(errStr, "only one usage of each socket address")
}

// Server represents the Kafka wire-compatible TCP server.
type Server struct {
	addr     string
	listener net.Listener
	handler  *Handler
	logger   *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// NewServer instantiates a new Kafka TCP server.
func NewServer(addr string, handler *Handler, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:    addr,
		handler: handler,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins listening and serving connections on the configured address.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		if isPortInUse(err) {
			return fmt.Errorf("%w: %s", ErrPortInUse, s.addr)
		}
		return fmt.Errorf("kafka server listen on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr().String()
	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok && s.handler != nil {
		if s.handler.advertisedPort <= 0 {
			s.handler.advertisedPort = int32(tcpAddr.Port)
		}
	}
	s.mu.Unlock()

	s.logger.Info("kafka protocol server listening", slog.String("addr", s.addr))

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the bound network address.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Error("accept error", slog.Any("error", err))
				return
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.wg.Done()
	}()

	var lengthBuf [4]byte

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Set read deadline so idle connections don't block shutdown indefinitely
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// 1. Read 4-byte message length prefix
		_, err := io.ReadFull(conn, lengthBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// Continue loop on timeout to check ctx
				continue
			}
			return
		}

		msgLength := int(binary.BigEndian.Uint32(lengthBuf[:]))
		if msgLength <= 0 || msgLength > MaxFrameSize {
			s.logger.Warn("invalid kafka frame length, closing connection",
				slog.Int("length", msgLength),
				slog.Int("max_allowed", MaxFrameSize),
			)
			return
		}

		// 2. Read full frame payload
		frameBuf := make([]byte, msgLength)
		_, err = io.ReadFull(conn, frameBuf)
		if err != nil {
			s.logger.Warn("failed to read complete kafka frame", slog.Any("error", err))
			return
		}

		// 3. Decode header
		dec := NewDecoder(frameBuf)
		header, err := DecodeRequestHeader(dec)
		if err != nil {
			s.logger.Warn("failed to decode kafka request header", slog.Any("error", err))
			return
		}

		remainingBody, _ := dec.Slice(dec.Remaining())

		// 4. Handle request
		respBytes, err := s.handler.HandleRequest(s.ctx, header, remainingBody)
		if err != nil {
			s.logger.Error("failed to handle kafka request",
				slog.Int("api_key", int(header.ApiKey)),
				slog.Int("correlation_id", int(header.CorrelationId)),
				slog.Any("error", err),
			)
			return
		}

		// 5. Send response: [4-byte length prefix][response bytes]
		// For acks == 0, Kafka clients do not expect any response over the wire
		if respBytes == nil {
			continue
		}

		var respLenBuf [4]byte
		binary.BigEndian.PutUint32(respLenBuf[:], uint32(len(respBytes)))

		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(respLenBuf[:]); err != nil {
			return
		}
		if _, err := conn.Write(respBytes); err != nil {
			return
		}
	}
}

// Close gracefully terminates the server.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	s.logger.Info("kafka protocol server closed")
	return err
}

