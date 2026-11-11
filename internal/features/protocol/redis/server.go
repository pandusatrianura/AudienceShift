package redis

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
)

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

// Server implements a Redis Streams RESP wire-compatible TCP server.
type Server struct {
	addr     string
	listener net.Listener
	broker   *broker.Service
	logger   *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// NewServer creates a new Redis Streams server.
func NewServer(addr string, b *broker.Service, l *slog.Logger) *Server {
	if l == nil {
		l = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		broker: b,
		logger: l,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start binds to the TCP port and starts listening for Redis connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		if isPortInUse(err) {
			return fmt.Errorf("%w: %s", ErrPortInUse, s.addr)
		}
		return fmt.Errorf("redis server listen on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.addr = ln.Addr().String()
	s.mu.Unlock()

	s.logger.Info("redis streams protocol server listening", slog.String("addr", s.addr))

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the bound address.
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

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		args, err := ReadCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// Keep connection alive on idle timeout, check context for cancellation
				continue
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		resp := s.executeCommand(args)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := writer.Write(resp); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}

		// Clean client disconnect on QUIT
		if strings.ToUpper(args[0]) == "QUIT" {
			return
		}
	}
}

func (s *Server) executeCommand(args []string) []byte {
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING":
		if len(args) > 1 {
			return EncodeBulkString(args[1])
		}
		return EncodeSimpleString("PONG")

	case "COMMAND":
		return EncodeEmptyArray()

	case "CLIENT":
		return EncodeSimpleString("OK")

	case "HELLO":
		return []byte("*14\r\n$6\r\nserver\r\n$11\r\npocketqueue\r\n$7\r\nversion\r\n$5\r\n0.1.0\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n")

	case "QUIT":
		return EncodeSimpleString("OK")

	case "INFO":
		info := "# Server\r\nredis_version:7.2.0-pocketqueue\r\nos:pocketqueue\r\n"
		return EncodeBulkString(info)

	case "XLEN":
		if len(args) < 2 {
			return EncodeError("wrong number of arguments for 'xlen' command")
		}
		meta, err := s.broker.GetTopicMetadata(s.ctx, args[1])
		if err != nil || len(meta.Partitions) == 0 {
			return EncodeInteger(0)
		}
		return EncodeInteger(meta.Partitions[0].TotalCount)

	case "XADD":
		if len(args) < 4 {
			return EncodeError("wrong number of arguments for 'xadd' command")
		}
		stream := args[1]
		idx := 2

		// Handle optional flags
		for idx < len(args) && (strings.ToUpper(args[idx]) == "NOMKSTREAM" || strings.ToUpper(args[idx]) == "MAXLEN") {
			if strings.ToUpper(args[idx]) == "MAXLEN" {
				idx += 2 // skip MAXLEN and count
			} else {
				idx++
			}
		}

		if idx >= len(args) {
			return EncodeError("syntax error in 'xadd' command")
		}

		// ID parameter (usually "*")
		idx++

		// Field-value pairs
		fields := make(map[string]string)
		var firstField string
		for i := idx; i+1 < len(args); i += 2 {
			if firstField == "" {
				firstField = args[i]
			}
			fields[args[i]] = args[i+1]
		}

		payload, _ := json.Marshal(fields)
		rec, err := s.broker.Produce(s.ctx, stream, 0, []byte(firstField), payload, nil)
		if err != nil {
			return EncodeError(fmt.Sprintf("failed to append to stream: %v", err))
		}

		// Redis stream ID: <offset>-0
		streamID := fmt.Sprintf("%d-0", rec.Offset)
		return EncodeBulkString(streamID)

	case "XRANGE":
		if len(args) < 4 {
			return EncodeError("wrong number of arguments for 'xrange' command")
		}
		stream := args[1]
		startStr := args[2]

		var startOffset int64 = 0
		if startStr != "-" {
			sParts := strings.Split(startStr, "-")
			parsed, _ := strconv.ParseInt(sParts[0], 10, 64)
			startOffset = parsed
		}

		count := 50
		if len(args) >= 6 && strings.ToUpper(args[4]) == "COUNT" {
			if c, err := strconv.Atoi(args[5]); err == nil && c > 0 {
				count = c
			}
		}

		records, err := s.broker.Fetch(s.ctx, stream, 0, startOffset, count)
		if err != nil {
			return EncodeEmptyArray()
		}

		entries := make([][]byte, 0, len(records))
		for _, r := range records {
			id := fmt.Sprintf("%d-0", r.Offset)
			var fieldValues [][2]string

			var obj map[string]string
			if err := json.Unmarshal(r.Value, &obj); err == nil && len(obj) > 0 {
				for k, v := range obj {
					fieldValues = append(fieldValues, [2]string{k, v})
				}
			} else {
				fieldValues = append(fieldValues, [2]string{"payload", string(r.Value)})
			}

			entries = append(entries, EncodeStreamEntry(id, fieldValues))
		}

		out := new(strings.Builder)
		out.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))
		for _, e := range entries {
			out.Write(e)
		}
		return []byte(out.String())

	case "XREAD":
		// Format: XREAD [COUNT n] [BLOCK ms] STREAMS stream1 ... offset1 ...
		streamIdx := -1
		for i, a := range args {
			if strings.ToUpper(a) == "STREAMS" {
				streamIdx = i
				break
			}
		}
		if streamIdx == -1 || streamIdx+2 >= len(args) {
			return EncodeError("syntax error in 'xread' command")
		}

		count := 50
		var blockMs int64 = -1
		for i := 1; i+1 < streamIdx; i++ {
			switch strings.ToUpper(args[i]) {
			case "COUNT":
				if c, err := strconv.Atoi(args[i+1]); err == nil && c > 0 {
					count = c
				}
			case "BLOCK":
				if ms, err := strconv.ParseInt(args[i+1], 10, 64); err == nil && ms >= 0 {
					blockMs = ms
				}
			}
		}

		streamsPart := args[streamIdx+1:]
		numStreams := len(streamsPart) / 2
		if numStreams <= 0 {
			return EncodeNullArray()
		}

		streamName := streamsPart[0]
		offsetStr := streamsPart[numStreams]
		var startOffset int64 = 0
		if offsetStr == "$" {
			meta, err := s.broker.GetTopicMetadata(s.ctx, streamName)
			if err == nil && len(meta.Partitions) > 0 {
				startOffset = meta.Partitions[0].Latest
			} else {
				startOffset = 0
			}
		} else if offsetStr != "0" {
			sParts := strings.Split(offsetStr, "-")
			parsed, _ := strconv.ParseInt(sParts[0], 10, 64)
			startOffset = parsed + 1 // XREAD returns strictly greater than ID
		}

		records, _ := s.broker.Fetch(s.ctx, streamName, 0, startOffset, count)
		if len(records) == 0 && blockMs >= 0 {
			bus := s.broker.EventBus()
			if bus != nil {
				subCh, unsub := bus.Subscribe()
				defer unsub()

				var timeoutCh <-chan time.Time
				if blockMs > 0 {
					timer := time.NewTimer(time.Duration(blockMs) * time.Millisecond)
					defer timer.Stop()
					timeoutCh = timer.C
				}

			waitLoop:
				for {
					select {
					case <-s.ctx.Done():
						return EncodeNullArray()
					case <-timeoutCh:
						return EncodeNullArray()
					case rec, ok := <-subCh:
						if !ok {
							return EncodeNullArray()
						}
						if rec.Topic == streamName && rec.Partition == 0 && rec.Offset >= startOffset {
							records = append(records, rec)
							if count > 1 {
								extra, _ := s.broker.Fetch(s.ctx, streamName, 0, rec.Offset+1, count-1)
								records = append(records, extra...)
							}
							break waitLoop
						}
					}
				}
			}
		}

		if len(records) == 0 {
			return EncodeNullArray()
		}

		entries := make([][]byte, 0, len(records))
		for _, r := range records {
			id := fmt.Sprintf("%d-0", r.Offset)
			var fieldValues [][2]string
			var obj map[string]string
			if err := json.Unmarshal(r.Value, &obj); err == nil && len(obj) > 0 {
				for k, v := range obj {
					fieldValues = append(fieldValues, [2]string{k, v})
				}
			} else {
				fieldValues = append(fieldValues, [2]string{"payload", string(r.Value)})
			}
			entries = append(entries, EncodeStreamEntry(id, fieldValues))
		}

		// Redis XREAD envelope: *1\r\n *2\r\n $streamName\r\n *entries...\r\n
		out := new(strings.Builder)
		out.WriteString("*1\r\n*2\r\n")
		out.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(streamName), streamName))
		out.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))
		for _, e := range entries {
			out.Write(e)
		}
		return []byte(out.String())

	case "XACK":
		return EncodeInteger(1)

	default:
		return EncodeError(fmt.Sprintf("unknown command '%s'", cmd))
	}
}

// Close gracefully stops the Redis protocol server.
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
	return err
}

