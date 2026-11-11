package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/config"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
	"github.com/pandusatrianura/AudienceShift/internal/features/tui"
	"github.com/pandusatrianura/AudienceShift/internal/features/web"
	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

func runMock(args []string) {
	// Auto-load .audienceshift.yaml / .audienceshift.yml if present
	cfgPath := config.FindConfigFile()
	fileCfg, _ := config.Load(cfgPath)

	defaultPort := ":9000"
	defaultTarget := "http://localhost:8080"
	defaultSecret := ""
	defaultForward := true
	defaultWeb := false
	defaultWebAddr := ":4040"
	defaultHeadless := false

	if fileCfg != nil {
		if fileCfg.Target != "" {
			defaultTarget = fileCfg.Target
		}
		if fileCfg.Secret != "" {
			defaultSecret = fileCfg.Secret
		}
		if fileCfg.Web {
			defaultWeb = true
		}
		if fileCfg.WebAddr != "" {
			defaultWebAddr = fileCfg.WebAddr
		}
		if fileCfg.Headless {
			defaultHeadless = true
		}
	}

	fs := flag.NewFlagSet("mock", flag.ExitOnError)
	port := fs.String("port", defaultPort, "Local HTTP port to receive mock webhooks on")
	forward := fs.Bool("forward", defaultForward, "Forward received webhooks to local target")
	secret := fs.String("secret", defaultSecret, "Webhook signing secret for automatic HMAC re-signing upon replay")
	enableWeb := fs.Bool("web", defaultWeb, "Enable local embedded Web Studio dashboard")
	webAddr := fs.String("web-addr", defaultWebAddr, "Local address for embedded Web Studio")
	headless := fs.Bool("headless", defaultHeadless, "Run in headless mode without interactive TUI")

	_ = fs.Parse(args)

	// Format mock port
	mockPort := *port
	if !strings.HasPrefix(mockPort, ":") {
		mockPort = ":" + mockPort
	}

	// Resolve local target: positional argument > config file > default
	localTarget := defaultTarget
	if len(fs.Args()) > 0 {
		arg := fs.Args()[0]
		if !strings.Contains(arg, ":") && !strings.HasPrefix(arg, "http") {
			localTarget = "http://localhost:" + arg
		} else {
			localTarget = arg
		}
	} else if !strings.HasPrefix(localTarget, "http://") && !strings.HasPrefix(localTarget, "https://") {
		if !strings.Contains(localTarget, ":") {
			localTarget = "http://localhost:" + localTarget
		} else {
			localTarget = "http://" + localTarget
		}
	}

	// Prevent infinite forwarding loop: target must not point to mock port on localhost
	validateTargetNotMockPort(localTarget, mockPort)

	// Start local mock HTTP listener
	ln, err := net.Listen("tcp", mockPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Unable to listen on %s: %v\nUse --port to specify an alternate port.\n", mockPort, err)
		os.Exit(1)
	}
	defer ln.Close()

	actualMockAddr := ln.Addr().String()
	mockBaseURL := "http://localhost" + mockPort

	store := inspector.NewMemoryStore(1000)
	bus := inspector.NewEventBus()
	defer bus.Close()

	replaySvc := replay.NewService(store, bus, localTarget, *secret)

	// Optional Web Studio with automatic port fallback
	var webURL string
	if *enableWeb {
		webSvc := web.NewServer(web.ServerOptions{
			TunnelID:    "mock-offline",
			PublicURL:   mockBaseURL,
			LocalTarget: localTarget,
			Store:       store,
			Bus:         bus,
			ReplaySvc:   replaySvc,
		})
		webLn, url, err := webSvc.Start(*webAddr)
		if err != nil {
			log.Printf("Web Studio failed to start: %v", err)
		} else {
			webURL = url
			defer webLn.Close()
		}
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Build Mock HTTP Handler
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corrID := inspector.GenerateID()
		ctx := logger.WithCorrelationID(r.Context(), corrID)
		logger.LogOperationStart(ctx, "mock_receive_webhook", corrID)

		// 10MB limit protection (Poison Pill defense)
		limitedReader := io.LimitReader(r.Body, 10<<20+1)
		bodyBytes, err := io.ReadAll(limitedReader)
		if err != nil {
			logger.LogOperationFailure(ctx, "mock_receive_webhook", corrID, time.Since(start), err)
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		if len(bodyBytes) > 10<<20 {
			errTooLarge := fmt.Errorf("payload exceeds 10MB limit")
			logger.LogOperationFailure(ctx, "mock_receive_webhook", corrID, time.Since(start), errTooLarge)
			http.Error(w, "Payload Too Large (Max 10MB)", http.StatusRequestEntityTooLarge)
			return
		}

		headers := make(map[string][]string, len(r.Header))
		for k, vv := range r.Header {
			copied := make([]string, len(vv))
			copy(copied, vv)
			headers[k] = copied
		}

		clientIP := r.RemoteAddr
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			clientIP = host
		}

		httpReq := inspector.HTTPRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			Query:    r.URL.RawQuery,
			Headers:  headers,
			Body:     bodyBytes,
			ClientIP: clientIP,
		}

		evt := inspector.NewWebhookEvent("mock-offline", httpReq)

		if *forward {
			destURL := strings.TrimRight(localTarget, "/") + httpReq.Path
			if httpReq.Query != "" {
				destURL += "?" + httpReq.Query
			}

			targetReq, err := http.NewRequestWithContext(ctx, httpReq.Method, destURL, bytes.NewReader(httpReq.Body))
			if err != nil {
				evt.Response = inspector.HTTPResponse{
					StatusCode: http.StatusBadGateway,
					Error:      fmt.Sprintf("Failed to construct forwarding request: %v", err),
				}
				evt.DurationMs = time.Since(start).Milliseconds()
				_ = store.Save(evt)
				bus.Publish(evt)
				logger.LogOperationFailure(ctx, "mock_forward_webhook", corrID, time.Since(start), err)
				http.Error(w, evt.Response.Error, http.StatusBadGateway)
				return
			}

			for k, vv := range headers {
				lk := strings.ToLower(k)
				if lk == "content-length" || lk == "host" || lk == "connection" {
					continue
				}
				for _, v := range vv {
					targetReq.Header.Add(k, v)
				}
			}
			targetReq.Header.Set("X-AudienceShift-Mock", "true")
			targetReq.Host = targetReq.URL.Host

			targetResp, err := httpClient.Do(targetReq)
			if err != nil {
				evt.Response = inspector.HTTPResponse{
					StatusCode: http.StatusBadGateway,
					Error:      fmt.Sprintf("Target unreachable: %v", err),
				}
				evt.DurationMs = time.Since(start).Milliseconds()
				_ = store.Save(evt)
				bus.Publish(evt)
				logger.LogOperationFailure(ctx, "mock_forward_webhook", corrID, time.Since(start), err)
				http.Error(w, evt.Response.Error, http.StatusBadGateway)
				return
			}
			defer targetResp.Body.Close()

			respBody, _ := io.ReadAll(io.LimitReader(targetResp.Body, 10<<20))
			evt.Response = inspector.HTTPResponse{
				StatusCode: targetResp.StatusCode,
				Headers:    targetResp.Header.Clone(),
				Body:       respBody,
			}
			evt.DurationMs = time.Since(start).Milliseconds()

			_ = store.Save(evt)
			bus.Publish(evt)
			logger.LogOperationSuccess(ctx, "mock_forward_webhook", corrID, time.Since(start),
				slog.Int("status", targetResp.StatusCode),
				slog.String("path", httpReq.Path),
			)

			// Forward target response back to sender
			for k, vv := range targetResp.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(targetResp.StatusCode)
			_, _ = w.Write(respBody)
		} else {
			// No-forward mock recorder mode
			evt.Response = inspector.HTTPResponse{
				StatusCode: http.StatusOK,
				Headers:    map[string][]string{"Content-Type": {"application/json"}},
				Body:       []byte(`{"status":"captured","mock":true}`),
			}
			evt.DurationMs = time.Since(start).Milliseconds()
			_ = store.Save(evt)
			bus.Publish(evt)
			logger.LogOperationSuccess(ctx, "mock_receive_webhook", corrID, time.Since(start),
				slog.Int("status", 200),
				slog.String("path", httpReq.Path),
			)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"status":"captured","id":%q,"provider":%q}`+"\n", evt.ID, evt.Provider)
		}
	})

	server := &http.Server{
		Handler:      mockHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 35 * time.Second,
	}

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("Mock server stopped: %v", err)
		}
	}()
	defer server.Close()

	// Handle OS interrupt signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		shutdownCtx, sCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer sCancel()
		_ = server.Shutdown(shutdownCtx)
		os.Exit(0)
	}()

	if *headless {
		fmt.Printf("AudienceShift Offline Mock Receiver Active!\n")
		fmt.Printf("  Receiver Addr: %s (%s)\n", actualMockAddr, mockBaseURL)
		fmt.Printf("  Forwarding To: %s (enabled: %v)\n", localTarget, *forward)
		if *enableWeb && webURL != "" {
			fmt.Printf("  Web Studio:    %s\n", webURL)
		}
		fmt.Printf("Send webhooks to %s/<any-path>\n", mockBaseURL)
		fmt.Printf("Press Ctrl+C to stop.\n\n")

		eventCh, unsub := bus.Subscribe(50)
		defer unsub()
		for evt := range eventCh {
			statusStr := fmt.Sprintf("%d", evt.Response.StatusCode)
			if evt.Response.StatusCode == 0 {
				statusStr = "..."
			}
			fmt.Printf("[%s] %-4s %-25s -> %s (%dms)\n",
				evt.Provider,
				evt.Request.Method,
				evt.Request.Path,
				statusStr,
				evt.DurationMs,
			)
		}
		return
	}

	// Run interactive TUI
	app, cleanup := tui.NewApp("mock-offline", mockBaseURL, localTarget, bus, replaySvc, webURL)
	defer cleanup()

	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
	}
}

func validateTargetNotMockPort(localTarget, mockPort string) {
	u, err := url.Parse(localTarget)
	if err != nil {
		return
	}
	host := u.Hostname()
	isLocal := host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == ""
	if !isLocal {
		return
	}

	targetPort := u.Port()
	if targetPort == "" {
		if u.Scheme == "https" {
			targetPort = "443"
		} else {
			targetPort = "80"
		}
	}

	cleanMockPort := strings.TrimPrefix(mockPort, ":")
	if targetPort == cleanMockPort {
		fmt.Fprintf(os.Stderr, "Error: Target %s loops back to the mock receiver itself (:%s).\nForwarding would create an infinite loop.\n", localTarget, cleanMockPort)
		os.Exit(1)
	}
}
