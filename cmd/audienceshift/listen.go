package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pandusatrianura/AudienceShift/internal/features/config"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
	"github.com/pandusatrianura/AudienceShift/internal/features/tui"
	"github.com/pandusatrianura/AudienceShift/internal/features/tunnel"
	"github.com/pandusatrianura/AudienceShift/internal/features/web"
)

func runListen(args []string) {
	// Auto-load .audienceshift.yaml / .audienceshift.yml if present
	cfgPath := config.FindConfigFile()
	fileCfg, _ := config.Load(cfgPath)

	defaultServer := "ws://localhost:8080/tunnel/connect"
	defaultTarget := "http://localhost:8080"
	defaultID := ""
	defaultToken := ""
	defaultSecret := ""
	defaultWeb := false
	defaultWebAddr := ":4040"
	defaultHeadless := false

	if fileCfg != nil {
		if fileCfg.Server != "" {
			defaultServer = fileCfg.Server
		}
		if fileCfg.Target != "" {
			defaultTarget = fileCfg.Target
		}
		if fileCfg.TunnelID != "" {
			defaultID = fileCfg.TunnelID
		}
		if fileCfg.Token != "" {
			defaultToken = fileCfg.Token
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

	fs := flag.NewFlagSet("listen", flag.ExitOnError)
	serverURL := fs.String("server", defaultServer, "AudienceShift tunnel server WebSocket URL")
	tunnelID := fs.String("id", defaultID, "Custom tunnel subdomain or identifier (default: auto-generated)")
	token := fs.String("token", defaultToken, "Authentication token for tunnel server")
	secret := fs.String("secret", defaultSecret, "Webhook signing secret for automatic HMAC re-signing upon replay")
	enableWeb := fs.Bool("web", defaultWeb, "Enable local embedded Web Studio dashboard")
	webAddr := fs.String("web-addr", defaultWebAddr, "Local address for embedded Web Studio")
	headless := fs.Bool("headless", defaultHeadless, "Run in headless mode without interactive TUI")
	exportPath := fs.String("export", "", "File path to export captured webhook events upon exit (e.g. events.json)")

	_ = fs.Parse(args)

	// Local target resolution: positional argument > config file > default
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

	if *tunnelID == "" {
		*tunnelID = tunnel.GenerateTunnelID()
	}

	// Calculate display public URL
	publicURL := fmt.Sprintf("https://%s.%s", *tunnelID, extractHostFromWS(*serverURL))

	// Core instances
	store := inspector.NewMemoryStore(1000)
	bus := inspector.NewEventBus()
	defer bus.Close()

	replaySvc := replay.NewService(store, bus, localTarget, *secret)

	// Optional Web Studio with automatic port fallback
	var webURL string
	if *enableWeb {
		webSvc := web.NewServer(web.ServerOptions{
			TunnelID:    *tunnelID,
			PublicURL:   publicURL,
			LocalTarget: localTarget,
			Store:       store,
			Bus:         bus,
			ReplaySvc:   replaySvc,
		})
		ln, url, err := webSvc.Start(*webAddr)
		if err != nil {
			log.Printf("Web Studio failed to start: %v", err)
		} else {
			webURL = url
			defer ln.Close()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Client initialization
	client := tunnel.NewClient(tunnel.ClientOptions{
		ServerURL:   *serverURL,
		TunnelID:    *tunnelID,
		AuthToken:   *token,
		LocalTarget: localTarget,
		Store:       store,
		Bus:         bus,
	})

	go func() {
		if err := client.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Tunnel client loop terminated: %v", err)
		}
	}()
	defer client.Stop()

	// Dump events to export file if requested upon exit
	defer func() {
		if *exportPath != "" && store != nil {
			evts, err := store.List(10000)
			if err == nil && len(evts) > 0 {
				data, err := inspector.ExportJSON(evts, false)
				if err == nil {
					_ = os.WriteFile(*exportPath, data, 0644)
					fmt.Printf("\n💾 Exported %d webhook events to %s\n", len(evts), *exportPath)
				}
			}
		}
	}()

	// Handle OS interrupt signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
		client.Stop()
		if *exportPath != "" && store != nil {
			evts, err := store.List(10000)
			if err == nil && len(evts) > 0 {
				data, err := inspector.ExportJSON(evts, false)
				if err == nil {
					_ = os.WriteFile(*exportPath, data, 0644)
					fmt.Printf("\n💾 Exported %d webhook events to %s\n", len(evts), *exportPath)
				}
			}
		}
		os.Exit(0)
	}()

	if *headless {
		fmt.Printf("AudienceShift Tunnel Active!\n")
		fmt.Printf("  Tunnel ID:    %s\n", *tunnelID)
		fmt.Printf("  Public URL:   %s\n", publicURL)
		fmt.Printf("  Local Target: %s\n", localTarget)
		if *enableWeb && webURL != "" {
			fmt.Printf("  Web Studio:   %s\n", webURL)
		}
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
	app, cleanup := tui.NewApp(*tunnelID, publicURL, localTarget, bus, replaySvc, webURL)
	defer cleanup()

	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
	}
}

func extractHostFromWS(wsURL string) string {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "tunnel.example.com"
	}
	host := u.Host
	if colon := strings.Index(host, ":"); colon != -1 {
		host = host[:colon]
	}
	return host
}
