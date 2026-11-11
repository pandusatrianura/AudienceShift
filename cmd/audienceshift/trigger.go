package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/catalog"
	"github.com/pandusatrianura/AudienceShift/internal/features/config"
)

func runTrigger(args []string) {
	fs := flag.NewFlagSet("trigger", flag.ExitOnError)
	secret := fs.String("secret", "", "Webhook signing secret (calculates HMAC for Stripe, GitHub, Midtrans, Slack)")
	duplicate := fs.Bool("duplicate", false, "Simulate duplicate delivery (fires 2 concurrent requests to test idempotency)")
	delay := fs.Duration("delay", 0, "Artificial delay to inject before sending webhook")
	listOnly := fs.Bool("list", false, "List all available presets in the webhook catalog")

	_ = fs.Parse(args)

	cat := catalog.NewCatalog()

	if *listOnly {
		fmt.Println("──────────────────────────────────────────────────────────")
		fmt.Println("🗂️  AudienceShift Webhook Preset Catalog")
		fmt.Println("──────────────────────────────────────────────────────────")
		for _, p := range cat.List() {
			fmt.Printf("• %-36s [%s] %s\n", p.ID, strings.ToUpper(p.Provider), p.Name)
			fmt.Printf("  %s\n", p.Description)
		}
		fmt.Println("\nUsage: audienceshift trigger <preset-id> [target_port_or_url]")
		return
	}

	if len(fs.Args()) < 1 {
		fmt.Println("Error: Preset ID required. Use 'audienceshift trigger -list' to view available presets.")
		os.Exit(1)
	}

	presetID := fs.Args()[0]

	cfgPath := config.FindConfigFile()
	fileCfg, _ := config.Load(cfgPath)

	target := "http://localhost:8080"
	if fileCfg != nil && fileCfg.Target != "" {
		target = fileCfg.Target
	}

	if len(fs.Args()) > 1 {
		arg := fs.Args()[1]
		if !strings.Contains(arg, ":") && !strings.HasPrefix(arg, "http") {
			target = "http://localhost:" + arg
		} else {
			target = arg
		}
	}
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		if !strings.Contains(target, ":") {
			target = "http://localhost:" + target
		} else {
			target = "http://" + target
		}
	}

	activeSecret := *secret
	if activeSecret == "" && fileCfg != nil {
		if p, err := cat.Get(presetID); err == nil {
			if s, ok := fileCfg.Secrets[p.Provider]; ok && s != "" {
				activeSecret = s
			} else if fileCfg.Secret != "" {
				activeSecret = fileCfg.Secret
			}
		}
	}

	reqData, err := cat.BuildRequest(presetID, activeSecret)
	if err != nil {
		log.Fatalf("Failed to build preset request: %v", err)
	}

	destURL := strings.TrimRight(target, "/") + reqData.Path

	fmt.Println("──────────────────────────────────────────────────────────")
	fmt.Printf("⚡ Triggering Webhook: %s\n", presetID)
	fmt.Printf("🎯 Destination:      %s\n", destURL)
	if activeSecret != "" {
		fmt.Printf("🔐 Signed with:       %s\n", activeSecret)
	}
	if *duplicate {
		fmt.Printf("🧪 Chaos Mode:       Duplicate Concurrent Delivery (At-Least-Once)\n")
	}
	fmt.Println("──────────────────────────────────────────────────────────")

	if *delay > 0 {
		fmt.Printf("⏳ Waiting %v before sending...\n", *delay)
		time.Sleep(*delay)
	}

	sendReq := func(isDuplicate bool) {
		httpReq, err := http.NewRequest(reqData.Method, destURL, bytes.NewReader(reqData.Body))
		if err != nil {
			log.Printf("Error creating request: %v", err)
			return
		}
		for k, vv := range reqData.Headers {
			for _, v := range vv {
				httpReq.Header.Add(k, v)
			}
		}
		httpReq.Header.Set("X-AudienceShift-Trigger", "true")
		if isDuplicate {
			httpReq.Header.Set("X-AudienceShift-Chaos-Duplicate", "true")
		}

		start := time.Now()
		resp, err := http.DefaultClient.Do(httpReq)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("❌ Failed to reach %s: %v\n", destURL, err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		tag := "Primary"
		if isDuplicate {
			tag = "Duplicate"
		}
		fmt.Printf("✅ [%s] Received Status %d in %dms\n", tag, resp.StatusCode, elapsed.Milliseconds())
		if len(body) > 0 {
			fmt.Printf("   Response: %s\n", string(body))
		}
	}

	if *duplicate {
		go sendReq(true)
		sendReq(false)
		time.Sleep(200 * time.Millisecond)
	} else {
		sendReq(false)
	}
}
