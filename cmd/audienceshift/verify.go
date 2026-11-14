package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/catalog"
	"github.com/pandusatrianura/AudienceShift/internal/features/config"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

type verifyResult struct {
	Name       string `json:"name"`
	Passed     bool   `json:"passed"`
	StatusCode int    `json:"status_code"`
	DurationMs int64  `json:"duration_ms"`
	Message    string `json:"message"`
}

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	provider := fs.String("provider", "stripe", "Webhook provider to verify (stripe, github, midtrans, slack, xendit)")
	secret := fs.String("secret", "whsec_test_secret_for_compliance_suite_12345", "Test webhook signing secret used for HMAC computation")
	pathOverride := fs.String("path", "", "Target URL path (default: /webhooks/<provider>)")
	jsonOutput := fs.Bool("json", false, "Output verification results in machine-readable JSON format for CI/CD")
	timeout := fs.Duration("timeout", 5*time.Second, "HTTP timeout for each test dispatch")

	_ = fs.Parse(args)

	cfgPath := config.FindConfigFile()
	fileCfg, _ := config.Load(cfgPath)

	targetBase := "http://localhost:8080"
	if fileCfg != nil && fileCfg.Target != "" {
		targetBase = fileCfg.Target
	}

	if len(fs.Args()) > 0 {
		arg := fs.Args()[0]
		if !strings.Contains(arg, ":") && !strings.HasPrefix(arg, "http") {
			targetBase = "http://localhost:" + arg
		} else {
			targetBase = arg
		}
	}

	if !strings.HasPrefix(targetBase, "http://") && !strings.HasPrefix(targetBase, "https://") {
		if !strings.Contains(targetBase, ":") {
			targetBase = "http://localhost:" + targetBase
		} else {
			targetBase = "http://" + targetBase
		}
	}

	prov := strings.ToLower(*provider)
	endpointPath := *pathOverride
	if endpointPath == "" {
		endpointPath = "/webhooks/" + prov
	}
	if !strings.HasPrefix(endpointPath, "/") {
		endpointPath = "/" + endpointPath
	}

	destURL := strings.TrimRight(targetBase, "/") + endpointPath

	cat := catalog.NewCatalog()
	if fileCfg != nil && len(fileCfg.CustomPresets) > 0 {
		cat.RegisterCustomPresets(fileCfg.CustomPresets)
	}

	// Pick a sample preset for the provider
	var samplePreset *catalog.Preset
	for _, p := range cat.List() {
		if strings.ToLower(p.Provider) == prov {
			samplePreset = p
			break
		}
	}
	if samplePreset == nil {
		samplePreset = &catalog.Preset{
			ID:       prov + ":sample",
			Provider: prov,
			Name:     prov + " sample",
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Body: `{"event":"compliance.test","id":"evt_test_123"}`,
		}
	}

	if !*jsonOutput {
		fmt.Println("──────────────────────────────────────────────────────────")
		fmt.Printf("🛡️  AudienceShift Webhook Compliance Verification Suite\n")
		fmt.Printf("🎯 Destination:  %s\n", destURL)
		fmt.Printf("🔐 Provider:     %s (Secret: %s)\n", strings.ToUpper(prov), *secret)
		fmt.Println("──────────────────────────────────────────────────────────")
	}

	client := &http.Client{
		Timeout: *timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var results []verifyResult
	allPassed := true

	// ─── TEST 1: Valid Signature Dispatch ──────────────────────────────────────
	{
		start := time.Now()
		req, _ := cat.BuildRequest(samplePreset.ID, *secret)
		req.Path = endpointPath
		httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader(req.Body))
		for k, vv := range req.Headers {
			for _, v := range vv {
				httpReq.Header.Add(k, v)
			}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		dur := time.Since(start).Milliseconds()

		if err != nil {
			allPassed = false
			results = append(results, verifyResult{
				Name:       "1. Valid Signature Dispatch",
				Passed:     false,
				DurationMs: dur,
				Message:    fmt.Sprintf("Connection failed: %v", err),
			})
		} else {
			_ = resp.Body.Close()
			passed := resp.StatusCode >= 200 && resp.StatusCode < 300
			if !passed {
				allPassed = false
			}
			results = append(results, verifyResult{
				Name:       "1. Valid Signature Dispatch",
				Passed:     passed,
				StatusCode: resp.StatusCode,
				DurationMs: dur,
				Message:    fmt.Sprintf("Expected 2xx, got %d %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			})
		}
	}

	// ─── TEST 2: Tampered / Invalid Signature Denial ───────────────────────────
	{
		start := time.Now()
		req, _ := cat.BuildRequest(samplePreset.ID, *secret)
		// Corrupt signature
		for k := range req.Headers {
			if strings.Contains(strings.ToLower(k), "signature") || strings.Contains(strings.ToLower(k), "token") {
				req.Headers[k] = []string{"forged_invalid_signature_hex_12345"}
			}
		}
		httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader(req.Body))
		for k, vv := range req.Headers {
			for _, v := range vv {
				httpReq.Header.Add(k, v)
			}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		dur := time.Since(start).Milliseconds()

		if err != nil {
			results = append(results, verifyResult{
				Name:       "2. Invalid Signature Denial",
				Passed:     false,
				DurationMs: dur,
				Message:    fmt.Sprintf("Connection failed: %v", err),
			})
			allPassed = false
		} else {
			_ = resp.Body.Close()
			// Must be rejected with 400, 401, 403, or 422
			passed := resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 422
			if !passed {
				allPassed = false
			}
			msg := fmt.Sprintf("Correctly rejected with %d", resp.StatusCode)
			if !passed {
				msg = fmt.Sprintf("Vulnerability: Backend accepted forged signature with %d", resp.StatusCode)
			}
			results = append(results, verifyResult{
				Name:       "2. Invalid Signature Denial",
				Passed:     passed,
				StatusCode: resp.StatusCode,
				DurationMs: dur,
				Message:    msg,
			})
		}
	}

	// ─── TEST 3: Missing Signature Denial ─────────────────────────────────────
	{
		start := time.Now()
		httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader([]byte(samplePreset.Body)))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		dur := time.Since(start).Milliseconds()

		if err != nil {
			results = append(results, verifyResult{
				Name:       "3. Missing Signature Denial",
				Passed:     false,
				DurationMs: dur,
				Message:    fmt.Sprintf("Connection failed: %v", err),
			})
			allPassed = false
		} else {
			_ = resp.Body.Close()
			passed := resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 422
			if !passed {
				allPassed = false
			}
			msg := fmt.Sprintf("Correctly rejected with %d", resp.StatusCode)
			if !passed {
				msg = fmt.Sprintf("Vulnerability: Backend accepted webhook missing security header with %d", resp.StatusCode)
			}
			results = append(results, verifyResult{
				Name:       "3. Missing Signature Denial",
				Passed:     passed,
				StatusCode: resp.StatusCode,
				DurationMs: dur,
				Message:    msg,
			})
		}
	}

	// ─── TEST 4: Expired Timestamp Anti-Replay (Stripe / Slack) ───────────────
	if prov == "stripe" || prov == "slack" {
		start := time.Now()
		req, _ := cat.BuildRequest(samplePreset.ID, *secret)
		// Generate signature 10 minutes in the past
		expiredTime := time.Now().Add(-10 * time.Minute)
		if prov == "stripe" {
			req.Headers["Stripe-Signature"] = []string{inspector.GenerateStripeSignature(req.Body, *secret, expiredTime)}
		} else if prov == "slack" {
			sig, ts := inspector.GenerateSlackSignature(req.Body, *secret, expiredTime)
			req.Headers["X-Slack-Signature"] = []string{sig}
			req.Headers["X-Slack-Request-Timestamp"] = []string{ts}
		}

		httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader(req.Body))
		for k, vv := range req.Headers {
			for _, v := range vv {
				httpReq.Header.Add(k, v)
			}
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		dur := time.Since(start).Milliseconds()

		if err != nil {
			results = append(results, verifyResult{
				Name:       "4. Expired Timestamp Denial",
				Passed:     false,
				DurationMs: dur,
				Message:    fmt.Sprintf("Connection failed: %v", err),
			})
			allPassed = false
		} else {
			_ = resp.Body.Close()
			passed := resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403
			if !passed {
				allPassed = false
			}
			msg := fmt.Sprintf("Correctly rejected with %d", resp.StatusCode)
			if !passed {
				msg = fmt.Sprintf("Vulnerability: Backend accepted expired timestamp replay with %d", resp.StatusCode)
			}
			results = append(results, verifyResult{
				Name:       "4. Expired Timestamp Denial",
				Passed:     passed,
				StatusCode: resp.StatusCode,
				DurationMs: dur,
				Message:    msg,
			})
		}
	}

	// ─── TEST 5: Idempotency & Duplicate Delivery ─────────────────────────────
	{
		start := time.Now()
		req1, _ := cat.BuildRequest(samplePreset.ID, *secret)
		req2, _ := cat.BuildRequest(samplePreset.ID, *secret)

		var wg sync.WaitGroup
		var code1, code2 int
		var err1, err2 error

		wg.Add(2)
		go func() {
			defer wg.Done()
			httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader(req1.Body))
			for k, vv := range req1.Headers {
				for _, v := range vv {
					httpReq.Header.Add(k, v)
				}
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(httpReq)
			if err == nil {
				code1 = resp.StatusCode
				_ = resp.Body.Close()
			} else {
				err1 = err
			}
		}()

		go func() {
			defer wg.Done()
			httpReq, _ := http.NewRequest("POST", destURL, bytes.NewReader(req2.Body))
			for k, vv := range req2.Headers {
				for _, v := range vv {
					httpReq.Header.Add(k, v)
				}
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(httpReq)
			if err == nil {
				code2 = resp.StatusCode
				_ = resp.Body.Close()
			} else {
				err2 = err
			}
		}()

		wg.Wait()
		dur := time.Since(start).Milliseconds()

		if err1 != nil || err2 != nil {
			results = append(results, verifyResult{
				Name:       "5. Duplicate Delivery Idempotency",
				Passed:     false,
				DurationMs: dur,
				Message:    fmt.Sprintf("Concurrent request error: req1=%v, req2=%v", err1, err2),
			})
			allPassed = false
		} else {
			passed := code1 >= 200 && code1 < 300 && code2 >= 200 && code2 < 300
			if !passed {
				allPassed = false
			}
			results = append(results, verifyResult{
				Name:       "5. Duplicate Delivery Idempotency",
				Passed:     passed,
				StatusCode: code1,
				DurationMs: dur,
				Message:    fmt.Sprintf("Both requests handled concurrently: req1=%d, req2=%d", code1, code2),
			})
		}
	}

	// ─── OUTPUT REPORT ────────────────────────────────────────────────────────
	if *jsonOutput {
		out := map[string]interface{}{
			"passed":       allPassed,
			"target":       destURL,
			"provider":     prov,
			"test_results": results,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
	} else {
		for _, r := range results {
			statusPill := "[PASS]"
			if !r.Passed {
				statusPill = "[FAIL]"
			}
			fmt.Printf("%-6s %-36s (%dms) -> %s\n", statusPill, r.Name, r.DurationMs, r.Message)
		}
		fmt.Println("──────────────────────────────────────────────────────────")
		if allPassed {
			fmt.Printf("✅ All %d compliance tests PASSED! Endpoint is defensively secure.\n", len(results))
		} else {
			fmt.Printf("❌ Compliance verification FAILED. Please review vulnerabilities above.\n")
		}
	}

	if !allPassed {
		os.Exit(1)
	}
}
