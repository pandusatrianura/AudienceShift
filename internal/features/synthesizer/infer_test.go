package synthesizer_test

import (
	"strings"
	"testing"

	"github.com/pandusatrianura/AudienceShift/internal/features/synthesizer"
)

func TestSynthesize_StripeWebhook(t *testing.T) {
	raw := []byte(`{
		"id": "evt_123456",
		"object": "event",
		"api_version": "2020-08-27",
		"created": 1600000000,
		"created_at": "2026-11-16T12:00:00Z",
		"livemode": false,
		"data": {
			"object": {
				"id": "pi_999",
				"amount": 2000,
				"currency": "usd",
				"customer_id": "cus_abc123",
				"tax_rate": 10.5
			}
		},
		"tags": ["payment", "invoice"]
	}`)

	res, err := synthesizer.Synthesize("StripeEvent", raw)
	if err != nil {
		t.Fatalf("synthesize failed: %v", err)
	}

	// 1. Check Go generation
	if !strings.Contains(res.Go, "type StripeEvent struct {") {
		t.Errorf("missing StripeEvent struct in Go: %s", res.Go)
	}
	if !strings.Contains(res.Go, "ID") || !strings.Contains(res.Go, "CustomerID") {
		t.Errorf("expected common initialisms ID / CustomerID in Go: %s", res.Go)
	}
	if !strings.Contains(res.Go, "CreatedAt") || !strings.Contains(res.Go, "time.Time") {
		t.Errorf("expected ISO time.Time detection in Go: %s", res.Go)
	}
	if !strings.Contains(res.Go, "TaxRate") || !strings.Contains(res.Go, "float64") {
		t.Errorf("expected float64 detection in Go: %s", res.Go)
	}
	if !strings.Contains(res.Go, "Tags") || !strings.Contains(res.Go, "[]string") {
		t.Errorf("expected []string detection in Go: %s", res.Go)
	}

	// 2. Check TypeScript generation
	if !strings.Contains(res.TypeScript, "export interface StripeEvent {") {
		t.Errorf("missing StripeEvent interface in TS: %s", res.TypeScript)
	}
	if !strings.Contains(res.TypeScript, "customer_id?: string;") {
		t.Errorf("missing customer_id in TS: %s", res.TypeScript)
	}

	// 3. Check Zod generation
	if !strings.Contains(res.Zod, "import { z } from \"zod\";") {
		t.Errorf("missing zod import in Zod schema: %s", res.Zod)
	}
	if !strings.Contains(res.Zod, "export const StripeEventSchema = z.object({") {
		t.Errorf("missing StripeEventSchema in Zod: %s", res.Zod)
	}
	if !strings.Contains(res.Zod, "created_at: z.string().datetime().optional()") {
		t.Errorf("missing datetime validator in Zod: %s", res.Zod)
	}
}

func TestSynthesize_InvalidJSON(t *testing.T) {
	_, err := synthesizer.Synthesize("Test", []byte(`{invalid`))
	if err == nil {
		t.Errorf("expected error for invalid JSON, got nil")
	}

	_, err = synthesizer.Synthesize("Test", []byte(``))
	if err == nil {
		t.Errorf("expected error for empty JSON, got nil")
	}
}
