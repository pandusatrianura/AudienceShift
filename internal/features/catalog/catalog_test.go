package catalog_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/catalog"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestCatalogListAndGet(t *testing.T) {
	cat := catalog.NewCatalog()
	presets := cat.List()

	if len(presets) < 5 {
		t.Fatalf("expected at least 5 presets, got %d", len(presets))
	}

	// Get Stripe preset
	stripePreset, err := cat.Get("stripe:payment_intent.succeeded")
	if err != nil {
		t.Fatalf("failed to get stripe preset: %v", err)
	}
	if stripePreset.Provider != "stripe" {
		t.Errorf("expected provider 'stripe', got %q", stripePreset.Provider)
	}

	// Get Midtrans preset
	midtransPreset, err := cat.Get("midtrans:settlement")
	if err != nil {
		t.Fatalf("failed to get midtrans preset: %v", err)
	}
	if midtransPreset.Provider != "midtrans" {
		t.Errorf("expected provider 'midtrans', got %q", midtransPreset.Provider)
	}

	// Invalid preset
	_, err = cat.Get("nonexistent:event")
	if err == nil {
		t.Fatal("expected error for non-existent preset")
	}
}

func TestBuildRequestWithSignatures(t *testing.T) {
	cat := catalog.NewCatalog()

	// 1. Stripe with signing secret
	stripeSecret := "whsec_catalog_test_123"
	req, err := cat.BuildRequest("stripe:payment_intent.succeeded", stripeSecret)
	if err != nil {
		t.Fatalf("BuildRequest failed: %v", err)
	}

	sigHeader := req.Header("Stripe-Signature")
	if sigHeader == "" {
		t.Fatalf("expected Stripe-Signature header to be generated")
	}
	if !inspector.VerifyStripeSignature(req.Body, sigHeader, stripeSecret, 1*time.Minute) {
		t.Fatalf("generated Stripe signature failed cryptographic verification")
	}

	// 2. GitHub with signing secret
	ghSecret := "gh_secret_test_999"
	ghReq, err := cat.BuildRequest("github:push", ghSecret)
	if err != nil {
		t.Fatalf("BuildRequest GitHub failed: %v", err)
	}
	ghSig := ghReq.Header("X-Hub-Signature-256")
	if !inspector.VerifyGitHubSignature(ghReq.Body, ghSig, ghSecret) {
		t.Fatalf("generated GitHub signature failed verification")
	}

	// 3. Midtrans with signing secret
	midtransSecret := "SB-Mid-server-key-1234"
	midReq, err := cat.BuildRequest("midtrans:settlement", midtransSecret)
	if err != nil {
		t.Fatalf("BuildRequest Midtrans failed: %v", err)
	}

	var m map[string]interface{}
	_ = json.Unmarshal(midReq.Body, &m)
	sigKey, _ := m["signature_key"].(string)
	if strings.Contains(sigKey, "midtrans_sample") {
		t.Fatalf("expected signature_key to be recalculated with real secret")
	}

	orderID, _ := m["order_id"].(string)
	statusCode, _ := m["status_code"].(string)
	grossAmount, _ := m["gross_amount"].(string)

	if !inspector.VerifyMidtransSignature(orderID, statusCode, grossAmount, midtransSecret, sigKey) {
		t.Fatalf("generated Midtrans signature failed verification")
	}

	// 4. Xendit with callback token
	xenditToken := "xnd_webhook_secret_999"
	xenditReq, err := cat.BuildRequest("xendit:invoice.paid", xenditToken)
	if err != nil {
		t.Fatalf("BuildRequest Xendit failed: %v", err)
	}
	if xenditReq.Header("X-Callback-Token") != xenditToken {
		t.Errorf("expected X-Callback-Token to be %q, got %q", xenditToken, xenditReq.Header("X-Callback-Token"))
	}

	// 5. GitLab with secret token
	gitlabToken := "gl_secret_token_123"
	gitlabReq, err := cat.BuildRequest("gitlab:push", gitlabToken)
	if err != nil {
		t.Fatalf("BuildRequest GitLab failed: %v", err)
	}
	if gitlabReq.Header("X-Gitlab-Token") != gitlabToken {
		t.Errorf("expected X-Gitlab-Token to be %q, got %q", gitlabToken, gitlabReq.Header("X-Gitlab-Token"))
	}

	// 6. Discord with secret signature
	discordSig := "discord_ed25519_test_sig"
	discordReq, err := cat.BuildRequest("discord:interaction", discordSig)
	if err != nil {
		t.Fatalf("BuildRequest Discord failed: %v", err)
	}
	if discordReq.Header("X-Signature-Ed25519") != discordSig {
		t.Errorf("expected X-Signature-Ed25519 to be %q, got %q", discordSig, discordReq.Header("X-Signature-Ed25519"))
	}

	// 7. Twilio with signature
	twilioSig := "twilio_auth_sig_456"
	twilioReq, err := cat.BuildRequest("twilio:sms.received", twilioSig)
	if err != nil {
		t.Fatalf("BuildRequest Twilio failed: %v", err)
	}
	if twilioReq.Header("X-Twilio-Signature") != twilioSig {
		t.Errorf("expected X-Twilio-Signature to be %q, got %q", twilioSig, twilioReq.Header("X-Twilio-Signature"))
	}
}
