package inspector_test

import (
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

func TestGitHubSignature(t *testing.T) {
	payload := []byte(`{"action":"opened","issue":{"number":1}}`)
	secret := "my_github_secret"

	sig := inspector.GenerateGitHubSignature(payload, secret)

	if !inspector.VerifyGitHubSignature(payload, sig, secret) {
		t.Errorf("GitHub signature verification failed")
	}

	// Tampered payload
	tampered := []byte(`{"action":"closed","issue":{"number":1}}`)
	if inspector.VerifyGitHubSignature(tampered, sig, secret) {
		t.Errorf("expected tampered payload to fail verification")
	}

	// Wrong secret
	if inspector.VerifyGitHubSignature(payload, sig, "wrong_secret") {
		t.Errorf("expected wrong secret to fail verification")
	}
}

func TestStripeSignature(t *testing.T) {
	payload := []byte(`{"type":"payment_intent.succeeded","amount":2000}`)
	secret := "whsec_test_secret_stripe"
	now := time.Now()

	sig := inspector.GenerateStripeSignature(payload, secret, now)

	// Valid with 5 minutes tolerance
	if !inspector.VerifyStripeSignature(payload, sig, secret, 5*time.Minute) {
		t.Errorf("Stripe signature verification failed")
	}

	// Tampered payload
	tampered := []byte(`{"type":"payment_intent.succeeded","amount":999999}`)
	if inspector.VerifyStripeSignature(tampered, sig, secret, 5*time.Minute) {
		t.Errorf("expected tampered Stripe payload to fail verification")
	}

	// Expired tolerance
	oldTime := now.Add(-10 * time.Minute)
	oldSig := inspector.GenerateStripeSignature(payload, secret, oldTime)
	if inspector.VerifyStripeSignature(payload, oldSig, secret, 1*time.Minute) {
		t.Errorf("expected expired Stripe signature to fail tolerance check")
	}
}

func TestSlackSignature(t *testing.T) {
	payload := []byte(`command=/echo&text=hello`)
	secret := "slack_signing_secret_123"
	now := time.Now()

	sig, ts := inspector.GenerateSlackSignature(payload, secret, now)

	if !inspector.VerifySlackSignature(payload, sig, ts, secret, 5*time.Minute) {
		t.Errorf("Slack signature verification failed")
	}

	// Tampered payload
	if inspector.VerifySlackSignature([]byte("tampered"), sig, ts, secret, 5*time.Minute) {
		t.Errorf("expected tampered Slack payload to fail verification")
	}
}

func TestMidtransSignature(t *testing.T) {
	orderID := "order-101"
	statusCode := "200"
	grossAmount := "150000.00"
	serverKey := "SB-Mid-server-key-xyz"

	sig := inspector.GenerateMidtransSignature(orderID, statusCode, grossAmount, serverKey)

	if !inspector.VerifyMidtransSignature(orderID, statusCode, grossAmount, serverKey, sig) {
		t.Errorf("Midtrans signature verification failed")
	}

	// Invalid order
	if inspector.VerifyMidtransSignature("order-999", statusCode, grossAmount, serverKey, sig) {
		t.Errorf("expected wrong order ID to fail Midtrans verification")
	}
}

func TestResignWebhook(t *testing.T) {
	// Stripe resigning
	stripeReq := &inspector.HTTPRequest{
		Headers: map[string][]string{
			"Stripe-Signature": {"old_sig"},
		},
		Body: []byte(`{"event":"checkout.session.completed"}`),
	}
	secret := "whsec_stripe_test"

	if err := inspector.ResignWebhook(stripeReq, secret); err != nil {
		t.Fatalf("ResignWebhook failed: %v", err)
	}

	newSig := stripeReq.Header("Stripe-Signature")
	if newSig == "old_sig" || newSig == "" {
		t.Fatalf("expected updated Stripe-Signature, got %q", newSig)
	}

	if !inspector.VerifyStripeSignature(stripeReq.Body, newSig, secret, 1*time.Minute) {
		t.Fatalf("re-signed Stripe signature could not be verified")
	}

	// GitHub resigning
	ghReq := &inspector.HTTPRequest{
		Headers: map[string][]string{
			"X-Hub-Signature-256": {"old_gh_sig"},
		},
		Body: []byte(`{"action":"created"}`),
	}
	ghSecret := "gh_secret_test"
	if err := inspector.ResignWebhook(ghReq, ghSecret); err != nil {
		t.Fatalf("ResignWebhook GitHub failed: %v", err)
	}
	newGHSig := ghReq.Header("X-Hub-Signature-256")
	if !inspector.VerifyGitHubSignature(ghReq.Body, newGHSig, ghSecret) {
		t.Fatalf("re-signed GitHub signature could not be verified")
	}
}
