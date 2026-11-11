package inspector

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// GenerateGitHubSignature computes an HMAC-SHA256 signature in GitHub format: sha256=<hex>.
func GenerateGitHubSignature(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifyGitHubSignature validates an X-Hub-Signature-256 header using constant-time comparison.
func VerifyGitHubSignature(payload []byte, signatureHeader string, secret string) bool {
	if secret == "" || signatureHeader == "" {
		return false
	}
	expected := GenerateGitHubSignature(payload, secret)
	return hmac.Equal([]byte(signatureHeader), []byte(expected))
}

// GenerateStripeSignature computes a Stripe-Signature header: t=<unix_ts>,v1=<hex>.
func GenerateStripeSignature(payload []byte, secret string, timestamp time.Time) string {
	ts := strconv.FormatInt(timestamp.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%s,v1=%s", ts, signature)
}

// VerifyStripeSignature validates a Stripe-Signature header against the payload and tolerance.
func VerifyStripeSignature(payload []byte, header string, secret string, tolerance time.Duration) bool {
	if secret == "" || header == "" {
		return false
	}

	parts := strings.Split(header, ",")
	var timestampStr string
	var signatures []string

	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if kv[0] == "t" {
			timestampStr = kv[1]
		} else if kv[0] == "v1" {
			signatures = append(signatures, kv[1])
		}
	}

	if timestampStr == "" || len(signatures) == 0 {
		return false
	}

	tsInt, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return false
	}

	if tolerance > 0 {
		eventTime := time.Unix(tsInt, 0)
		diff := time.Since(eventTime)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			return false
		}
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestampStr))
	mac.Write([]byte("."))
	mac.Write(payload)
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	for _, sig := range signatures {
		if hmac.Equal([]byte(sig), []byte(expectedSig)) {
			return true
		}
	}

	return false
}

// GenerateSlackSignature computes X-Slack-Signature: v0=<hex> and returns the timestamp header value.
func GenerateSlackSignature(payload []byte, secret string, timestamp time.Time) (string, string) {
	ts := strconv.FormatInt(timestamp.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:"))
	mac.Write([]byte(ts))
	mac.Write([]byte(":"))
	mac.Write(payload)
	signature := "v0=" + hex.EncodeToString(mac.Sum(nil))
	return signature, ts
}

// VerifySlackSignature validates Slack's signature headers.
func VerifySlackSignature(payload []byte, sigHeader string, tsHeader string, secret string, tolerance time.Duration) bool {
	if secret == "" || sigHeader == "" || tsHeader == "" {
		return false
	}

	tsInt, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return false
	}

	if tolerance > 0 {
		eventTime := time.Unix(tsInt, 0)
		diff := time.Since(eventTime)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			return false
		}
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:"))
	mac.Write([]byte(tsHeader))
	mac.Write([]byte(":"))
	mac.Write(payload)
	expectedSig := "v0=" + hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(sigHeader), []byte(expectedSig))
}

// GenerateMidtransSignature computes a Midtrans SHA-512 signature: SHA512(order_id + status_code + gross_amount + server_key).
func GenerateMidtransSignature(orderID, statusCode, grossAmount, serverKey string) string {
	hasher := sha512.New()
	hasher.Write([]byte(orderID + statusCode + grossAmount + serverKey))
	return hex.EncodeToString(hasher.Sum(nil))
}

// VerifyMidtransSignature validates a Midtrans signature string.
func VerifyMidtransSignature(orderID, statusCode, grossAmount, serverKey, expectedSig string) bool {
	if serverKey == "" || expectedSig == "" {
		return false
	}
	calculated := GenerateMidtransSignature(orderID, statusCode, grossAmount, serverKey)
	return hmac.Equal([]byte(strings.ToLower(calculated)), []byte(strings.ToLower(expectedSig)))
}

// ResignWebhook inspects the request headers, detects the provider, and updates the signature header with newly calculated HMAC.
func ResignWebhook(req *HTTPRequest, secret string) error {
	if req == nil {
		return errors.New("cannot resign nil request")
	}
	if secret == "" {
		return errors.New("cannot resign with empty secret")
	}

	provider := DetectProvider(req.Headers)
	if req.Headers == nil {
		req.Headers = make(map[string][]string)
	}

	now := time.Now().UTC()

	switch provider {
	case "stripe":
		sig := GenerateStripeSignature(req.Body, secret, now)
		req.Headers["Stripe-Signature"] = []string{sig}
		return nil

	case "github":
		sig := GenerateGitHubSignature(req.Body, secret)
		req.Headers["X-Hub-Signature-256"] = []string{sig}
		return nil

	case "slack":
		sig, ts := GenerateSlackSignature(req.Body, secret, now)
		req.Headers["X-Slack-Signature"] = []string{sig}
		req.Headers["X-Slack-Request-Timestamp"] = []string{ts}
		return nil

	default:
		// Generic HMAC-SHA256 signature
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(req.Body)
		req.Headers["X-Signature-SHA256"] = []string{hex.EncodeToString(mac.Sum(nil))}
		return nil
	}
}
