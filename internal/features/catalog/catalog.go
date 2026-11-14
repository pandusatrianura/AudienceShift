package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/config"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
)

var (
	ErrPresetNotFound = errors.New("webhook preset not found")
)

// Preset represents an official webhook template payload with default headers and metadata.
type Preset struct {
	ID          string              `json:"id"`          // e.g. "stripe:payment_intent.succeeded"
	Provider    string              `json:"provider"`    // e.g. "stripe", "midtrans", "github", "slack"
	Name        string              `json:"name"`        // e.g. "Payment Intent Succeeded"
	Description string              `json:"description"` // e.g. "Triggered when a customer completes payment"
	Headers     map[string][]string `json:"headers"`
	Body        string              `json:"body"`
}

var defaultPresets = []*Preset{
	// ─── STRIPE ─────────────────────────────────────────────────────────────
	{
		ID:          "stripe:payment_intent.succeeded",
		Provider:    "stripe",
		Name:        "Stripe: Payment Intent Succeeded",
		Description: "Fires when customer payment is successfully authorized and captured",
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: `{
  "id": "evt_test_payment_intent_succeeded",
  "object": "event",
  "api_version": "2024-04-10",
  "created": 1715000000,
  "type": "payment_intent.succeeded",
  "data": {
    "object": {
      "id": "pi_3MtwBwLkdIwHu7ix28a3tqPa",
      "object": "payment_intent",
      "amount": 2000,
      "amount_received": 2000,
      "currency": "usd",
      "status": "succeeded",
      "client_secret": "pi_3MtwBwLkdIwHu7ix28a3tqPa_secret_test",
      "payment_method_types": ["card"]
    }
  }
}`,
	},
	{
		ID:          "stripe:customer.subscription.deleted",
		Provider:    "stripe",
		Name:        "Stripe: Subscription Canceled",
		Description: "Fires when a customer cancels recurring subscription",
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: `{
  "id": "evt_test_subscription_deleted",
  "object": "event",
  "type": "customer.subscription.deleted",
  "data": {
    "object": {
      "id": "sub_1MtwBxLkdIwHu7ix38b4uqPb",
      "customer": "cus_N5xLkdIwHu7ix",
      "status": "canceled",
      "canceled_at": 1715001200
    }
  }
}`,
	},

	// ─── MIDTRANS ───────────────────────────────────────────────────────────
	{
		ID:          "midtrans:settlement",
		Provider:    "midtrans",
		Name:        "Midtrans: Payment Settlement (Success)",
		Description: "Fires when payment is completed via QRIS, GoPay, BCA, or Credit Card",
		Headers: map[string][]string{
			"Content-Type":    {"application/json"},
			"X-Midtrans-Type": {"notification"},
		},
		Body: `{
  "transaction_time": "2026-11-10 14:20:00",
  "transaction_status": "settlement",
  "transaction_id": "8488e5d0-9944-4f51-b846-993d0532454b",
  "status_message": "Success, transaction is found",
  "status_code": "200",
  "signature_key": "midtrans_sample_signature_512",
  "payment_type": "qris",
  "order_id": "ORDER-10029",
  "gross_amount": "150000.00",
  "currency": "IDR",
  "fraud_status": "accept"
}`,
	},
	{
		ID:          "midtrans:expire",
		Provider:    "midtrans",
		Name:        "Midtrans: Payment Expired",
		Description: "Fires when customer fails to pay before deadline expires",
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: `{
  "transaction_time": "2026-11-10 10:00:00",
  "transaction_status": "expire",
  "transaction_id": "7133e5d0-1234-4f51-b846-993d0532454b",
  "status_message": "Transaction is expired",
  "status_code": "407",
  "signature_key": "midtrans_sample_signature_expire",
  "payment_type": "bank_transfer",
  "order_id": "ORDER-10028",
  "gross_amount": "75000.00"
}`,
	},

	// ─── GITHUB ─────────────────────────────────────────────────────────────
	{
		ID:          "github:push",
		Provider:    "github",
		Name:        "GitHub: Push Event",
		Description: "Fires when git commits are pushed to repository branch",
		Headers: map[string][]string{
			"Content-Type":      {"application/json"},
			"X-GitHub-Event":    {"push"},
			"X-GitHub-Delivery": {"72d3162e-cc78-11e3-81ab-4c9367dc0958"},
		},
		Body: `{
  "ref": "refs/heads/main",
  "before": "0000000000000000000000000000000000000000",
  "after": "6113728f27ae82c7b1a177c8d93f44c0ad71fb57",
  "repository": {
    "id": 1296269,
    "name": "AudienceShift",
    "full_name": "pandusatrianura/AudienceShift"
  },
  "pusher": {
    "name": "pandusatrianura",
    "email": "pandu@example.com"
  }
}`,
	},
	{
		ID:          "github:pull_request.opened",
		Provider:    "github",
		Name:        "GitHub: Pull Request Opened",
		Description: "Fires when a new PR is created",
		Headers: map[string][]string{
			"Content-Type":   {"application/json"},
			"X-GitHub-Event": {"pull_request"},
		},
		Body: `{
  "action": "opened",
  "number": 42,
  "pull_request": {
    "id": 1,
    "title": "feat: add webhook preset catalog",
    "state": "open",
    "user": { "login": "contributor-jane" }
  }
}`,
	},

	// ─── SLACK ──────────────────────────────────────────────────────────────
	{
		ID:          "slack:app_mention",
		Provider:    "slack",
		Name:        "Slack: App Mention",
		Description: "Fires when the bot user is tagged in a public or private Slack channel",
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: `{
  "token": "verification_token_sample",
  "team_id": "T012AB3CDE",
  "api_app_id": "A012VJ4CKE",
  "event": {
    "type": "app_mention",
    "user": "U01234567",
    "text": "<@U0LAN0Z89> deploy release to production",
    "ts": "1715003000.000200",
    "channel": "C012AB3CD"
  },
  "type": "event_callback"
}`,
	},

	// ─── SHOPIFY ────────────────────────────────────────────────────────────
	{
		ID:          "shopify:orders.create",
		Provider:    "shopify",
		Name:        "Shopify: Order Created",
		Description: "Fires when an order is placed on your Shopify store",
		Headers: map[string][]string{
			"Content-Type":          {"application/json"},
			"X-Shopify-Topic":       {"orders/create"},
			"X-Shopify-Shop-Domain": {"shop.myshopify.com"},
		},
		Body: `{
  "id": 820982911946154508,
  "email": "customer@example.com",
  "total_price": "299.99",
  "currency": "USD",
  "financial_status": "paid",
  "line_items": [
    {
      "id": 866550311766439020,
      "title": "Wireless Noise Cancelling Headphones",
      "quantity": 1,
      "price": "299.99"
    }
  ]
}`,
	},

	// ─── XENDIT ─────────────────────────────────────────────────────────────
	{
		ID:          "xendit:invoice.paid",
		Provider:    "xendit",
		Name:        "Xendit: Invoice Paid (Success)",
		Description: "Fires when a customer successfully pays an invoice via Virtual Account, QRIS, or eWallet",
		Headers: map[string][]string{
			"Content-Type":     {"application/json"},
			"X-Callback-Token": {"xnd_webhook_callback_token_sample"},
		},
		Body: `{
  "id": "xnd_inv_609a39f1c7d24a001a1c991e",
  "external_id": "ORDER-10029",
  "user_id": "608d4ff0959c9b001a6132aa",
  "is_high": false,
  "payment_method": "BCA",
  "status": "PAID",
  "merchant_name": "AudienceShift Store",
  "amount": 150000,
  "paid_amount": 150000,
  "bank_code": "BCA",
  "paid_at": "2026-11-10T14:30:00.000Z",
  "payer_email": "customer@example.com",
  "description": "Invoice for ORDER-10029",
  "payment_channel": "VIRTUAL_ACCOUNT"
}`,
	},

	// ─── GITLAB ─────────────────────────────────────────────────────────────
	{
		ID:          "gitlab:push",
		Provider:    "gitlab",
		Name:        "GitLab: Push Hook",
		Description: "Fires when commits are pushed to a GitLab repository branch",
		Headers: map[string][]string{
			"Content-Type":   {"application/json"},
			"X-Gitlab-Event": {"Push Hook"},
			"X-Gitlab-Token": {"gitlab_webhook_secret_token"},
		},
		Body: `{
  "object_kind": "push",
  "event_name": "push",
  "before": "0000000000000000000000000000000000000000",
  "after": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
  "ref": "refs/heads/main",
  "checkout_sha": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
  "project": {
    "id": 15,
    "name": "audienceshift",
    "web_url": "https://gitlab.example.com/team/audienceshift"
  },
  "user_name": "Jane Contributor",
  "total_commits_count": 1
}`,
	},

	// ─── DISCORD ────────────────────────────────────────────────────────────
	{
		ID:          "discord:interaction",
		Provider:    "discord",
		Name:        "Discord: Ping Interaction",
		Description: "Fires when Discord verifies bot endpoint or triggers an interaction command",
		Headers: map[string][]string{
			"Content-Type":          {"application/json"},
			"X-Signature-Ed25519":   {"sample_ed25519_signature_test"},
			"X-Signature-Timestamp": {"1715003000"},
		},
		Body: `{
  "id": "119876543210987654",
  "type": 1,
  "token": "interaction_sample_token",
  "version": 1
}`,
	},

	// ─── TWILIO ─────────────────────────────────────────────────────────────
	{
		ID:          "twilio:sms.received",
		Provider:    "twilio",
		Name:        "Twilio: Inbound SMS Received",
		Description: "Fires when Twilio receives an SMS and triggers the webhook callback",
		Headers: map[string][]string{
			"Content-Type":       {"application/x-www-form-urlencoded"},
			"X-Twilio-Signature": {"sample_twilio_signature_test"},
		},
		Body: `From=%2B12025550143&To=%2B12025550189&Body=Hello+from+AudienceShift&MessageSid=SM1234567890abcdef&AccountSid=AC1234567890abcdef`,
	},
}

// Catalog provides preset discovery and request creation.
type Catalog struct {
	presets     map[string]*Preset
	orderedList []*Preset
}

// NewCatalog initializes a new Preset Catalog with standard presets.
func NewCatalog() *Catalog {
	c := &Catalog{
		presets:     make(map[string]*Preset, len(defaultPresets)),
		orderedList: make([]*Preset, 0, len(defaultPresets)),
	}
	for _, p := range defaultPresets {
		c.presets[p.ID] = p
		c.orderedList = append(c.orderedList, p)
	}
	return c
}

// Register adds or updates a preset in the catalog.
func (c *Catalog) Register(p *Preset) {
	if p == nil || p.ID == "" {
		return
	}
	if _, exists := c.presets[p.ID]; !exists {
		c.orderedList = append(c.orderedList, p)
	}
	c.presets[p.ID] = p
}

// RegisterCustomPresets imports custom presets from config.
func (c *Catalog) RegisterCustomPresets(custom []config.CustomPresetConfig) {
	for _, cp := range custom {
		headers := make(map[string][]string)
		for k, v := range cp.Headers {
			headers[k] = []string{v}
		}
		if _, ok := headers["Content-Type"]; !ok {
			headers["Content-Type"] = []string{"application/json"}
		}
		c.Register(&Preset{
			ID:          cp.ID,
			Provider:    cp.Provider,
			Name:        cp.Name,
			Description: cp.Description,
			Headers:     headers,
			Body:        cp.Body,
		})
	}
}

// List returns all registered webhook presets.
func (c *Catalog) List() []*Preset {
	return c.orderedList
}

// Get finds a preset by its unique ID (e.g. "stripe:payment_intent.succeeded").
func (c *Catalog) Get(id string) (*Preset, error) {
	preset, found := c.presets[id]
	if !found {
		return nil, fmt.Errorf("%w: %s", ErrPresetNotFound, id)
	}
	return preset, nil
}

// BuildRequest constructs an inspector.HTTPRequest with valid cryptographic signatures.
func (c *Catalog) BuildRequest(presetID string, signingSecret string) (*inspector.HTTPRequest, error) {
	preset, err := c.Get(presetID)
	if err != nil {
		return nil, err
	}

	headers := make(map[string][]string, len(preset.Headers))
	for k, vv := range preset.Headers {
		copied := make([]string, len(vv))
		copy(copied, vv)
		headers[k] = copied
	}

	bodyBytes := []byte(preset.Body)

	// If Midtrans, update signature_key field in JSON if secret provided
	if preset.Provider == "midtrans" && signingSecret != "" {
		var m map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &m); err == nil {
			orderID, _ := m["order_id"].(string)
			statusCode, _ := m["status_code"].(string)
			grossAmount, _ := m["gross_amount"].(string)
			if orderID != "" && statusCode != "" && grossAmount != "" {
				m["signature_key"] = inspector.GenerateMidtransSignature(orderID, statusCode, grossAmount, signingSecret)
				if reencoded, err := json.MarshalIndent(m, "", "  "); err == nil {
					bodyBytes = reencoded
				}
			}
		}
	}

	req := &inspector.HTTPRequest{
		Method:  "POST",
		Path:    "/webhooks/" + strings.ToLower(preset.Provider),
		Headers: headers,
		Body:    bodyBytes,
	}

	// Apply provider cryptographic signature header
	if signingSecret != "" && preset.Provider != "midtrans" {
		now := time.Now().UTC()
		switch preset.Provider {
		case "stripe":
			sig := inspector.GenerateStripeSignature(req.Body, signingSecret, now)
			req.Headers["Stripe-Signature"] = []string{sig}
		case "github":
			sig := inspector.GenerateGitHubSignature(req.Body, signingSecret)
			req.Headers["X-Hub-Signature-256"] = []string{sig}
		case "slack":
			sig, ts := inspector.GenerateSlackSignature(req.Body, signingSecret, now)
			req.Headers["X-Slack-Signature"] = []string{sig}
			req.Headers["X-Slack-Request-Timestamp"] = []string{ts}
		case "xendit":
			req.Headers["X-Callback-Token"] = []string{signingSecret}
		case "gitlab":
			req.Headers["X-Gitlab-Token"] = []string{signingSecret}
		case "discord":
			req.Headers["X-Signature-Ed25519"] = []string{signingSecret}
		case "twilio":
			req.Headers["X-Twilio-Signature"] = []string{signingSecret}
		default:
			_ = inspector.ResignWebhook(req, signingSecret)
		}
	}

	return req, nil
}
