package replay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

// ChaosOptions specifies simulation settings for testing consumer idempotency and resilience.
type ChaosOptions struct {
	Duplicate bool          `json:"duplicate"` // Send duplicate event concurrently (at-least-once delivery test)
	Delay     time.Duration `json:"delay"`     // Inject network latency before dispatch
}

// MutationOptions contains overrides applied to an event prior to replaying.
type MutationOptions struct {
	Method        string              `json:"method,omitempty"`
	Path          string              `json:"path,omitempty"`
	Headers       map[string][]string `json:"headers,omitempty"`
	Body          []byte              `json:"body,omitempty"`
	SigningSecret string              `json:"signing_secret,omitempty"`
	TargetURL     string              `json:"target_url,omitempty"`
	Chaos         ChaosOptions        `json:"chaos,omitempty"`
}

// Service handles mutating and replaying historical webhook events against local services.
type Service struct {
	store         inspector.EventStore
	bus           *inspector.EventBus
	defaultTarget string
	defaultSecret string
	httpClient    *http.Client
}

// NewService creates a new ReplayService instance with optional default signing secret.
func NewService(store inspector.EventStore, bus *inspector.EventBus, defaultTarget string, defaultSecret ...string) *Service {
	if defaultTarget == "" {
		defaultTarget = "http://localhost:8080"
	}
	if !strings.HasPrefix(defaultTarget, "http://") && !strings.HasPrefix(defaultTarget, "https://") {
		defaultTarget = "http://" + defaultTarget
	}

	sec := ""
	if len(defaultSecret) > 0 {
		sec = defaultSecret[0]
	}

	return &Service{
		store:         store,
		bus:           bus,
		defaultTarget: defaultTarget,
		defaultSecret: sec,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// SetDefaultSigningSecret configures or updates the default secret for auto-recalculation.
func (s *Service) SetDefaultSigningSecret(secret string) {
	s.defaultSecret = secret
}

// ReplayByID fetches an event by ID from the store, applies mutations, and re-executes it.
func (s *Service) ReplayByID(ctx context.Context, eventID string, opts MutationOptions) (*inspector.WebhookEvent, error) {
	if s.store == nil {
		return nil, errors.New("event store not configured")
	}

	orig, err := s.store.GetByID(eventID)
	if err != nil {
		return nil, fmt.Errorf("retrieving event %s for replay: %w", eventID, err)
	}

	return s.Replay(ctx, orig, opts)
}

// Replay clones an event, applies mutations and optional signature recalculation, and executes the HTTP call.
func (s *Service) Replay(ctx context.Context, original *inspector.WebhookEvent, opts MutationOptions) (*inspector.WebhookEvent, error) {
	if original == nil {
		return nil, errors.New("cannot replay nil event")
	}

	start := time.Now()
	corrID := logger.GetCorrelationID(ctx)
	if corrID == "" {
		corrID = original.ID
	}
	logger.LogOperationStart(ctx, "replay_event", corrID)

	// 1. Create a clone prepared for replay
	replayed := original.CreateReplay()

	// 2. Apply mutations
	if opts.Method != "" {
		replayed.Request.Method = strings.ToUpper(opts.Method)
	}
	if opts.Path != "" {
		replayed.Request.Path = opts.Path
	}
	if opts.Body != nil {
		replayed.Request.Body = opts.Body
	}
	if opts.Headers != nil {
		if replayed.Request.Headers == nil {
			replayed.Request.Headers = make(map[string][]string)
		}
		for k, v := range opts.Headers {
			replayed.Request.Headers[k] = v
		}
	}

	// 3. Re-calculate cryptographic signature if secret was provided or configured in service
	secret := opts.SigningSecret
	if secret == "" {
		secret = s.defaultSecret
	}

	if secret != "" {
		if err := inspector.ResignWebhook(&replayed.Request, secret); err != nil {
			logger.LogOperationFailure(ctx, "replay_event", corrID, time.Since(start), err)
			return nil, fmt.Errorf("recalculating webhook signature: %w", err)
		}
	} else if replayed.Provider == "stripe" {
		if replayed.Request.Headers == nil {
			replayed.Request.Headers = make(map[string][]string)
		}
		replayed.Request.Headers["X-AudienceShift-Warning"] = []string{"Stripe signature timestamp not refreshed (requires signing secret for HMAC recalculation)"}
	}

	// 4. Inject artificial chaos latency if configured
	if opts.Chaos.Delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(opts.Chaos.Delay):
		}
	}

	// 5. Resolve destination target
	target := s.defaultTarget
	if opts.TargetURL != "" {
		target = opts.TargetURL
	}
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}

	destURL := strings.TrimRight(target, "/") + replayed.Request.Path
	if replayed.Request.Query != "" {
		destURL += "?" + replayed.Request.Query
	}

	// If Chaos.Duplicate is true, launch duplicate request concurrently to stress-test idempotency
	if opts.Chaos.Duplicate {
		go func() {
			dupReq, err := http.NewRequestWithContext(context.Background(), replayed.Request.Method, destURL, bytes.NewReader(replayed.Request.Body))
			if err != nil {
				return
			}
			for k, vv := range replayed.Request.Headers {
				for _, v := range vv {
					dupReq.Header.Add(k, v)
				}
			}
			dupReq.Header.Set("X-AudienceShift-Replay", "true")
			dupReq.Header.Set("X-AudienceShift-Chaos-Duplicate", "true")
			dupReq.Header.Set("X-AudienceShift-Original-ID", original.ID)
			dupReq.Host = dupReq.URL.Host

			dupStart := time.Now()
			dupResp, err := s.httpClient.Do(dupReq)
			if err == nil {
				defer dupResp.Body.Close()
				dupBody, _ := io.ReadAll(dupResp.Body)
				dupEvt := replayed.CreateReplay()
				dupEvt.Response = inspector.HTTPResponse{
					StatusCode: dupResp.StatusCode,
					Headers:    dupResp.Header.Clone(),
					Body:       dupBody,
				}
				dupEvt.DurationMs = time.Since(dupStart).Milliseconds()
				if s.store != nil {
					_ = s.store.Save(dupEvt)
				}
				if s.bus != nil {
					s.bus.Publish(dupEvt)
				}
			}
		}()
	}

	// 6. Construct HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, replayed.Request.Method, destURL, bytes.NewReader(replayed.Request.Body))
	if err != nil {
		logger.LogOperationFailure(ctx, "replay_event", corrID, time.Since(start), err)
		return nil, fmt.Errorf("building replay http request: %w", err)
	}

	// Copy headers
	for k, vv := range replayed.Request.Headers {
		for _, v := range vv {
			httpReq.Header.Add(k, v)
		}
	}
	httpReq.Header.Set("X-AudienceShift-Replay", "true")
	httpReq.Header.Set("X-AudienceShift-Original-ID", original.ID)
	httpReq.Host = httpReq.URL.Host

	// 6. Execute request against local service
	reqStart := time.Now()
	resp, err := s.httpClient.Do(httpReq)
	duration := time.Since(reqStart)

	if err != nil {
		replayed.Response = inspector.HTTPResponse{
			StatusCode: http.StatusBadGateway,
			Error:      fmt.Sprintf("Replay execution failed to connect to %s: %v", destURL, err),
		}
		replayed.DurationMs = duration.Milliseconds()

		if s.store != nil {
			_ = s.store.Save(replayed)
		}
		if s.bus != nil {
			s.bus.Publish(replayed)
		}

		logger.LogOperationFailure(ctx, "replay_event", corrID, time.Since(start), err)
		return replayed, nil
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	replayed.Response = inspector.HTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       respBody,
	}
	replayed.DurationMs = duration.Milliseconds()

	// 7. Persist replayed event and notify listeners
	if s.store != nil {
		_ = s.store.Save(replayed)
	}
	if s.bus != nil {
		s.bus.Publish(replayed)
	}

	logger.LogOperationSuccess(ctx, "replay_event", corrID, time.Since(start))
	return replayed, nil
}
