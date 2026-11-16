package replay

import (
	"context"
	"fmt"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

// DLQResendOptions configures bulk replay of failed events from the Dead Letter Queue.
type DLQResendOptions struct {
	Limit     int           `json:"limit"`      // Max failed events to resend (default: 50)
	Interval  time.Duration `json:"interval"`   // Delay between consecutive resends (default: 50ms)
	TargetURL string        `json:"target_url"` // Target override (optional)
}

// DLQResendResult reports the outcome of a bulk resend operation.
type DLQResendResult struct {
	TotalProcessed int      `json:"total_processed"`
	SuccessCount   int      `json:"success_count"`
	FailedCount    int      `json:"failed_count"`
	ResentEventIDs []string `json:"resent_event_ids"`
}

// ResendDLQ fetches failed events from the store and replays them sequentially with rate-limiting pacing.
func (s *Service) ResendDLQ(ctx context.Context, opts DLQResendOptions) (*DLQResendResult, error) {
	if s.store == nil {
		return nil, fmt.Errorf("event store not configured")
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}

	start := time.Now()
	corrID := logger.GetCorrelationID(ctx)
	if corrID == "" {
		corrID = fmt.Sprintf("dlq_resend_%d", start.UnixNano())
	}

	logger.LogOperationStart(ctx, "dlq_bulk_resend", corrID)

	failedEvents, err := s.store.ListFailed(limit)
	if err != nil {
		logger.LogOperationFailure(ctx, "dlq_bulk_resend", corrID, time.Since(start), err)
		return nil, fmt.Errorf("listing failed events from store: %w", err)
	}

	result := &DLQResendResult{
		TotalProcessed: len(failedEvents),
		ResentEventIDs: make([]string, 0, len(failedEvents)),
	}

	for i, evt := range failedEvents {
		select {
		case <-ctx.Done():
			logger.LogOperationFailure(ctx, "dlq_bulk_resend", corrID, time.Since(start), ctx.Err())
			return result, ctx.Err()
		default:
		}

		if i > 0 && interval > 0 {
			select {
			case <-ctx.Done():
				logger.LogOperationFailure(ctx, "dlq_bulk_resend", corrID, time.Since(start), ctx.Err())
				return result, ctx.Err()
			case <-time.After(interval):
			}
		}

		replayed, err := s.Replay(ctx, evt, MutationOptions{
			TargetURL: opts.TargetURL,
		})

		if err != nil || replayed.IsFailed() {
			result.FailedCount++
		} else {
			result.SuccessCount++
		}
		if replayed != nil {
			result.ResentEventIDs = append(result.ResentEventIDs, replayed.ID)
		}
	}

	logger.LogOperationSuccess(ctx, "dlq_bulk_resend", corrID, time.Since(start))
	return result, nil
}
