package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

type contextKey string

const (
	CorrelationIDKey contextKey = "correlation_id"
	UserIDKey        contextKey = "user_id"
)

var (
	defaultLogger *slog.Logger
	once          sync.Once
)

// Init initializes the default structured logger with specified output and level.
func Init(w io.Writer, level slog.Level) {
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewJSONHandler(w, opts)
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

func getLogger() *slog.Logger {
	once.Do(func() {
		if defaultLogger == nil {
			Init(os.Stdout, slog.LevelInfo)
		}
	})
	return defaultLogger
}

// WithCorrelationID attaches a correlation ID to the context.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, CorrelationIDKey, correlationID)
}

// GetCorrelationID extracts the correlation ID from the context or returns empty string.
func GetCorrelationID(ctx context.Context) string {
	if val, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return val
	}
	return ""
}

// LogOperationStart logs the start of an operation with correlation context.
func LogOperationStart(ctx context.Context, operation string, correlationID string, extra ...slog.Attr) {
	if correlationID == "" {
		correlationID = GetCorrelationID(ctx)
	}
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("correlationId", correlationID),
		slog.String("status", "started"),
	}
	attrs = append(attrs, extra...)
	getLogger().LogAttrs(ctx, slog.LevelInfo, "Operation started: "+operation, attrs...)
}

// LogOperationSuccess logs the successful completion of an operation.
func LogOperationSuccess(ctx context.Context, operation string, correlationID string, duration time.Duration, extra ...slog.Attr) {
	if correlationID == "" {
		correlationID = GetCorrelationID(ctx)
	}
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("correlationId", correlationID),
		slog.Int64("duration_ms", duration.Milliseconds()),
		slog.String("status", "success"),
	}
	attrs = append(attrs, extra...)
	getLogger().LogAttrs(ctx, slog.LevelInfo, "Operation succeeded: "+operation, attrs...)
}

// LogOperationFailure logs an operation failure with error context.
func LogOperationFailure(ctx context.Context, operation string, correlationID string, duration time.Duration, err error, extra ...slog.Attr) {
	if correlationID == "" {
		correlationID = GetCorrelationID(ctx)
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	attrs := []slog.Attr{
		slog.String("operation", operation),
		slog.String("correlationId", correlationID),
		slog.Int64("duration_ms", duration.Milliseconds()),
		slog.String("status", "failure"),
		slog.String("error", errMsg),
	}
	attrs = append(attrs, extra...)
	getLogger().LogAttrs(ctx, slog.LevelError, "Operation failed: "+operation, attrs...)
}
