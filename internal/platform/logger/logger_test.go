package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/pandusatrianura/AudienceShift/internal/platform/logger"
)

func TestLoggerOperations(t *testing.T) {
	buf := &bytes.Buffer{}
	logger.Init(buf, slog.LevelDebug)

	ctx := context.Background()
	ctx = logger.WithCorrelationID(ctx, "corr-12345")

	if id := logger.GetCorrelationID(ctx); id != "corr-12345" {
		t.Fatalf("expected correlation ID 'corr-12345', got %q", id)
	}

	// Test operation start
	buf.Reset()
	logger.LogOperationStart(ctx, "test_operation", "")
	var startMap map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &startMap); err != nil {
		t.Fatalf("failed to parse JSON log: %v, raw: %s", err, buf.String())
	}
	if startMap["operation"] != "test_operation" || startMap["correlationId"] != "corr-12345" {
		t.Fatalf("unexpected start log content: %v", startMap)
	}

	// Test operation success
	buf.Reset()
	logger.LogOperationSuccess(ctx, "test_operation", "", 45*time.Millisecond)
	var succMap map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &succMap); err != nil {
		t.Fatalf("failed to parse JSON log: %v", err)
	}
	if succMap["status"] != "success" || succMap["duration_ms"].(float64) != 45 {
		t.Fatalf("unexpected success log: %v", succMap)
	}

	// Test operation failure
	buf.Reset()
	testErr := errors.New("boom")
	logger.LogOperationFailure(ctx, "test_operation", "", 12*time.Millisecond, testErr)
	var failMap map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &failMap); err != nil {
		t.Fatalf("failed to parse JSON log: %v", err)
	}
	if failMap["status"] != "failure" || failMap["error"] != "boom" {
		t.Fatalf("unexpected failure log: %v", failMap)
	}
}
