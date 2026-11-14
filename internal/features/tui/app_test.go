package tui_test

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/tui"
)

func TestTUIModelLifecycle(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe(10)
	defer unsub()

	model := tui.NewModel("test-tunnel", "https://test.tunnel.com", "http://localhost:8080", ch, nil, "http://localhost:4040")

	// 1. Initial view contains header and tunnel info
	initialView := model.View()
	if !strings.Contains(initialView, "AUDIENCESHIFT") {
		t.Errorf("expected view to contain AUDIENCESHIFT")
	}
	if !strings.Contains(initialView, "test-tunnel") {
		t.Errorf("expected view to contain tunnel ID")
	}

	// 2. Simulate resize
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	model = updated.(tui.Model)

	// 3. Receive event
	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Headers: map[string][]string{
			"Content-Type":     {"application/json"},
			"Stripe-Signature": {"sig_123"},
		},
		Body: []byte(`{"id":"evt_123","amount":1000}`),
	})
	evt.Response = inspector.HTTPResponse{
		StatusCode: 200,
	}

	updated, _ = model.Update(evt)
	model = updated.(tui.Model)

	eventView := model.View()
	if !strings.Contains(eventView, "/webhooks/stripe") {
		t.Errorf("expected event view to contain webhook path")
	}

	// 4. Test Space to Pause
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeySpace})
	model = updated.(tui.Model)
	if !strings.Contains(model.View(), "PAUSED") {
		t.Errorf("expected view to show PAUSED")
	}

	// 5. Test Copy as cURL key 'y'
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	model = updated.(tui.Model)
	if !strings.Contains(model.View(), "Copied cURL") {
		t.Errorf("expected status message 'Copied cURL'")
	}
}

func TestBadges(t *testing.T) {
	b200 := tui.StatusBadge(200)
	if !strings.Contains(b200, "2xx") {
		t.Errorf("expected 2xx status badge")
	}

	b400 := tui.StatusBadge(404)
	if !strings.Contains(b400, "4xx") {
		t.Errorf("expected 4xx status badge")
	}

	b500 := tui.StatusBadge(502)
	if !strings.Contains(b500, "5xx") {
		t.Errorf("expected 5xx status badge")
	}
}

func TestANSISanitization(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe(5)
	defer unsub()

	model := tui.NewModel("test-tunnel", "https://test.tunnel.com", "http://localhost:8080", ch, nil)

	// Malicious payload containing terminal screen clear and cursor moves
	maliciousPath := "/webhook/\x1b[2J\x1b[Hevil"
	maliciousBody := []byte("{\x1b[31m\"danger\"\x1b[0m: \"\x1b[1;1Hhijack\"}")

	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   maliciousPath,
		Body:   maliciousBody,
	})

	updated, _ := model.Update(evt)
	rendered := updated.(tui.Model).View()

	// Ensure ANSI escapes are stripped
	if strings.Contains(rendered, "\x1b[2J") || strings.Contains(rendered, "\x1b[H") {
		t.Errorf("rendered view contains raw ANSI escape sequences!")
	}
	if !strings.Contains(rendered, "evil") {
		t.Errorf("sanitization stripped readable text")
	}
}

func TestTUITabToggle(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe(5)
	defer unsub()

	model := tui.NewModel("test-tunnel", "https://test.tunnel.com", "http://localhost:8080", ch, nil)

	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/api/pay",
		Body:   []byte(`{"status":"paid"}`),
	})
	evt.Response = inspector.HTTPResponse{
		StatusCode: 200,
		Headers: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: []byte(`{"result":"ok"}`),
	}

	updated, _ := model.Update(evt)
	model = updated.(tui.Model)

	// Default tab is Request
	viewReq := model.View()
	if !strings.Contains(viewReq, "[1] Request") || !strings.Contains(viewReq, "Headers:") {
		t.Errorf("expected Request view")
	}

	// Press Tab to switch to Response tab
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = updated.(tui.Model)

	viewResp := model.View()
	if !strings.Contains(viewResp, "200 OK") || !strings.Contains(viewResp, "Response Body:") {
		t.Errorf("expected Response view, got:\n%s", viewResp)
	}

	// Press Tab again to toggle back
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = updated.(tui.Model)
	if !strings.Contains(model.View(), "Payload:") {
		t.Errorf("expected toggle back to Request tab")
	}
}

func TestTUIFiltering(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe(5)
	defer unsub()

	model := tui.NewModel("test-tunnel", "https://test.tunnel.com", "http://localhost:8080", ch, nil)

	// Event 1: stripe
	evt1 := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/stripe",
		Body:   []byte(`{}`),
	})
	evt1.Provider = "stripe"
	evt1.Response.StatusCode = 200

	// Event 2: github
	evt2 := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/github",
		Body:   []byte(`{}`),
	})
	evt2.Provider = "github"
	evt2.Response.StatusCode = 200

	updated, _ := model.Update(evt1)
	model = updated.(tui.Model)
	updated, _ = model.Update(evt2)
	model = updated.(tui.Model)

	// Enter filter mode with '/'
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = updated.(tui.Model)

	// Type "stripe"
	for _, r := range "stripe" {
		updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = updated.(tui.Model)
	}

	// Press Enter to confirm filter
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(tui.Model)

	filteredView := model.View()
	if !strings.Contains(filteredView, "stripe") {
		t.Errorf("expected filtered view to contain stripe")
	}
	if strings.Contains(filteredView, "github") {
		t.Errorf("expected filtered view to NOT contain github")
	}

	// Press Esc to clear filter
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = updated.(tui.Model)
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyEsc})
	model = updated.(tui.Model)

	clearedView := model.View()
	if !strings.Contains(clearedView, "github") {
		t.Errorf("expected cleared filter to show github again")
	}
}

func TestTUIScrolling(t *testing.T) {
	bus := inspector.NewEventBus()
	defer bus.Close()
	ch, unsub := bus.Subscribe(5)
	defer unsub()

	model := tui.NewModel("test-tunnel", "https://test.tunnel.com", "http://localhost:8080", ch, nil)

	evt := inspector.NewWebhookEvent("test-tunnel", inspector.HTTPRequest{
		Method: "POST",
		Path:   "/webhooks/large",
		Body:   []byte("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10"),
	})
	updated, _ := model.Update(evt)
	model = updated.(tui.Model)

	// Press PgDn
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	model = updated.(tui.Model)

	// Press PgUp
	updated, _ = model.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	model = updated.(tui.Model)

	if !strings.Contains(model.View(), "large") {
		t.Errorf("expected view to remain valid after scrolling")
	}
}
