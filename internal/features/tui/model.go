package tui

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

type replayResultMsg string

// Model maintains the state for the terminal interactive Webhook Studio.
type Model struct {
	TunnelID    string
	PublicURL   string
	LocalTarget string
	WebURL      string

	events    []*inspector.WebhookEvent
	cursor    int
	paused    bool
	statusMsg string

	eventChan <-chan *inspector.WebhookEvent
	replaySvc *replay.Service

	width  int
	height int
}

// NewModel instantiates the TUI model.
func NewModel(tunnelID, publicURL, localTarget string, eventChan <-chan *inspector.WebhookEvent, replaySvc *replay.Service, webURL ...string) Model {
	wURL := ""
	if len(webURL) > 0 {
		wURL = webURL[0]
	}
	return Model{
		TunnelID:    tunnelID,
		PublicURL:   publicURL,
		LocalTarget: localTarget,
		WebURL:      wURL,
		events:      make([]*inspector.WebhookEvent, 0),
		cursor:      0,
		statusMsg:   "Ready. Waiting for incoming webhooks...",
		eventChan:   eventChan,
		replaySvc:   replaySvc,
		width:       100,
		height:      24,
	}
}

func waitForEvent(ch <-chan *inspector.WebhookEvent) tea.Cmd {
	return func() tea.Msg {
		if ch == nil {
			return nil
		}
		evt, ok := <-ch
		if !ok {
			return nil
		}
		return evt
	}
}

// Init triggers initial commands upon TUI launch.
func (m Model) Init() tea.Cmd {
	return waitForEvent(m.eventChan)
}

// Update processes incoming keypresses and asynchronous event messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case *inspector.WebhookEvent:
		if msg != nil {
			// If inspecting a specific item, adjust cursor to prevent jumping
			if m.cursor > 0 {
				m.cursor++
			}
			m.events = append([]*inspector.WebhookEvent{msg}, m.events...)
			if len(m.events) > 500 {
				m.events = m.events[:500]
			}
			if !m.paused {
				m.statusMsg = fmt.Sprintf("Received: %s %s (%s)", msg.Request.Method, sanitizeTerminalString(msg.Request.Path), msg.Provider)
			}
		}
		return m, waitForEvent(m.eventChan)

	case replayResultMsg:
		m.statusMsg = string(msg)
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit

		case " ":
			m.paused = !m.paused
			if m.paused {
				m.statusMsg = "⏸️  Live feed PAUSED. (Press Space to resume)"
			} else {
				m.statusMsg = "▶️  Live feed RESUMED."
			}

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.events)-1 {
				m.cursor++
			}

		case "c":
			m.events = nil
			m.cursor = 0
			m.statusMsg = "Cleared event history."

		case "y":
			if len(m.events) > 0 && m.cursor < len(m.events) {
				selected := m.events[m.cursor]
				curlCmd := selected.ToCURL(m.LocalTarget)
				// Copy to terminal clipboard via ANSI OSC 52
				fmt.Printf("\x1b]52;c;%s\x07", base64.StdEncoding.EncodeToString([]byte(curlCmd)))
				m.statusMsg = fmt.Sprintf("📋 Copied cURL for %s to clipboard!", selected.ID)
			}

		case "r":
			if len(m.events) > 0 && m.cursor < len(m.events) && m.replaySvc != nil {
				target := m.events[m.cursor]
				m.statusMsg = fmt.Sprintf("Replaying %s...", target.ID)
				return m, func() tea.Msg {
					replayed, err := m.replaySvc.Replay(context.Background(), target, replay.MutationOptions{})
					if err != nil {
						return replayResultMsg(fmt.Sprintf("Replay error: %v", err))
					}
					return replayResultMsg(fmt.Sprintf("Replayed %s -> Status %d (%dms)", target.ID, replayed.Response.StatusCode, replayed.DurationMs))
				}
			}
		}
	}

	return m, nil
}

// View renders the dual-pane terminal layout.
func (m Model) View() string {
	var b strings.Builder

	// 1. Header Banner
	banner := HeaderStyle.Render(" AUDIENCESHIFT STUDIO ") + " " +
		SubheaderStyle.Render("Public: ") + MethodStyle.Render(m.PublicURL) + "  " +
		SubheaderStyle.Render("Local: ") + SubheaderStyle.Render(m.LocalTarget)
	if m.WebURL != "" {
		banner += "  " + SubheaderStyle.Render("Web: ") + MethodStyle.Render(m.WebURL)
	}
	b.WriteString(banner + "\n\n")

	// Split pane dimensions
	leftWidth := m.width * 40 / 100
	rightWidth := m.width - leftWidth - 6
	if leftWidth < 30 {
		leftWidth = 30
	}
	if rightWidth < 40 {
		rightWidth = 40
	}
	paneHeight := m.height - 7
	if paneHeight < 8 {
		paneHeight = 8
	}

	// 2. Left Pane: Event List
	var leftContent strings.Builder
	if len(m.events) == 0 {
		leftContent.WriteString("\n  " + SubheaderStyle.Render("No webhooks captured yet.") + "\n")
		leftContent.WriteString("  " + SubheaderStyle.Render("Waiting for external traffic...") + "\n")
	} else {
		for i, evt := range m.events {
			if i >= paneHeight-1 {
				break
			}
			statusStr := fmt.Sprintf("%d", evt.Response.StatusCode)
			if evt.Response.StatusCode == 0 {
				statusStr = "..."
			}

			line := fmt.Sprintf("%s %-4s %-16s %s",
				statusStr,
				evt.Request.Method,
				truncate(sanitizeTerminalString(evt.Request.Path), 16),
				ProviderBadge(evt.Provider),
			)

			if i == m.cursor {
				leftContent.WriteString(SelectedItemStyle.Render("> "+line) + "\n")
			} else {
				leftContent.WriteString("  " + line + "\n")
			}
		}
	}

	// 3. Right Pane: Detail View of Selected Event
	var rightContent strings.Builder
	if len(m.events) > 0 && m.cursor < len(m.events) {
		selected := m.events[m.cursor]
		rightContent.WriteString(fmt.Sprintf("%s %s\n", MethodStyle.Render(selected.Request.Method), sanitizeTerminalString(selected.Request.Path)))
		rightContent.WriteString(fmt.Sprintf("%s | Time: %s | Latency: %dms\n",
			ProviderBadge(selected.Provider),
			selected.Timestamp.Format("15:04:05"),
			selected.DurationMs,
		))
		if selected.ReplayedFromID != "" {
			rightContent.WriteString(SubheaderStyle.Render("Replayed from: "+selected.ReplayedFromID) + "\n")
		}
		rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")

		// Headers snippet (using masked headers to prevent secret leaks)
		rightContent.WriteString(MethodStyle.Render("Headers:") + "\n")
		headerCount := 0
		headersToDisplay := selected.Request.MaskedHeaders()
		if headersToDisplay == nil {
			headersToDisplay = selected.Request.Headers
		}
		for k, v := range headersToDisplay {
			if headerCount < 4 {
				kClean := sanitizeTerminalString(k)
				vClean := sanitizeTerminalString(strings.Join(v, ", "))
				rightContent.WriteString(fmt.Sprintf("  %s: %s\n", SubheaderStyle.Render(kClean), truncate(vClean, rightWidth-len(kClean)-6)))
				headerCount++
			}
		}

		rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")
		rightContent.WriteString(MethodStyle.Render("Payload:") + "\n")
		prettyBody := selected.Request.PrettyJSONBody()
		bodyLines := strings.Split(prettyBody, "\n")
		for idx, l := range bodyLines {
			if idx > paneHeight-12 {
				rightContent.WriteString(SubheaderStyle.Render(fmt.Sprintf("... (%d more lines)", len(bodyLines)-idx)) + "\n")
				break
			}
			rightContent.WriteString("  " + truncate(sanitizeTerminalString(l), rightWidth-4) + "\n")
		}
	} else {
		rightContent.WriteString("\n  " + SubheaderStyle.Render("Select a webhook on the left to inspect headers and body.") + "\n")
	}

	leftPane := PaneStyle.Width(leftWidth).Height(paneHeight).Render(leftContent.String())
	rightPane := PaneStyle.Width(rightWidth).Height(paneHeight).Render(rightContent.String())

	// Join panes side by side
	leftLines := strings.Split(leftPane, "\n")
	rightLines := strings.Split(rightPane, "\n")
	maxLines := len(leftLines)
	if len(rightLines) > maxLines {
		maxLines = len(rightLines)
	}

	for i := 0; i < maxLines; i++ {
		l := ""
		r := ""
		if i < len(leftLines) {
			l = leftLines[i]
		}
		if i < len(rightLines) {
			r = rightLines[i]
		}
		b.WriteString(l + " " + r + "\n")
	}

	// 4. Footer & Help
	footer := fmt.Sprintf("[%s] %s | %s %s  %s %s  %s %s  %s %s  %s %s  %s %s",
		m.TunnelID,
		m.statusMsg,
		HelpKeyStyle.Render("↑/↓:"), HelpDescStyle.Render("Nav"),
		HelpKeyStyle.Render("Space:"), HelpDescStyle.Render("Pause"),
		HelpKeyStyle.Render("r:"), HelpDescStyle.Render("Replay"),
		HelpKeyStyle.Render("y:"), HelpDescStyle.Render("Copy cURL"),
		HelpKeyStyle.Render("c:"), HelpDescStyle.Render("Clear"),
		HelpKeyStyle.Render("q:"), HelpDescStyle.Render("Quit"),
	)
	b.WriteString("\n" + footer)

	return b.String()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07`)

// sanitizeTerminalString strips ANSI escape codes and dangerous control characters to prevent terminal corruption.
func sanitizeTerminalString(s string) string {
	cleaned := ansiEscapeRegex.ReplaceAllString(s, "")
	var b strings.Builder
	for _, r := range cleaned {
		if r == '\t' || r == '\n' || r == '\r' || (r >= 32 && r != 127) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
