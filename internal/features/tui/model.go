package tui

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

type replayResultMsg string

// TabMode designates the active inspection tab on the right pane.
type TabMode int

const (
	TabRequest TabMode = iota
	TabResponse
)

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

	// Navigation & Inspection Enhancements
	activeTab    TabMode
	scrollOffset int
	filtering    bool
	filterText   string

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
		TunnelID:     tunnelID,
		PublicURL:    publicURL,
		LocalTarget:  localTarget,
		WebURL:       wURL,
		events:       make([]*inspector.WebhookEvent, 0),
		cursor:       0,
		statusMsg:    "Ready. Waiting for incoming webhooks...",
		activeTab:    TabRequest,
		scrollOffset: 0,
		filtering:    false,
		filterText:   "",
		eventChan:    eventChan,
		replaySvc:    replaySvc,
		width:        100,
		height:       24,
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

func (m Model) getFilteredEvents() []*inspector.WebhookEvent {
	if m.filterText == "" {
		return m.events
	}
	query := strings.ToLower(m.filterText)
	var filtered []*inspector.WebhookEvent
	for _, evt := range m.events {
		if strings.Contains(strings.ToLower(evt.Request.Path), query) ||
			strings.Contains(strings.ToLower(evt.Provider), query) ||
			strings.Contains(strings.ToLower(evt.Request.Method), query) ||
			strings.Contains(fmt.Sprintf("%d", evt.Response.StatusCode), query) {
			filtered = append(filtered, evt)
		}
	}
	return filtered
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
		// 1. If in interactive filter input mode, intercept typing keys
		if m.filtering {
			switch msg.String() {
			case "ctrl+c":
				return m, tea.Quit
			case "esc":
				m.filtering = false
				m.filterText = ""
				m.cursor = 0
				m.scrollOffset = 0
				m.statusMsg = "Filter cleared."
				return m, nil
			case "enter":
				m.filtering = false
				m.cursor = 0
				m.scrollOffset = 0
				if m.filterText != "" {
					m.statusMsg = fmt.Sprintf("Applied filter: %q (press / or Esc to clear)", m.filterText)
				}
				return m, nil
			case "backspace":
				if len(m.filterText) > 0 {
					m.filterText = m.filterText[:len(m.filterText)-1]
					m.cursor = 0
				}
				return m, nil
			default:
				// Append valid runes
				if len(msg.Runes) > 0 {
					m.filterText += string(msg.Runes)
					m.cursor = 0
				}
				return m, nil
			}
		}

		// 2. Normal Command Keybindings
		filtered := m.getFilteredEvents()
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit

		case "tab", "t":
			if m.activeTab == TabRequest {
				m.activeTab = TabResponse
			} else {
				m.activeTab = TabRequest
			}
			m.scrollOffset = 0
			return m, nil

		case "/":
			m.filtering = true
			m.statusMsg = "Type query to filter webhooks (Enter to apply, Esc to cancel)..."
			return m, nil

		case "pgdown", "ctrl+d":
			m.scrollOffset += 6
			return m, nil

		case "pgup", "ctrl+u":
			m.scrollOffset -= 6
			if m.scrollOffset < 0 {
				m.scrollOffset = 0
			}
			return m, nil

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
				m.scrollOffset = 0
			}

		case "down", "j":
			if m.cursor < len(filtered)-1 {
				m.cursor++
				m.scrollOffset = 0
			}

		case "c":
			if m.filterText != "" {
				m.filterText = ""
				m.cursor = 0
				m.statusMsg = "Filter cleared."
			} else {
				m.events = nil
				m.cursor = 0
				m.scrollOffset = 0
				m.statusMsg = "Cleared event history."
			}

		case "y":
			if len(filtered) > 0 && m.cursor < len(filtered) {
				selected := filtered[m.cursor]
				curlCmd := selected.ToCURL(m.LocalTarget)
				// Copy to terminal clipboard via ANSI OSC 52
				fmt.Printf("\x1b]52;c;%s\x07", base64.StdEncoding.EncodeToString([]byte(curlCmd)))
				m.statusMsg = fmt.Sprintf("📋 Copied cURL for %s to clipboard!", selected.ID)
			}

		case "r":
			if len(filtered) > 0 && m.cursor < len(filtered) && m.replaySvc != nil {
				target := filtered[m.cursor]
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
	leftWidth := m.width * 38 / 100
	rightWidth := m.width - leftWidth - 6
	if leftWidth < 28 {
		leftWidth = 28
	}
	if rightWidth < 40 {
		rightWidth = 40
	}
	paneHeight := m.height - 7
	if paneHeight < 8 {
		paneHeight = 8
	}

	filtered := m.getFilteredEvents()
	if m.cursor >= len(filtered) && len(filtered) > 0 {
		m.cursor = len(filtered) - 1
	}

	// 2. Left Pane: Event List
	var leftContent strings.Builder
	if m.filterText != "" {
		leftContent.WriteString(FilterPromptStyle.Render("Filter: ") + FilterTextStyle.Render(m.filterText) + "\n")
		leftContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", leftWidth-2)) + "\n")
	}

	if len(filtered) == 0 {
		if m.filterText != "" {
			leftContent.WriteString("\n  " + SubheaderStyle.Render("No events match filter.") + "\n")
			leftContent.WriteString("  " + SubheaderStyle.Render("Press Esc or / to clear.") + "\n")
		} else {
			leftContent.WriteString("\n  " + SubheaderStyle.Render("No webhooks captured yet.") + "\n")
			leftContent.WriteString("  " + SubheaderStyle.Render("Waiting for external traffic...") + "\n")
		}
	} else {
		for i, evt := range filtered {
			if i >= paneHeight-1 {
				break
			}
			statusStr := fmt.Sprintf("%d", evt.Response.StatusCode)
			if evt.Response.StatusCode == 0 {
				statusStr = "..."
			}

			line := fmt.Sprintf("%s %-4s %-14s %s",
				statusStr,
				evt.Request.Method,
				truncate(sanitizeTerminalString(evt.Request.Path), 14),
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
	if len(filtered) > 0 && m.cursor < len(filtered) {
		selected := filtered[m.cursor]

		// Tab Switcher Header Bar
		reqTab := InactiveTabStyle.Render(" [1] Request ")
		respTab := InactiveTabStyle.Render(" [2] Response ")
		if m.activeTab == TabRequest {
			reqTab = ActiveTabStyle.Render(" [1] Request ")
		} else {
			respTab = ActiveTabStyle.Render(" [2] Response ")
		}

		tabBar := reqTab + " " + respTab + "  " + SubheaderStyle.Render("(Tab to toggle)")
		rightContent.WriteString(tabBar + "\n")
		rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")

		if m.activeTab == TabRequest {
			// Tab 1: Request View
			rightContent.WriteString(fmt.Sprintf("%s %s\n", MethodStyle.Render(selected.Request.Method), sanitizeTerminalString(selected.Request.Path)))
			rightContent.WriteString(fmt.Sprintf("%s | Time: %s | Latency: %dms\n",
				ProviderBadge(selected.Provider),
				selected.Timestamp.Format("15:04:05"),
				selected.DurationMs,
			))
			if selected.ReplayedFromID != "" {
				rightContent.WriteString(SubheaderStyle.Render("⚡ Replayed from: "+selected.ReplayedFromID) + "\n")
			}
			rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")

			// Headers section
			rightContent.WriteString(MethodStyle.Render("Headers:") + "\n")
			headersToDisplay := selected.Request.MaskedHeaders()
			if headersToDisplay == nil {
				headersToDisplay = selected.Request.Headers
			}
			hCount := 0
			for k, v := range headersToDisplay {
				if hCount < 6 {
					kClean := sanitizeTerminalString(k)
					vClean := sanitizeTerminalString(strings.Join(v, ", "))
					rightContent.WriteString(fmt.Sprintf("  %s: %s\n", SubheaderStyle.Render(kClean), truncate(vClean, rightWidth-len(kClean)-6)))
					hCount++
				}
			}
			if len(headersToDisplay) > 6 {
				rightContent.WriteString(SubheaderStyle.Render(fmt.Sprintf("  ... and %d more headers", len(headersToDisplay)-6)) + "\n")
			}

			rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")
			rightContent.WriteString(MethodStyle.Render("Payload:") + "\n")

			prettyBody := selected.Request.PrettyJSONBody()
			bodyLines := strings.Split(prettyBody, "\n")
			startLine := m.scrollOffset
			if startLine >= len(bodyLines) {
				startLine = 0
			}

			renderedCount := 0
			for idx := startLine; idx < len(bodyLines); idx++ {
				if renderedCount > paneHeight-14 {
					rightContent.WriteString(SubheaderStyle.Render(fmt.Sprintf("... (%d more lines - PgDn to scroll)", len(bodyLines)-idx)) + "\n")
					break
				}
				rightContent.WriteString("  " + truncate(sanitizeTerminalString(bodyLines[idx]), rightWidth-4) + "\n")
				renderedCount++
			}

		} else {
			// Tab 2: Response View
			statusStr := fmt.Sprintf("%d %s", selected.Response.StatusCode, http.StatusText(selected.Response.StatusCode))
			if selected.Response.StatusCode == 0 {
				statusStr = "0 Waiting / In Flight"
			}
			rightContent.WriteString(fmt.Sprintf("Status: %s | Latency: %dms\n", StatusBadge(selected.Response.StatusCode)+" "+statusStr, selected.DurationMs))
			rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")

			// Error notification if present
			if selected.Response.Error != "" {
				rightContent.WriteString(StatusErrorStyle.Render("Error / Gateway Failure:") + "\n")
				rightContent.WriteString(fmt.Sprintf("  %s\n", sanitizeTerminalString(selected.Response.Error)))
				rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")
			}

			// Response Headers
			if len(selected.Response.Headers) > 0 {
				rightContent.WriteString(MethodStyle.Render("Response Headers:") + "\n")
				hCount := 0
				for k, v := range selected.Response.Headers {
					if hCount < 5 {
						kClean := sanitizeTerminalString(k)
						vClean := sanitizeTerminalString(strings.Join(v, ", "))
						rightContent.WriteString(fmt.Sprintf("  %s: %s\n", SubheaderStyle.Render(kClean), truncate(vClean, rightWidth-len(kClean)-6)))
						hCount++
					}
				}
				rightContent.WriteString(SubheaderStyle.Render(strings.Repeat("─", rightWidth-2)) + "\n")
			}

			// Response Body
			rightContent.WriteString(MethodStyle.Render("Response Body:") + "\n")
			if len(selected.Response.Body) == 0 {
				rightContent.WriteString("  " + SubheaderStyle.Render("(empty response body)") + "\n")
			} else if !utf8.Valid(selected.Response.Body) {
				rightContent.WriteString(fmt.Sprintf("  %s\n", SubheaderStyle.Render(fmt.Sprintf("[Binary Data: %d bytes]", len(selected.Response.Body)))))
			} else {
				// Attempt to format JSON response
				var prettyResp string
				var prettyJSON bytes.Buffer
				if err := json.Indent(&prettyJSON, selected.Response.Body, "", "  "); err == nil {
					prettyResp = prettyJSON.String()
				} else {
					prettyResp = string(selected.Response.Body)
				}

				respLines := strings.Split(prettyResp, "\n")
				startLine := m.scrollOffset
				if startLine >= len(respLines) {
					startLine = 0
				}
				renderedCount := 0
				for idx := startLine; idx < len(respLines); idx++ {
					if renderedCount > paneHeight-14 {
						rightContent.WriteString(SubheaderStyle.Render(fmt.Sprintf("... (%d more lines - PgDn to scroll)", len(respLines)-idx)) + "\n")
						break
					}
					rightContent.WriteString("  " + truncate(sanitizeTerminalString(respLines[idx]), rightWidth-4) + "\n")
					renderedCount++
				}
			}
		}

	} else {
		rightContent.WriteString("\n  " + SubheaderStyle.Render("Select a webhook on the left to inspect headers, payload, and response.") + "\n")
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
	var footer string
	if m.filtering {
		footer = fmt.Sprintf("🔎 %s %s%s",
			FilterPromptStyle.Render("Filter Webhooks:"),
			FilterTextStyle.Render(m.filterText),
			HelpDescStyle.Render("█  (Enter to apply, Esc to cancel)"),
		)
	} else {
		footer = fmt.Sprintf("[%s] %s | %s %s  %s %s  %s %s  %s %s  %s %s  %s %s  %s %s",
			m.TunnelID,
			m.statusMsg,
			HelpKeyStyle.Render("Tab:"), HelpDescStyle.Render("Req/Resp"),
			HelpKeyStyle.Render("↑/↓:"), HelpDescStyle.Render("Nav"),
			HelpKeyStyle.Render("PgUp/Dn:"), HelpDescStyle.Render("Scroll"),
			HelpKeyStyle.Render("/:"), HelpDescStyle.Render("Filter"),
			HelpKeyStyle.Render("r:"), HelpDescStyle.Render("Replay"),
			HelpKeyStyle.Render("y:"), HelpDescStyle.Render("cURL"),
			HelpKeyStyle.Render("q:"), HelpDescStyle.Render("Quit"),
		)
	}
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
