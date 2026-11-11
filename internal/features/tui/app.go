package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/pandusatrianura/AudienceShift/internal/features/inspector"
	"github.com/pandusatrianura/AudienceShift/internal/features/replay"
)

// App manages the Bubbletea TUI program lifecycle.
type App struct {
	prog *tea.Program
}

// Run starts the Bubbletea event loop.
func (a *App) Run() error {
	_, err := a.prog.Run()
	return err
}

// NewApp creates and configures the TUI program subscribed to the event bus.
func NewApp(tunnelID, publicURL, localTarget string, bus *inspector.EventBus, replaySvc *replay.Service, webURL string) (*App, func()) {
	eventChan, unsub := bus.Subscribe(500)
	m := NewModel(tunnelID, publicURL, localTarget, eventChan, replaySvc, webURL)
	prog := tea.NewProgram(m, tea.WithAltScreen())
	cleanup := func() {
		unsub()
	}
	return &App{prog: prog}, cleanup
}
