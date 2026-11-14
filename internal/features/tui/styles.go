package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Brand colors
	ColorPrimary   = lipgloss.Color("#6366F1") // Indigo
	ColorSecondary = lipgloss.Color("#EC4899") // Pink
	ColorMuted     = lipgloss.Color("#64748B") // Slate
	ColorBgDark    = lipgloss.Color("#0F172A") // Deep slate
	ColorBorder    = lipgloss.Color("#334155") // Slate border

	// Status badge colors
	ColorSuccess = lipgloss.Color("#10B981") // Emerald
	ColorWarn    = lipgloss.Color("#F59E0B") // Amber
	ColorError   = lipgloss.Color("#EF4444") // Rose
	ColorInfo    = lipgloss.Color("#38BDF8") // Sky

	// Provider colors
	ColorStripe   = lipgloss.Color("#635BFF")
	ColorGitHub   = lipgloss.Color("#E2E8F0")
	ColorMidtrans = lipgloss.Color("#002554")
	ColorSlack    = lipgloss.Color("#4A154B")
	ColorShopify  = lipgloss.Color("#95BF47")
	ColorXendit   = lipgloss.Color("#1570EF")
	ColorGitLab   = lipgloss.Color("#FC6D26")
	ColorDiscord  = lipgloss.Color("#5865F2")
	ColorTwilio   = lipgloss.Color("#F22F46")

	// Styles
	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorPrimary).
			Padding(0, 1)

	SubheaderStyle = lipgloss.NewStyle().
			Foreground(ColorMuted)

	PaneStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorBorder).
			Padding(0, 1)

	SelectedItemStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(lipgloss.Color("#1E293B"))

	MethodStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorInfo)

	StatusSuccessStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorSuccess)

	StatusWarnStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorWarn)

	StatusErrorStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorError)

	ProviderBadgeStyle = lipgloss.NewStyle().
				Bold(true).
				Padding(0, 1).
				Foreground(lipgloss.Color("#FFFFFF"))

	HelpKeyStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorPrimary)

	HelpDescStyle = lipgloss.NewStyle().
			Foreground(ColorMuted)

	ActiveTabStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(ColorPrimary).
			Padding(0, 1)

	InactiveTabStyle = lipgloss.NewStyle().
			Foreground(ColorMuted).
			Background(lipgloss.Color("#1E293B")).
			Padding(0, 1)

	FilterPromptStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorWarn)

	FilterTextStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF"))
)

// ProviderBadge returns a styled badge for recognized webhook sources.
func ProviderBadge(provider string) string {
	switch provider {
	case "stripe":
		return ProviderBadgeStyle.Copy().Background(ColorStripe).Render("STRIPE")
	case "github":
		return ProviderBadgeStyle.Copy().Background(lipgloss.Color("#24292E")).Render("GITHUB")
	case "slack":
		return ProviderBadgeStyle.Copy().Background(ColorSlack).Render("SLACK")
	case "shopify":
		return ProviderBadgeStyle.Copy().Background(ColorShopify).Render("SHOPIFY")
	case "midtrans":
		return ProviderBadgeStyle.Copy().Background(ColorMidtrans).Render("MIDTRANS")
	case "xendit":
		return ProviderBadgeStyle.Copy().Background(ColorXendit).Render("XENDIT")
	case "gitlab":
		return ProviderBadgeStyle.Copy().Background(ColorGitLab).Render("GITLAB")
	case "discord":
		return ProviderBadgeStyle.Copy().Background(ColorDiscord).Render("DISCORD")
	case "twilio":
		return ProviderBadgeStyle.Copy().Background(ColorTwilio).Render("TWILIO")
	default:
		return ProviderBadgeStyle.Copy().Background(ColorMuted).Render("GENERIC")
	}
}

// StatusBadge returns a colored HTTP status string.
func StatusBadge(code int) string {
	switch {
	case code >= 200 && code < 300:
		return StatusSuccessStyle.Render(lipgloss.NewStyle().Render(string(rune('0'+code/100)) + "xx"))
	case code >= 400 && code < 500:
		return StatusWarnStyle.Render(string(rune('0'+code/100)) + "xx")
	default:
		return StatusErrorStyle.Render(string(rune('0'+code/100)) + "xx")
	}
}
