# AudienceShift Terminal TUI Demo Script (Charm VHS)
# Run with: vhs docs/demo.tape

Output docs/assets/demo.gif

Set FontSize 15
Set Width 1200
Set Height 720
Set Padding 24
Set Theme "Catppuccin Mocha"
Set WindowBar "Colorful"
Set FontFamily "Cascadia Code"
Set LetterSpacing 0
Set LineHeight 1.3

# 1. Launch AudienceShift Listen with Web Studio
Type "audienceshift listen 3000 --web"
Sleep 600ms
Enter
Sleep 2s

# 2. Inspect events as they stream in
Sleep 1.5s
Down
Sleep 1s
Down
Sleep 1.2s

# 3. Trigger 1-Key Replay (r)
Type "r"
Sleep 2s

# 4. Export as cURL to clipboard (y)
Type "y"
Sleep 2.5s

# 5. Pause feed (Space)
Type " "
Sleep 2s

# 6. Finish loop
Sleep 1s
