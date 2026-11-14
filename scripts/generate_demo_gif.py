"""
AudienceShift Animated Demo GIF Generator
Generates a high-resolution, production-quality terminal TUI demo GIF.
"""

import os
from PIL import Image, ImageDraw, ImageFont
import imageio

OUTPUT_DIR = "docs/assets"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "demo.gif")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dimensions
WIDTH = 1100
HEIGHT = 680

# Colors (Hex -> RGB)
def hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return tuple(int(hex_str[i:i+2], 16) for i in (0, 2, 4))

BG_DARK      = hex_to_rgb("#0B0F19")
BG_CARD      = hex_to_rgb("#111827")
BG_SELECT    = hex_to_rgb("#1E293B")
BORDER_COLOR = hex_to_rgb("#334155")
TEXT_MAIN    = hex_to_rgb("#F8FAFC")
TEXT_MUTED   = hex_to_rgb("#64748B")
TEXT_DIM     = hex_to_rgb("#94A3B8")

COLOR_PRIMARY = hex_to_rgb("#6366F1")  # Indigo
COLOR_SUCCESS = hex_to_rgb("#10B981")  # Emerald
COLOR_WARN    = hex_to_rgb("#F59E0B")  # Amber
COLOR_ERROR   = hex_to_rgb("#EF4444")  # Rose
COLOR_INFO    = hex_to_rgb("#38BDF8")  # Sky

BADGE_STRIPE   = hex_to_rgb("#635BFF")
BADGE_XENDIT   = hex_to_rgb("#1570EF")
BADGE_MIDTRANS = hex_to_rgb("#002554")
BADGE_GITHUB   = hex_to_rgb("#24292E")

# Fonts
FONT_REG_PATH = "C:/Windows/Fonts/consola.ttf"
FONT_BOLD_PATH = "C:/Windows/Fonts/consolab.ttf"

font_title = ImageFont.truetype(FONT_BOLD_PATH, 14)
font_banner = ImageFont.truetype(FONT_BOLD_PATH, 13)
font_body = ImageFont.truetype(FONT_REG_PATH, 13)
font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)
font_small = ImageFont.truetype(FONT_REG_PATH, 11)
font_footer = ImageFont.truetype(FONT_BOLD_PATH, 12)

def draw_pill(draw, x, y, text, font, bg_color, text_color=(255, 255, 255), pad_x=6, pad_y=2, radius=4):
    bbox = font.getbbox(text)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    w = tw + pad_x * 2
    h = th + pad_y * 2
    draw.rounded_rectangle([x, y, x + w, y + h], radius=radius, fill=bg_color)
    draw.text((x + pad_x, y + pad_y - bbox[1]), text, font=font, fill=text_color)
    return w

def create_base_frame(selected_idx=0, status_msg="Ready. Waiting for incoming webhooks...", events_shown=4, replay_pulse=False):
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_DARK)
    draw = ImageDraw.Draw(img)

    # 1. Window Title Bar (macOS / Linux terminal style)
    draw.rectangle([0, 0, WIDTH, 36], fill=hex_to_rgb("#161E2E"))
    draw.line([0, 36, WIDTH, 36], fill=BORDER_COLOR, width=1)

    # Window traffic lights
    draw.ellipse([14, 12, 26, 24], fill=hex_to_rgb("#EF4444"))
    draw.ellipse([34, 12, 46, 24], fill=hex_to_rgb("#F59E0B"))
    draw.ellipse([54, 12, 66, 24], fill=hex_to_rgb("#10B981"))

    # Window Title
    title = "audienceshift listen 3000 --web  (PID 14092) — 124x36"
    draw.text((WIDTH // 2 - 160, 10), title, font=font_title, fill=TEXT_MUTED)

    # 2. AudienceShift Header Banner
    banner_y = 48
    bx = 18
    w_pill = draw_pill(draw, bx, banner_y, " AUDIENCESHIFT STUDIO ", font_banner, COLOR_PRIMARY, (255, 255, 255), 8, 3)
    bx += w_pill + 14

    draw.text((bx, banner_y + 2), "Public: ", font=font_bold, fill=TEXT_MUTED)
    bx += 56
    draw.text((bx, banner_y + 2), "https://alice.tunnel.yourdomain.com", font=font_bold, fill=COLOR_INFO)
    bx += 280

    draw.text((bx, banner_y + 2), "Local: ", font=font_bold, fill=TEXT_MUTED)
    bx += 48
    draw.text((bx, banner_y + 2), "http://localhost:3000", font=font_body, fill=TEXT_DIM)
    bx += 175

    draw.text((bx, banner_y + 2), "Web: ", font=font_bold, fill=TEXT_MUTED)
    bx += 40
    draw.text((bx, banner_y + 2), "http://localhost:4040", font=font_bold, fill=COLOR_SUCCESS)

    # 3. Main Split-Pane
    top_y = 82
    bottom_y = HEIGHT - 46
    split_x = 440

    # Left Pane (Event List)
    draw.rounded_rectangle([14, top_y, split_x - 6, bottom_y], radius=6, outline=BORDER_COLOR, fill=BG_CARD)
    # Right Pane (Inspector)
    draw.rounded_rectangle([split_x + 6, top_y, WIDTH - 14, bottom_y], radius=6, outline=BORDER_COLOR, fill=BG_CARD)

    # Left Pane Header
    draw.text((26, top_y + 10), "STATUS  METHOD  PATH                SRC", font=font_bold, fill=TEXT_MUTED)
    draw.line([22, top_y + 30, split_x - 14, top_y + 30], fill=BORDER_COLOR, width=1)

    # All Webhook Events Data
    all_events = [
        {"status": 200, "method": "POST", "path": "/webhooks/stripe",   "src": "STRIPE",   "color": BADGE_STRIPE,   "id": "evt_stripe_102"},
        {"status": 200, "method": "POST", "path": "/api/v1/xendit",     "src": "XENDIT",   "color": BADGE_XENDIT,   "id": "evt_xnd_901"},
        {"status": 200, "method": "POST", "path": "/midtrans/callback", "src": "MIDTRANS", "color": BADGE_MIDTRANS, "id": "evt_mdt_405"},
        {"status": 200, "method": "POST", "path": "/github/push",       "src": "GITHUB",   "color": BADGE_GITHUB,   "id": "evt_gh_782"},
    ]

    # Render Events on Left Pane
    row_y = top_y + 38
    for i in range(min(events_shown, len(all_events))):
        evt = all_events[i]
        is_sel = (i == selected_idx)
        if is_sel:
            draw.rounded_rectangle([18, row_y - 2, split_x - 10, row_y + 26], radius=4, fill=BG_SELECT)
            draw.text((22, row_y + 4), "▶", font=font_bold, fill=COLOR_INFO)

        # Status badge (2xx emerald)
        draw_pill(draw, 40, row_y + 2, "2xx", font_small, COLOR_SUCCESS, (255, 255, 255), pad_x=4, pad_y=1)
        # Method
        draw.text((78, row_y + 4), evt["method"], font=font_bold, fill=COLOR_INFO)
        # Path
        draw.text((125, row_y + 4), evt["path"][:18], font=font_body, fill=TEXT_MAIN if is_sel else TEXT_DIM)
        # Provider badge
        draw_pill(draw, 340, row_y + 2, evt["src"], font_small, evt["color"], (255, 255, 255), pad_x=5, pad_y=1)

        row_y += 32

    # Right Pane (Selected Webhook Details)
    rx = split_x + 22
    ry = top_y + 14

    sel_evt = all_events[min(selected_idx, len(all_events)-1)]

    # Detail Title
    draw.text((rx, ry), f"{sel_evt['method']} {sel_evt['path']}", font=font_bold, fill=TEXT_MAIN)
    ry += 24

    # Meta
    draw_pill(draw, rx, ry, sel_evt["src"], font_small, sel_evt["color"], (255, 255, 255), 5, 1)
    meta_text = " | Time: 14:32:05 | Latency: 14ms | Target: http://localhost:3000"
    draw.text((rx + 70, ry + 1), meta_text, font=font_body, fill=TEXT_MUTED)
    ry += 24

    # Divider
    draw.line([rx, ry, WIDTH - 28, ry], fill=BORDER_COLOR, width=1)
    ry += 10

    # Headers Section (Masked!)
    draw.text((rx, ry), "Headers (Masked for Privacy):", font=font_bold, fill=COLOR_INFO)
    ry += 20

    if sel_evt["src"] == "XENDIT":
        headers = [
            ("Content-Type", "application/json; charset=utf-8"),
            ("X-Callback-Token", "xnd_live_****...9999"),
            ("User-Agent", "Xendit-Webhook/2.0"),
            ("X-Forwarded-Host", "alice.tunnel.yourdomain.com"),
        ]
        body_lines = [
            '{\n',
            '  "id": "xnd_inv_609a39f1c7d24a001a1c991e",\n',
            '  "external_id": "ORDER-10029",\n',
            '  "user_id": "608d4ff0959c9b001a6132aa",\n',
            '  "status": "PAID",\n',
            '  "amount": 150000,\n',
            '  "payment_method": "BCA",\n',
            '  "paid_at": "2026-11-14T14:32:04.000Z",\n',
            '  "payment_channel": "VIRTUAL_ACCOUNT"\n',
            '}\n'
        ]
    elif sel_evt["src"] == "STRIPE":
        headers = [
            ("Content-Type", "application/json"),
            ("Stripe-Signature", "t=1731580000,v1=sec_****...8819"),
            ("Authorization", "Bearer sk_test_****...456"),
            ("User-Agent", "Stripe/1.0 (+https://stripe.com/docs/webhooks)"),
        ]
        body_lines = [
            '{\n',
            '  "id": "evt_3MtwBwLkdIwHu7ix",\n',
            '  "object": "event",\n',
            '  "type": "payment_intent.succeeded",\n',
            '  "data": {\n',
            '    "object": {\n',
            '      "amount": 2000,\n',
            '      "currency": "usd",\n',
            '      "status": "succeeded"\n',
            '    }\n',
            '  }\n',
            '}\n'
        ]
    else:
        headers = [
            ("Content-Type", "application/json"),
            ("X-Signature-Key", "mdt_sec_****...1102"),
            ("User-Agent", "Veritrans/1.0"),
            ("Host", "localhost:3000"),
        ]
        body_lines = [
            '{\n',
            '  "transaction_id": "8488e5d0-9944-4f51",\n',
            '  "transaction_status": "settlement",\n',
            '  "gross_amount": "150000.00",\n',
            '  "signature_key": "9a38f...39b"\n',
            '}\n'
        ]

    for k, v in headers:
        draw.text((rx + 8, ry), f"  {k}:", font=font_bold, fill=TEXT_MUTED)
        draw.text((rx + 180, ry), v, font=font_body, fill=COLOR_WARN if "Token" in k or "Signature" in k or "Authorization" in k else TEXT_DIM)
        ry += 18

    ry += 8
    # Body Section
    draw.text((rx, ry), "Payload (Formatted JSON):", font=font_bold, fill=COLOR_INFO)
    ry += 20

    for line in body_lines:
        line_clean = line.strip('\n')
        color = TEXT_MAIN
        if ":" in line_clean:
            parts = line_clean.split(":", 1)
            draw.text((rx + 8, ry), parts[0] + ":", font=font_bold, fill=COLOR_INFO)
            draw.text((rx + 8 + font_bold.getlength(parts[0] + ": "), ry), parts[1], font=font_body, fill=COLOR_SUCCESS if '"PAID"' in line_clean or '"succeeded"' in line_clean else TEXT_MAIN)
        else:
            draw.text((rx + 8, ry), line_clean, font=font_body, fill=TEXT_MUTED)
        ry += 18

    # 4. Footer & Help Bar
    draw.line([0, HEIGHT - 42, WIDTH, HEIGHT - 42], fill=BORDER_COLOR, width=1)
    draw.rectangle([0, HEIGHT - 41, WIDTH, HEIGHT], fill=hex_to_rgb("#0F172A"))

    # Status Message
    status_color = COLOR_SUCCESS if "Replay" in status_msg or "Copied" in status_msg else (COLOR_WARN if "PAUSED" in status_msg else COLOR_INFO)
    draw.text((20, HEIGHT - 32), status_msg, font=font_footer, fill=status_color)

    # Hotkeys
    help_text = "[↑/↓] Navigate   [r] Replay   [y] Copy cURL   [Space] Pause   [c] Clear   [q] Quit"
    draw.text((WIDTH - 640, HEIGHT - 32), help_text, font=font_small, fill=TEXT_MUTED)

    return img

print("Generating animation frames...")
frames = []
durations = []

# Frame Sequence 1: Boot & Waiting
for _ in range(5):
    frames.append(create_base_frame(selected_idx=0, status_msg="Ready. Waiting for incoming webhooks...", events_shown=1))
    durations.append(0.3)

# Frame Sequence 2: 2nd and 3rd webhooks arrive
frames.append(create_base_frame(selected_idx=0, status_msg="Received: POST /webhooks/stripe (stripe)", events_shown=2))
durations.append(0.6)

frames.append(create_base_frame(selected_idx=0, status_msg="Received: POST /api/v1/xendit (xendit)", events_shown=3))
durations.append(0.8)

frames.append(create_base_frame(selected_idx=0, status_msg="Received: POST /midtrans/callback (midtrans)", events_shown=4))
durations.append(1.0)

# Frame Sequence 3: User navigates down with arrow key [↓] to select Xendit event
frames.append(create_base_frame(selected_idx=1, status_msg="Selected: evt_xnd_901 (xendit:invoice.paid)", events_shown=4))
durations.append(1.2)

# Frame Sequence 4: User presses 'r' (Instant Replay)
frames.append(create_base_frame(selected_idx=1, status_msg="Replaying evt_xnd_901 to http://localhost:3000...", events_shown=4))
durations.append(0.5)

frames.append(create_base_frame(selected_idx=1, status_msg="⚡ Replayed evt_xnd_901 -> Status 200 (14ms) [HMAC Recalculated]", events_shown=4))
durations.append(1.8)

# Frame Sequence 5: User presses 'y' (Copy as cURL via OSC 52)
frames.append(create_base_frame(selected_idx=1, status_msg="📋 Copied cURL command for evt_xnd_901 to clipboard!", events_shown=4))
durations.append(2.0)

# Frame Sequence 6: User presses Space (Pause feed)
frames.append(create_base_frame(selected_idx=1, status_msg="⏸️  Live feed PAUSED. (Press Space to resume)", events_shown=4))
durations.append(1.2)

# Save as optimized GIF using imageio
print(f"Writing animated GIF to {OUTPUT_FILE} ({len(frames)} frames)...")
imageio.mimsave(OUTPUT_FILE, frames, duration=durations, loop=0)

size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
print(f"Successfully generated {OUTPUT_FILE} ({size_mb:.2f} MB)")
