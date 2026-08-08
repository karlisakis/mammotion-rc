# Luba Remote — design system

Shared by every page: `static/theme.css` (tokens + primitives) and
`static/theme.js` (light/dark/system switching). Page-specific rules live in
that page's own file. **Never edit `theme.css` to fix one page.**

## Direction

"Instrument" — a precision control surface, not a dashboard template.

- Near-neutral graphite, deliberately **not** blue-slate. The old
  `#0b0f14` + `#2dd47a` pairing is the generic-AI-dark-app look; avoid it.
- Warm paper (`#F6F6F4`) in light mode rather than pure white — reads
  considered rather than default.
- **One** signal colour (machine lime) used only where something is live,
  primary, or focused. If everything is accented, nothing is.
- Hairline borders and flat surfaces. Shadows only on genuine overlays
  (toasts, sheets) — never on static cards.
- Tabular numerals everywhere, so telemetry doesn't jitter as digits change.
- Tight heading tracking (`-0.02em`). Uppercase micro-labels (`.u-label`)
  for section headers only, never body copy.

## Using it

```html
<head>
  <script src="/static/theme.js"></script>   <!-- FIRST, synchronous: no flash -->
  <link rel="stylesheet" href="/static/theme.css">
  <link rel="stylesheet" href="/static/style.css">  <!-- page styles, optional -->
</head>
```

`theme.js` must be synchronous and before the stylesheets, or the page paints
the OS theme first and visibly flips.

## Tokens

Colour: `--bg --surface --surface-2 --surface-3 --border --border-strong
--text --text-2 --text-3`, semantic `--danger --warn --info` each with a
`-soft` background variant.

The accent needs three tokens because a lime that sings on near-black is
illegible on white:

| Token | Use |
|---|---|
| `--accent` | fill (primary buttons, active segments, slider thumbs) |
| `--accent-ink` | text/icons **on** that fill |
| `--accent-fg` | accent as text/icon **on the page background** — darkened in light mode |
| `--accent-soft` | tinted background for accent chips |

Also: `--r-sm/md/lg/full`, `--shadow-1/2`, `--ease --dur-1 --dur-2`,
`--font --font-mono`, `--topbar-h --tabbar-h`.

## Primitives

`.card` `.card-pad` `.hr` · `.btn` (+ `.btn-primary` `.btn-danger`
`.btn-ghost` `.btn-icon`) · `.chip` (+ `.chip-accent/-danger/-warn/-info`) ·
`.dot` `.dot-pulse` · `.field` (inputs/selects) · `input[type=range]` ·
`.segmented` · `.topbar` `.tabbar` · `.toast-region` `.toast` · `.spinner`
`.skeleton` · `.u-label` `.u-dim` `.u-mono` `.visually-hidden`

Button busy state: set `aria-busy="true"` and wrap the icon in `.btn-ico` —
the icon hides and a spinner takes its place. Flash results with
`.flash-ok` / `.flash-fail` for ~600 ms.

## Non-negotiables

- **No external assets.** No webfonts, CDNs, icon packs or images. System
  fonts and inline SVG only — this app is used over a low-bandwidth Wi-Fi
  HaLow link. (`index.html`'s two existing CDN scripts, AgoraRTC and nipplejs,
  are the sole exception.)
- **Never** put `overflow-x: hidden` on both `html` and `body`. A non-visible
  `overflow-x` forces `overflow-y` to `auto`, creating nested scroll
  containers — this froze touch scrolling on iOS once already. `theme.css`
  sets `overflow-x: clip` on `body` alone; leave it that way.
- The only `touch-action: none` on any page is the joystick zone.
- Touch targets ≥44px. Inputs ≥16px font, or iOS zooms the page on focus.
- Respect `env(safe-area-inset-*)` on anything fixed to a screen edge.
- Light **and** dark must both be checked before shipping a page.
- `prefers-reduced-motion` is honoured globally in `theme.css`; don't
  reintroduce unconditional animation.

## Native-app feel

The app is installable (see `manifest.webmanifest`) and should behave like a
native app when launched from the home screen: fixed top bar, bottom tab bar
on phones, no horizontal scroll, no visible browser affordances, safe-area
aware, momentum scrolling inside panes rather than the page where it matters.

On wide screens the tab bar disappears and the full dashboard is laid out at
once — tabs are a phone affordance, not a desktop one.
