# Mammotion RC — Home Assistant add-on

Runs the [mammotion-rc](https://github.com/karlisakis/mammotion-rc) web control
server (joystick driving, live camera, status) on Home Assistant OS, talking to
your mower through the HC33 BLE proxy on your LAN.

## Prerequisites

- The HC33 is flashed and mounted on the mower (see the main repo README).
- A **secondary** Mammotion account exists and the mower is shared to it —
  never use your main account here.
- The HC33 and Home Assistant are on the same LAN. Give the HC33 a **DHCP
  reservation** in your router: the roster pins the proxy by IP, and a lease
  change silently breaks the connection until you re-run onboarding.

## Install

1. **Get the folder onto the box.** Install the *Samba share* (or *Advanced
   SSH & Web Terminal*) add-on, then copy this `mammotion_rc` folder into the
   `addons` share so it lives at `/addons/mammotion_rc`.
2. **Make it appear.** Settings → Add-ons → Add-on Store → **⋮** →
   **Check for updates**. A *Local add-ons* section appears with
   **Mammotion RC**.
3. **Install.** The first build takes a few minutes — it downloads the repo
   snapshot from GitHub and installs the Python dependencies.
4. **Configure.** On the *Configuration* tab set a **web password** (strongly
   recommended — without it anyone on the network can drive the mower).
5. **Start** the add-on. *Start on boot* and *Watchdog* should be on
   (the defaults).

## First run

Open `https://<your-ha-ip>:8443` (also linked via **Open Web UI**). Accept the
one-time self-signed-certificate warning. You land on onboarding:

1. Sign in with the **secondary** Mammotion account.
2. Power the mower on, then **Scan** — the server finds the HC33 on the LAN
   and pairs it with your cloud mower automatically (a proxy showing
   `bonded_name=none` hasn't seen the mower yet — power it on and re-scan).
3. **Save.** You're live.

## Remote access

Install the **Tailscale** add-on and reach the UI at
`https://<tailscale-ip-or-name>:8443` from anywhere. Prefer this over port
forwarding — nothing gets exposed to the public internet.

## Updating

Push changes to the repo's `main` branch, then open the add-on page and hit
**Rebuild**. The build re-downloads the repo snapshot; your configuration
(`mowers.toml`, `secrets.toml`, certificate) lives in the add-on's persistent
data volume and survives rebuilds, restarts, and HAOS updates.

## Notes & troubleshooting

- **One client per proxy.** Each HC33 accepts a single TCP connection — never
  run a second copy of the web server (e.g. on a NAS or laptop) against the
  same mower at the same time.
- **Camera needs HTTPS.** The video feed is WebRTC; browsers require a secure
  context. The add-on serves HTTPS with a self-signed cert out of the box.
- **HA box changed IP?** The cert's SANs bake in the old IP. Enable the
  `regenerate_cert` option, restart, then disable it again.
- **Changed the `port` option?** Also edit the `watchdog:` and `webui:` lines
  in `config.yaml` to the same port — they're static.
- **Onboarding finds no proxies?** Discovery is a UDP broadcast on port 9878;
  it requires the HC33 to be reachable on the same L2 network. You can always
  add a mower manually by typing the HC33's IP.
