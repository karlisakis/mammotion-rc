# Mammotion RC (Home Assistant add-on)

Runs the mammotion-rc web control server — joystick driving, live camera,
status — on Home Assistant OS, always on, talking to the mower through the
HC33 BLE proxy.

- **Install & usage:** see [DOCS.md](DOCS.md) (the add-on's *Documentation* tab).
- **Project:** https://github.com/karlisakis/mammotion-rc

Quick facts: builds locally from a GitHub snapshot of `main` (hit *Rebuild* to
update), `python:3.13-slim` base (PyMammotion needs Python ≥3.13), host
networking (UDP proxy discovery), state persisted in `/data`, HTTPS on `8443`
with an auto-generated self-signed certificate.
