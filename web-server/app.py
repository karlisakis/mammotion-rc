"""Luba remote-control web server.

Browses to the page → drives the mower over WebSocket joystick + REST actions,
all routed through HC33ProxyTransport to the HC33 firmware over WiFi/HaLow.

Camera is optional and starts OFF.  Click "Start Camera" to:
  1. Lazily log into the Mammotion cloud (HTTP only — no MQTT, no device
     registration).
  2. Fetch a fresh Agora stream token.
  3. Send the explicit `device_agora_join_channel_with_position(1)` to the
     mower over BLE so it starts publishing.
  4. Return the token to the browser, which subscribes via Agora Web SDK.

Single-process, single-file.  Use uvicorn directly:

    uvicorn app:app --host 0.0.0.0 --port 8000 --reload

or just `python app.py` for the same with --reload disabled.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import sys
import time
from pathlib import Path

# Must run before any asyncio loop is created.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# MUST precede every pymammotion import: it sets the app-level OAuth/Aliyun
# constants as env defaults, and pymammotion.const reads os.environ at import
# time.  Imported only for that import-time side effect.
import mammotion_creds  # noqa: F401,E402  (side-effecting; keep before pymammotion)

from fastapi import Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from pymammotion.aliyun.cloud_gateway import CloudIOTGateway
from pymammotion.data.model.device import MowingDevice
from pymammotion.device.handle import DeviceHandle
from pymammotion.http.http import MammotionHTTP
from pymammotion.transport.base import TransportAvailability
from pymammotion.utility.device_type import DeviceType
from pymammotion.utility.movement import get_percent, transform_both_speeds

# Local module (ships with the web-server, not with PyMammotion): our TCP-over-
# HC33 transport that subclasses pymammotion's Transport.  Keeping it here lets
# us install a clean, unpatched upstream PyMammotion.
from hc33_proxy import HC33ProxyTransport

import persist
from discover import discover, poke_proxy

_LOGGER = logging.getLogger("luba_web")

# ── Mower configuration ──────────────────────────────────────────────────────
# The roster lives in mowers.toml (sibling to this file), normally written by
# the onboarding flow.  An empty/missing file means "not yet onboarded" — the
# server still starts, serves the onboarding page, and builds handles live once
# the user saves.  See persist.py for the schema and secrets handling.
#
# MOWERS is mutated in place (never rebound) so handlers holding the reference
# see live updates after onboarding/settings saves.
MOWERS: list[dict] = persist.load_mowers()

# Joystick → command mapping thresholds.  Inputs below DEAD_ZONE are treated
# as "centered" (= stop).  Forward/back wins over left/right when both axes
# are active (bang-bang cardinal motion for MVP — diagonal mixing later).
DEAD_ZONE = 0.15
# Optional ceiling on stick magnitude → command magnitude.  The mower expects
# the `linear` / `angular` arguments in [0.0, 1.0]; values > 1 produce huge
# internal speeds via `get_percent(value*100)`.  Keep <= 1.0.  Drop this below
# 1 if full deflection is too fast for comfort.
MAX_LINEAR  = 1.0
MAX_ANGULAR = 1.0

# ── Runtime state ────────────────────────────────────────────────────────────
class State:
    handles:    dict[str, DeviceHandle] = {}
    transports: dict[str, HC33ProxyTransport] = {}
    http:       MammotionHTTP | None = None     # lazy-built on first camera request / onboarding login
    cloud_client: CloudIOTGateway | None = None  # Aliyun gateway, cached across scans
    error_codes: dict | None = None             # cloud error-code table (code str -> ErrorInfo); None=unfetched, {}=unavailable
    # Onboarding-only: creds captured at the login step, used to write
    # secrets.toml on save.  Cleared after save.
    onboard_email:    str | None = None
    onboard_password: str | None = None
    # Per-mower auto-reconnect bookkeeping (see _watchdog / _auto_reconnect).
    auto_retrying:  dict[str, bool] = {}        # True while a retry sequence is mid-flight
    auto_gave_up:   dict[str, bool] = {}        # True after retries exhausted — manual reconnect required
    manual_op:      dict[str, bool] = {}        # True while /api/reconnect is running, suppresses watchdog
    watchdog_tasks: dict[str, asyncio.Task] = {}
    # Open joystick WebSockets, tracked so the lifespan shutdown can close them.
    # Without this, uvicorn hangs in "Waiting for connections to close" because
    # the joystick handler blocks forever in ws.receive_json() until the browser
    # tab closes — which Ctrl-C on Windows + selector loop can't interrupt.
    websockets: set[WebSocket] = set()


state = State()


# Backoff schedule for auto-reconnect on unexpected drops.  Five attempts total;
# total wall-clock window ≈ 48 s before giving up.  Once exhausted, the user
# must hit Reconnect — we don't pester a closed browser indefinitely.
AUTO_RECONNECT_DELAYS = [1, 2, 5, 10, 30]


async def _auto_reconnect(name: str) -> None:
    """Run the retry sequence for one mower after an unexpected transport drop."""
    state.auto_retrying[name] = True
    state.auto_gave_up[name]  = False
    t = state.transports[name]
    h = state.handles[name]
    try:
        for i, delay in enumerate(AUTO_RECONNECT_DELAYS, 1):
            await asyncio.sleep(delay)
            _LOGGER.info("auto-reconnect %s: attempt %d/%d", name, i, len(AUTO_RECONNECT_DELAYS))
            try:
                with contextlib.suppress(Exception):
                    await t.disconnect()
                await t.connect()
                await h.start()
                with contextlib.suppress(Exception):
                    await h.request_report_snapshot()
                _LOGGER.info("auto-reconnect %s: succeeded on attempt %d", name, i)
                return
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("auto-reconnect %s: attempt %d failed: %s", name, i, exc)
        _LOGGER.error("auto-reconnect %s: gave up after %d attempts — manual Reconnect required",
                      name, len(AUTO_RECONNECT_DELAYS))
        state.auto_gave_up[name] = True
    finally:
        state.auto_retrying[name] = False


async def _watchdog(name: str) -> None:
    """Watch one transport for unexpected drops and fire the retry sequence.

    Triggers only on a CONNECTED → DISCONNECTED transition.  Skips while
    `manual_op` is set (the /api/reconnect handler owns the lifecycle in that
    window) and while a retry sequence is already running.
    """
    t = state.transports[name]
    last = t.availability
    ticks = 0
    while True:
        try:
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            return
        try:
            current = t.availability
            unexpected_drop = (
                current == TransportAvailability.DISCONNECTED
                and last == TransportAvailability.CONNECTED
                and not state.manual_op.get(name, False)
                and not state.auto_retrying.get(name, False)
            )
            if unexpected_drop:
                _LOGGER.warning("watchdog %s: unexpected drop — starting auto-reconnect", name)
                asyncio.create_task(_auto_reconnect(name), name=f"auto-reconnect-{name}")
            last = current
            # Refresh the headlight state every ~6 s while connected.  The mower
            # auto-offs the light after a while and only a get_car_light response
            # updates lamp_info, so we actively re-probe; the answer lands async
            # and /api/status serves it on the next poll.  Piggybacking the 2 s
            # watchdog tick avoids a second timer.
            ticks += 1
            if current == TransportAvailability.CONNECTED and ticks % 3 == 0:
                h = state.handles.get(name)
                if h is not None:
                    with contextlib.suppress(Exception):
                        await h.send_raw(h.commands.get_car_light(1126))
                        # Cutter mode + live RPM land in cutter_work_mode_info
                        # only in reply to this probe — it's what lets the UI
                        # prove the blades are actually spinning.
                        await h.send_raw(h.commands.get_cutter_mode())
            # Refresh the fault log every ~60 s (HA polls get_errors at the same
            # cadence).  The device only populates errors.err_code_list in reply
            # to get_error_code, so without this the buffer stays empty and no
            # fault ever shows.  ticks%30==1 → first poll ~2 s after connect, so
            # the status chip has data almost immediately.
            if current == TransportAvailability.CONNECTED and ticks % 30 == 1:
                h = state.handles.get(name)
                if h is not None:
                    with contextlib.suppress(Exception):
                        await h.send_raw(h.commands.get_error_code())
                        await h.send_raw(h.commands.get_error_timestamp())
        except Exception:  # noqa: BLE001
            _LOGGER.exception("watchdog %s tick crashed", name)


# ── Handle lifecycle helpers (shared by lifespan + onboarding apply) ──────────
async def _build_and_connect(cfg: dict) -> None:
    """Build a transport + handle for one mower, connect, and arm its watchdog.

    Connect failures are non-fatal: the handle/watchdog are still registered so
    a later Reconnect (or auto-reconnect) can recover without a server restart.
    """
    name = cfg["name"]
    transport = HC33ProxyTransport(device_id=name, host=cfg["hc33_host"], port=cfg["hc33_port"])
    handle = DeviceHandle(
        device_id=name,
        device_name=name,
        initial_device=MowingDevice(name=name),
        iot_id=cfg["iot_id"],
        ble_transport=transport,
        prefer_ble=True,
    )
    state.handles[name] = handle
    state.transports[name] = transport
    try:
        await transport.connect()
        await handle.start()
        _LOGGER.info("connected to HC33 for %s at %s:%d", name, cfg["hc33_host"], cfg["hc33_port"])
    except Exception:
        _LOGGER.exception("initial HC33 connect for %s failed — will retry on first command", name)
    state.watchdog_tasks[name] = asyncio.create_task(_watchdog(name), name=f"watchdog-{name}")


async def _teardown_all(*, close_ws: bool) -> None:
    """Tear down every handle/transport/watchdog.  Used on shutdown and before
    re-applying a new roster from onboarding/settings."""
    if close_ws:
        for ws in list(state.websockets):
            with contextlib.suppress(Exception):
                await ws.close(code=1001, reason="server reconfigured")
    for task in state.watchdog_tasks.values():
        task.cancel()
    for task in state.watchdog_tasks.values():
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    for h in state.handles.values():
        with contextlib.suppress(Exception):
            await h.stop()
    for t in state.transports.values():
        with contextlib.suppress(Exception):
            await t.disconnect()
    state.watchdog_tasks.clear()
    state.handles.clear()
    state.transports.clear()
    state.auto_retrying.clear()
    state.auto_gave_up.clear()
    state.manual_op.clear()


async def _apply_mowers(new_mowers: list[dict]) -> None:
    """Swap the live roster: tear down current handles, rebuild from
    *new_mowers*, and update the module-level MOWERS in place."""
    await _teardown_all(close_ws=True)
    MOWERS[:] = new_mowers
    for cfg in MOWERS:
        await _build_and_connect(cfg)


def _served_port(default: int = 8443) -> int:
    """Port uvicorn was told to serve on, parsed from the launch command
    (`... --port 8443`), so the startup banner can show a real URL.  Falls back
    to the run-server default when not present on the command line."""
    argv = sys.argv
    if "--port" in argv:
        try:
            return int(argv[argv.index("--port") + 1])
        except (ValueError, IndexError):
            pass
    return default


def _primary_lan_ip() -> "str | None":
    """Best-effort primary LAN IPv4 (the egress source IP).  A UDP connect()
    sends no packet — it just makes the OS pick the outbound interface — then we
    read back the local address.  Returns None if it can't be determined."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except OSError:
            return None


# ── Lifespan: build BLE-only handles, connect transports ─────────────────────
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not MOWERS:
        _LOGGER.info("no mowers configured — starting in onboarding mode")
    for cfg in MOWERS:
        await _build_and_connect(cfg)

    # Uvicorn logs its bind address as https://0.0.0.0:PORT — that's "listen on
    # every interface", not a URL you can open.  Print the actual LAN address so
    # users have something browsable to copy/paste.
    ip = _primary_lan_ip() or "<this-machine-ip>"
    _LOGGER.info(
        "web UI ready — open  https://%s:%d/  from any device on this LAN "
        "(Uvicorn's 0.0.0.0 line above is the bind address, not a URL to open)",
        ip, _served_port(),
    )

    yield

    await _teardown_all(close_ws=True)


app = FastAPI(lifespan=lifespan)


# ── Authentication: signed cookie + HTML login form ───────────────────────────
# A pure-ASGI middleware so it covers EVERY route — pages, REST, static mount,
# and the joystick WebSocket — in one place.  Disabled (pass-through) when no
# password is set, so an un-onboarded install isn't accidentally locked out.
# Use over TLS only (run-server.bat serves https on 8443).
#
# Session = a signed cookie.  When it's missing/invalid, browser navigations are
# redirected to an HTML login form (/login) rather than the native HTTP Basic
# dialog — the form integrates with Chrome/iOS password managers (save +
# autofill), which the Basic dialog never did, especially on iOS where a
# self-signed cert stops the cookie persisting across restarts.  A `Basic`
# Authorization header is still accepted for scripted clients but never
# advertised (no 401 challenge), so it won't pop a browser dialog.
#
# WebSocket gotcha: browsers (notably Chrome) do NOT attach an `Authorization`
# header to a `wss://` upgrade, so gating the socket on Basic alone closes the
# joystick.  The cookie covers it: browsers DO send cookies on same-origin WS
# upgrades, and the page only loads when authenticated, so the cookie is present.
import base64
import hashlib
import hmac
import secrets as _secrets
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import HTMLResponse, PlainTextResponse, RedirectResponse

_WEB_USER, _WEB_PASS = persist.load_web_auth()
_AUTH_COOKIE = "luba_auth"
# Persist the auth cookie so a first Basic login is remembered across browser
# restarts (iOS Chrome/Safari drop session cookies on close, re-prompting Basic).
_AUTH_COOKIE_MAX_AGE = 90 * 24 * 3600  # 90 days
# Cookie value derived from the password (HMAC, not the password itself).  Stable
# across restarts so an open tab's cookie keeps working; rotates if you change
# the password.  None when auth is disabled.
_WS_TOKEN = (
    hmac.new(_WEB_PASS.encode("utf-8"), b"luba-ws-auth", hashlib.sha256).hexdigest()
    if _WEB_PASS else None
)


def _set_auth_cookie(resp) -> None:
    """Attach the persistent auth cookie to a Starlette response.  No Secure flag
    on purpose — a self-signed cert is a non-secure context, which can stop the
    browser storing a Secure cookie; the value is an HMAC, not the password."""
    resp.set_cookie(
        _AUTH_COOKIE,
        _WS_TOKEN or "",
        max_age=_AUTH_COOKIE_MAX_AGE,
        httponly=True,
        samesite="strict",
        path="/",
    )


class BasicAuthMiddleware:
    def __init__(self, app, username: str, password: str | None, token: str | None):
        self.app = app
        self.username = username
        self.password = password
        self.token = token

    async def __call__(self, scope, receive, send):
        if not self.password or scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        # The login form + its POST handler must be reachable without auth, else
        # the unauthenticated redirect below would loop.
        if scope["type"] == "http" and scope.get("path") == "/login":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        ok = self._basic_ok(headers) or self._cookie_ok(headers)

        if scope["type"] == "websocket":
            if ok:
                await self.app(scope, receive, send)
            else:
                # Reject the handshake before accept → uvicorn returns HTTP 403.
                await send({"type": "websocket.close", "code": 1008})
            return

        if not ok:
            # Browser navigations → the login form.  API/asset/XHR → a plain 401
            # with NO WWW-Authenticate header, so no native Basic dialog pops;
            # the client treats it as "session expired" and can send the user to
            # /login.  (On first load the GET / navigation is what gets gated, so
            # the SPA never even boots unauthenticated.)
            accept = headers.get(b"accept", b"")
            if scope.get("method") == "GET" and b"text/html" in accept:
                resp = RedirectResponse("/login", status_code=303)
            else:
                resp = PlainTextResponse("Authentication required", status_code=401)
            await resp(scope, receive, send)
            return

        # Authenticated HTTP: drop the auth cookie so the WS upgrade can carry it.
        # NOTE: Secure flag omitted on purpose — self-signed certs make the origin
        # a non-secure context, which can stop the browser storing a Secure cookie.
        # The token is an HMAC (not the password); served HTTPS-only, so low risk.
        cookie = (
            f"{_AUTH_COOKIE}={self.token}; Path=/; Max-Age={_AUTH_COOKIE_MAX_AGE}; "
            "HttpOnly; SameSite=Strict"
        ).encode("latin-1")

        async def send_with_cookie(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((b"set-cookie", cookie))
            await send(message)

        await self.app(scope, receive, send_with_cookie)

    def _basic_ok(self, headers: dict) -> bool:
        raw = headers.get(b"authorization", b"")
        if not raw.startswith(b"Basic "):
            return False
        try:
            user, _, pw = base64.b64decode(raw[6:]).decode("utf-8").partition(":")
        except Exception:  # noqa: BLE001 — any malformed header = unauthorized
            return False
        # Constant-time compares to avoid leaking length/contents via timing.
        return (
            _secrets.compare_digest(user, self.username)
            and _secrets.compare_digest(pw, self.password)
        )

    def _cookie_ok(self, headers: dict) -> bool:
        raw = headers.get(b"cookie", b"").decode("latin-1")
        for part in raw.split(";"):
            k, _, v = part.strip().partition("=")
            if k == _AUTH_COOKIE:
                return _secrets.compare_digest(v, self.token or "")
        return False


app.add_middleware(
    BasicAuthMiddleware, username=_WEB_USER, password=_WEB_PASS, token=_WS_TOKEN
)


# Never let the browser serve stale UI/API from cache.  iOS Safari/Chrome in
# particular cling to old app.js/index.html and silently break after an update
# (e.g. empty mower list until you open an incognito tab).
@app.middleware("http")
async def _no_store(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response
if _WEB_PASS:
    _LOGGER.info("web UI protected by HTTP Basic (username=%r)", _WEB_USER)
else:
    _LOGGER.warning(
        "no web password set (LUBA_WEB_PASSWORD env or web_password in "
        "secrets.toml) — the web UI is UNPROTECTED"
    )


# ── Static files ─────────────────────────────────────────────────────────────
HERE = Path(__file__).parent
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


def _is_onboarded() -> bool:
    """Onboarded once at least one mower is configured."""
    return bool(MOWERS)


def _login_html(error: bool = False) -> str:
    """Standalone (inline-styled, no /static deps) sign-in page.  A real <form>
    so Chrome / iOS password managers offer to save and later autofill it."""
    err = '<p class="err">Wrong username or password.</p>' if error else ""
    user = _WEB_USER or ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Luba Remote — Sign in</title>
<style>
  :root {{ --bg:#0b0f14; --surface:#121924; --surface-2:#1a2432;
           --border:rgba(255,255,255,.07); --text:#e8eef5; --text-dim:#8fa0b3;
           --accent:#2dd47a; --accent-press:#24b968; --accent-ink:#04371c;
           --danger:#ff5d5d; --radius:14px; --radius-sm:10px;
           --shadow:0 8px 28px rgba(0,0,0,.45);
           --font:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }}
  body {{ margin:0; min-height:100vh; display:flex; align-items:center;
         justify-content:center; color:var(--text); font-family:var(--font);
         background:radial-gradient(circle at 50% -20%, #131c29, var(--bg) 60%) var(--bg);
         padding:16px; box-sizing:border-box; }}
  form {{ background:var(--surface); padding:32px 28px;
          border:1px solid var(--border); border-radius:var(--radius);
          width:min(90vw,340px); box-shadow:var(--shadow); }}
  h1 {{ margin:0 0 20px; font-size:20px; font-weight:700; letter-spacing:.2px; }}
  label {{ display:block; font-size:12px; font-weight:600; letter-spacing:.3px;
           color:var(--text-dim); margin:14px 0 6px; }}
  input {{ width:100%; box-sizing:border-box; padding:12px; font-size:16px;
           border:1px solid var(--border); border-radius:var(--radius-sm);
           background:var(--surface-2); color:var(--text); outline:none;
           transition:border-color .12s ease; }}
  input:focus {{ border-color:rgba(45,212,122,.5); }}
  button {{ width:100%; margin-top:22px; padding:13px; font-size:16px; border:0;
            border-radius:var(--radius-sm); background:var(--accent);
            color:var(--accent-ink); font-weight:700; cursor:pointer;
            transition:background .12s ease; }}
  button:hover {{ background:var(--accent-press); }}
  button:active {{ background:var(--accent-press); }}
  .err {{ color:var(--danger); font-size:13px; font-weight:600; margin:0 0 10px; }}
</style>
</head>
<body>
<form method="post" action="/login" autocomplete="on">
  <h1>Luba Remote</h1>
  {err}
  <label for="u">Username</label>
  <input id="u" name="username" value="{user}" autocomplete="username"
         autocapitalize="none" autocorrect="off" spellcheck="false" required>
  <label for="p">Password</label>
  <input id="p" name="password" type="password" autocomplete="current-password"
         required>
  <button type="submit">Sign in</button>
</form>
</body>
</html>"""


@app.get("/login")
async def login_form(request: Request):
    # Auth disabled, or already signed in → straight to the app.
    if not _WEB_PASS:
        return RedirectResponse("/", status_code=303)
    tok = request.cookies.get(_AUTH_COOKIE)
    if tok and _secrets.compare_digest(tok, _WS_TOKEN or ""):
        return RedirectResponse("/", status_code=303)
    return HTMLResponse(_login_html())


@app.post("/login")
async def login_submit(request: Request):
    # Parse the urlencoded form by hand to avoid a python-multipart dependency.
    data = parse_qs((await request.body()).decode("utf-8", "replace"))
    user = (data.get("username", [""])[0]).strip()
    pw = data.get("password", [""])[0]
    if (
        _WEB_PASS
        and _secrets.compare_digest(user, _WEB_USER)
        and _secrets.compare_digest(pw, _WEB_PASS)
    ):
        resp = RedirectResponse("/", status_code=303)
        _set_auth_cookie(resp)
        return resp
    return HTMLResponse(_login_html(error=True), status_code=401)


@app.get("/")
async def root():
    # First run (no roster yet) → send the user straight to onboarding.
    if not _is_onboarded():
        return FileResponse(HERE / "static" / "onboarding.html")
    return FileResponse(HERE / "static" / "index.html")


@app.get("/onboarding")
async def onboarding_page():
    """The onboarding/settings page.  Reachable any time (e.g. the Settings
    link in the main UI) to re-scan or edit the roster."""
    return FileResponse(HERE / "static" / "onboarding.html")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _cfg(name: str) -> dict:
    cfg = next((m for m in MOWERS if m["name"] == name), None)
    if cfg is None:
        raise HTTPException(404, f"unknown mower {name!r}")
    return cfg


def _handle(name: str) -> DeviceHandle:
    h = state.handles.get(name)
    if h is None:
        raise HTTPException(404, f"unknown mower {name!r}")
    return h


async def _ensure_http() -> MammotionHTTP:
    """Return a logged-in MammotionHTTP, logging in from stored secrets if
    needed.  Used by the camera and by the onboarding scan (settings path)."""
    if state.http is not None and state.http.login_info is not None:
        return state.http
    email, password = persist.load_secrets()
    if not email or not password:
        raise HTTPException(
            503,
            "Cloud login required but no credentials are configured. "
            "Run onboarding to set your Mammotion email/password.",
        )
    http = MammotionHTTP()
    resp = await http.login_v2(email, password)
    if resp.code != 0:
        raise HTTPException(502, f"Mammotion login failed: {resp.msg!r}")
    state.http = http
    return http


# ── REST: list, action, camera start/stop ────────────────────────────────────
@app.get("/api/mowers")
async def list_mowers():
    return [
        {"name": m["name"], "nickname": m.get("nickname"), "camera": m["iot_id"] is not None}
        for m in MOWERS
    ]


# Action name → (command method, kwargs).  Zero-arg commands have empty kwargs.
_ACTIONS = {
    "pause":      ("pause_execute_task",   {}),
    "resume":     ("resume_execute_task",  {}),
    "dock":       ("return_to_dock",       {}),
    "undock":     ("leave_dock",           {}),
    "stop":       ("stop_and_not_save_task", {}),
    "light-on":   ("set_car_manual_light", {"manual_ctrl": True}),
    "light-off":  ("set_car_manual_light", {"manual_ctrl": False}),
    # Job control: start the mower's planned task / cancel the running one.
    "start-job":  ("start_job",            {}),
    "cancel-job": ("cancel_job",           {}),
    # Manual mowing: spin the blades up/down while driving by joystick.  The
    # mower's own safety logic still applies (lift/tilt/bumper cut the blades
    # regardless of what we send).
    "blades-on":  ("set_blade_control",    {"on_off": 1}),
    "blades-off": ("set_blade_control",    {"on_off": 0}),
}


@app.post("/api/action/{name}/{action}")
async def post_action(name: str, action: str):
    h = _handle(name)
    entry = _ACTIONS.get(action)
    if entry is None:
        raise HTTPException(400, f"unknown action {action!r}")
    method, kwargs = entry
    cmd_bytes = getattr(h.commands, method)(**kwargs)
    await h.send_raw(cmd_bytes)
    # For a headlight toggle, immediately request a read-back instead of waiting
    # for the next ~6 s watchdog probe.  The response lands async and a fast
    # client-side poll (see action() in app.js) picks up the confirmed state —
    # so the toggle reflects real success/failure within ~1-2 s.
    if action in ("light-on", "light-off"):
        with contextlib.suppress(Exception):
            await h.send_raw(h.commands.get_car_light(1126))
    return {"ok": True}


def _device_limits(h) -> "dict | None":
    """Per-model operating limits (blade height mm, working speed m/s) from
    pymammotion's device-config table, or None before the mower has reported
    its model info.  Used to bound the settings endpoint and to scale the UI
    sliders to what this specific mower supports."""
    try:
        limits = h.snapshot.raw.device_limits
        if limits.blade_height.max <= 0:
            return None
        return {
            "blade_height":  {"min": limits.blade_height.min,  "max": limits.blade_height.max},
            "working_speed": {"min": limits.working_speed.min, "max": limits.working_speed.max},
        }
    except (AttributeError, TypeError):
        return None


def _current_blade_height(h) -> "int | None":
    """Blade height (mm) from the last work report, or None if not reported.
    0 means 'not reported yet' on the wire, so it maps to None too."""
    try:
        return int(h.snapshot.raw.report_data.work.knife_height) or None
    except (AttributeError, TypeError, ValueError):
        return None


@app.post("/api/set/{name}/{setting}")
async def post_setting(name: str, setting: str, payload: dict = Body(...)):
    """Parameterized mower settings — body {"value": <number>}.

    blade_height: cutting height in mm.  speed: working speed in m/s.  Values
    are clamped to the mower's own model limits when it has reported them,
    otherwise to conservative Luba-family bounds, and the applied value is
    returned so the UI can reflect the clamp.
    """
    h = _handle(name)
    try:
        value = float(payload.get("value"))
    except (TypeError, ValueError):
        raise HTTPException(400, 'body must be {"value": <number>}')
    limits = _device_limits(h)
    if setting == "blade_height":
        lo, hi = (limits["blade_height"]["min"], limits["blade_height"]["max"]) if limits else (20, 100)
        v: "int | float" = int(min(max(value, lo), hi))
        await h.send_raw(h.commands.set_blade_height(int(v)))
    elif setting == "speed":
        lo, hi = (limits["working_speed"]["min"], limits["working_speed"]["max"]) if limits else (0.2, 0.6)
        v = round(min(max(value, lo), hi), 2)
        await h.send_raw(h.commands.set_speed(float(v)))
    elif setting == "cutter_mode":
        # Blade RPM preset: 0 = normal, 1 = slow/eco, 2 = fast.
        v = int(value)
        if v not in (0, 1, 2):
            raise HTTPException(400, "cutter_mode must be 0 (normal), 1 (slow) or 2 (fast)")
        await h.send_raw(h.commands.set_cutter_mode(v))
        with contextlib.suppress(Exception):
            await h.send_raw(h.commands.get_cutter_mode())  # fast read-back
    else:
        raise HTTPException(400, f"unknown setting {setting!r}")
    _LOGGER.info("set %s=%s for %s (requested %s)", setting, v, name, value)
    return {"ok": True, "applied": v}


def _current_orientation(h) -> "int | None":
    """Mower heading in degrees (0-359, geographic north = 0), or None if the
    mower hasn't reported a location yet.  Populated from RptDevLocation.real_toward
    by pymammotion (device.location.orientation)."""
    try:
        return int(h.snapshot.raw.location.orientation) % 360
    except (AttributeError, TypeError, ValueError):
        return None


# report_data.dev.sys_status (pymammotion WorkMode enum) → short human label for
# the UI status chip.  Only the values a running mower actually reports are
# spelled out; anything else falls back to "Mode <n>".  There is no "docked"
# mode — a mower sitting on the dock reports Charging (15) or, once full,
# Standby (11).
_MOWER_STATUS_LABELS = {
    0:  "Idle",
    1:  "Online",
    2:  "Offline",
    8:  "Disabled",
    10: "Starting up",
    11: "Standby",
    13: "Mowing",
    14: "Returning home",
    15: "Charging",
    16: "Updating",
    17: "Locked",
    19: "Paused",
    20: "Manual control",
    22: "Update complete",
    23: "Update failed",
    31: "Editing map",
    32: "Editing map",
    34: "Editing map",
    35: "Editing map",
    36: "Editing map",
    37: "Location error",
    38: "Off boundary",
    39: "Paused (charging)",
}

# A blocking fault stops the mower and drops it into Paused (MODE_PAUSE), so the
# UI only treats a code from the error log as a *live* fault in this state.  In
# any other state (Idle, Mowing, Returning, Charging, …) a code still sitting in
# the history ring is stale — the mower has moved on.
_MODE_PAUSE = 19


def _sys_status(h) -> "int | None":
    """The mower's raw sys_status (WorkMode) from the last report, or None."""
    try:
        return int(h.snapshot.raw.report_data.dev.sys_status)
    except (AttributeError, TypeError, ValueError):
        return None


def _last_error(h) -> int:
    """The mower's most-recent fault code (0 if none / not polled yet).

    errors.err_code_list is a 10-slot history ring paired with
    err_code_list_time; the entry with the newest timestamp is HA's "last
    error".  Populated by the watchdog's periodic get_error_code poll.  Codes
    are signed on the wire (e.g. -2801); the sign is normalised at lookup."""
    try:
        errors = h.snapshot.raw.errors
        pairs = [
            (int(t), int(c))
            for c, t in zip(errors.err_code_list, errors.err_code_list_time)
            if int(c) != 0
        ]
    except (AttributeError, TypeError, ValueError):
        return 0
    if not pairs:
        return 0
    return max(pairs)[1]  # code with the newest timestamp


async def _error_table() -> dict:
    """Cloud-fetched {code: ErrorInfo} table used to turn numeric mower fault
    codes into the same human text the Mammotion app / HA shows.  The device
    only sends numeric codes over BLE; the text lives in a CSV the cloud serves
    (MammotionHTTP.get_all_error_codes).  Fetched once and cached; returns {}
    (and caches it) when cloud login isn't configured or the fetch fails, so the
    UI degrades to bare numeric codes rather than erroring."""
    if state.error_codes is not None:
        return state.error_codes
    try:
        http = await _ensure_http()
        state.error_codes = await http.get_all_error_codes()
    except Exception as exc:  # no cloud creds, network blip, or API change
        _LOGGER.info("error-code table unavailable, showing numeric codes: %s", exc)
        state.error_codes = {}
    return state.error_codes


def _lookup_error(code: int, table: dict) -> "str | None":
    """HA-style human fault string for a code, e.g.
    "mcu: Lift sensor triggered, Please handle promptly", or None if code is 0.
    Format = "<module>: <en_implication>, <en_solution>" with empty parts
    dropped; falls back to "E<code>" if the row has no English text.  Codes are
    signed on the wire (e.g. -2801) while table keys are positive, so try abs()."""
    if not code:
        return None
    info = table.get(str(code)) or table.get(str(abs(code)))
    if info is None:
        return f"E{code}"
    module = (getattr(info, "module", "") or "").strip()
    impl = (getattr(info, "en_implication", "") or "").strip()
    soln = (getattr(info, "en_solution", "") or "").strip()
    body = ", ".join(p for p in (impl, soln) if p)
    if not body:
        return f"E{code}"
    return f"{module}: {body}" if module else body


@app.get("/api/status/{name}")
async def status(name: str):
    """Two-layer view of the link to a mower.

    `availability` reports the TCP/HaLow socket to the HC33 (transport layer).
    `mower_silent_s` reports how long since pymammotion last decoded a LubaMsg
    from the mower itself (BLE layer) — None means we've never heard from it
    since the handle was started, large numbers mean the BLE side is wedged
    even though the TCP socket is healthy.
    """
    _cfg(name)
    t = state.transports[name]
    h = state.handles[name]
    last = h.last_report_at  # monotonic seconds, 0.0 if never
    silent_s = None if last == 0.0 else max(0.0, time.monotonic() - last)
    # Battery telemetry from the last decoded report.  Same access path handle.py
    # uses in _device_mode(); stays None until the mower has reported once.
    battery = None
    charging = None
    try:
        dev = h.snapshot.raw.report_data.dev
        battery = int(dev.battery_val)
        charging = int(dev.charge_state) != 0
    except (AttributeError, TypeError, ValueError):
        pass
    # Headlight state.  The watchdog re-probes get_car_light(1126) every ~6 s,
    # so this tracks the firmware's auto-off.  Stays None until the mower has
    # answered at least once, so the UI keeps its optimistic guess rather than
    # snapping the toggle to a stale default right after connect.
    light_on = None
    try:
        light_on = bool(h.snapshot.raw.mower_state.lamp_info.manual_light)
    except AttributeError:
        pass
    # Mower work-state ("Mowing", "Charging", …) from sys_status, + the most
    # recent fault from the polled error log (see _last_error).  The error log is
    # a *history* ring — the live toapp_err_code push doesn't cross the HC33
    # proxy — so an old code persists there long after the mower recovers.  Only
    # surface it as a live fault when the mower is Paused (MODE_PAUSE): a blocking
    # fault stops the mower into that state, whereas in Idle/Mowing/Returning/
    # Charging/etc. a lingering code is stale history, not a current problem.
    # Fault text comes from the cloud table, only looked up when a code is present
    # so a healthy mower never triggers a cloud login.
    sys_status = _sys_status(h)
    mower_state_label = (
        None if sys_status is None
        else _MOWER_STATUS_LABELS.get(sys_status, f"Mode {sys_status}")
    )
    err_code = _last_error(h) if sys_status == _MODE_PAUSE else 0
    mower_error = _lookup_error(err_code, await _error_table()) if err_code else None
    # Blade/cutter telemetry.  blades_on decodes sensor_status bits 9-11
    # CONSERVATIVELY: the documented encoding is OFF=0/ON=1, but upstream's
    # blade_state property maps ANY nonzero (1-7) to ON — and sibling sensors
    # use the same 3-bit field as a health scheme (1=warning, 2-7=error), so a
    # warning state would misread as "blades spinning".  We report ON only for
    # the exact documented value 1; the raw bits are on /static/diag.html.
    blades_on = None
    cutter_mode = None
    cutter_rpm = None
    try:
        blade_bits = (int(h.snapshot.raw.report_data.dev.sensor_status) >> 9) & 0x7
        blades_on = blade_bits == 1
    except (AttributeError, TypeError, ValueError):
        pass
    # Mode + RPM: the get_cutter_mode reply is routed by pymammotion's state
    # reducer into mower_state.cutter_mode/cutter_rpm — NOT into
    # report_data.cutter_work_mode_info (that one only fills when cutter info
    # rides inside a toapp_report_data).  Prefer the reducer path, fall back to
    # the report path.  (0 = normal, 1 = slow, 2 = fast.)
    try:
        ms = h.snapshot.raw.mower_state
        cutter_mode = int(ms.cutter_mode)
        cutter_rpm = int(ms.cutter_rpm)
    except (AttributeError, TypeError, ValueError):
        pass
    if not cutter_rpm:
        try:
            cw = h.snapshot.raw.report_data.cutter_work_mode_info
            cutter_mode = int(cw.current_cutter_mode) if cutter_mode is None else cutter_mode
            cutter_rpm = int(cw.current_cutter_rpm)
        except (AttributeError, TypeError, ValueError):
            pass
    # Job/manual-drive telemetry from the work report: mow completion %, area
    # mowed (device units), and the manual-drive speed readback.
    mow_percent = None
    area_mowed = None
    man_run_speed = None
    try:
        w = h.snapshot.raw.report_data.work
        mow_percent = int(w.mow_percent)
        area_mowed = int(w.area_mowed)
        man_run_speed = int(w.man_run_speed)
    except (AttributeError, TypeError, ValueError):
        pass
    # Raw sensor_status bit-field (source bits behind blades_on — see
    # /static/diag.html for the decoded view).  None until the mower reports.
    sensor_status = None
    try:
        sensor_status = int(h.snapshot.raw.report_data.dev.sensor_status)
    except (AttributeError, TypeError, ValueError):
        pass
    # While the auto-reconnect watchdog is mid-retry the underlying transport
    # flickers DISCONNECTED↔CONNECTING; report a steady "connecting" to the UI
    # so the badge doesn't strobe red between attempts.
    if state.auto_retrying.get(name, False):
        availability = "connecting"
    else:
        availability = t.availability.name.lower()  # connected | connecting | disconnected
    return {
        "connected":      t.is_connected,
        "availability":   availability,
        "mower_silent_s": silent_s,
        "battery":        battery,    # 0..100, or None if not reported yet
        "charging":       charging,   # True while on the dock charging
        "light_on":       light_on,   # headlight state, or None if not probed yet
        "status":         mower_state_label,  # "Mowing"/"Charging"/… or None
        "error":          mower_error,        # human fault text, or None if no fault
        "auto_retrying":  state.auto_retrying.get(name, False),
        "auto_gave_up":   state.auto_gave_up.get(name, False),
        "orientation":    _current_orientation(h),  # heading in degrees, or None
        "blade_height":   _current_blade_height(h),  # mm from last work report, or None
        "limits":         _device_limits(h),         # per-model slider bounds, or None
        "blades_on":      blades_on,     # mower-reported disc rotation, or None
        "cutter_mode":    cutter_mode,   # 0 normal / 1 slow / 2 fast, or None
        "cutter_rpm":     cutter_rpm,    # live blade RPM, or None
        "mow_percent":    mow_percent,   # job completion 0-100, or None
        "area_mowed":     area_mowed,    # area mowed (device units), or None
        "man_run_speed":  man_run_speed, # manual-drive speed readback, or None
        "sensor_status":  sensor_status, # raw sensor bit-field int, or None
    }


@app.get("/api/heading/{name}")
async def heading(name: str):
    """Lightweight heading poll for the camera compass overlay (~1 Hz while the
    camera is open).  Kept separate from /api/status so the fast poll stays cheap."""
    _cfg(name)
    h = state.handles[name]
    return {"orientation": _current_orientation(h)}


# ── Diagnostics / capability probe (backing /static/diag.html) ──────────────
# Read-only introspection: GET /api/diag/{name} dumps everything the last
# decoded reports gave us (raw sensor_status bits included, so the library's
# blade-state decode can be eyeballed against reality), and POST
# /api/diag/{name}/probe sends a battery of STRICTLY read-only query commands,
# diffing the snapshot around each one to map which queries this firmware
# actually answers over the BLE proxy.
import dataclasses as _dataclasses
import enum as _enum


def _diag_jsonable(v):
    """Best-effort JSON-safe rendering of snapshot values (enums, dataclasses,
    nested containers).  Falls back to repr() rather than raising."""
    if isinstance(v, _enum.Enum):
        return f"{v.name}({v.value})"
    if _dataclasses.is_dataclass(v) and not isinstance(v, type):
        try:
            return {k: _diag_jsonable(x) for k, x in _dataclasses.asdict(v).items()}
        except Exception:  # noqa: BLE001
            return repr(v)
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_diag_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _diag_jsonable(x) for k, x in v.items()}
    return repr(v)


def _diag_get(h, path: str):
    """Dotted-attribute path off h.snapshot.raw, JSON-safe; None on any failure."""
    try:
        v = h.snapshot.raw
        for part in path.split("."):
            v = getattr(v, part)
        return _diag_jsonable(v)
    except Exception:  # noqa: BLE001
        return None


# DeviceData's documented sensor_status bit-field accessors (report_info.py).
_DIAG_SENSOR_ACCESSORS = (
    "bumper_state", "blade_state",
    "ult_left", "ult_left_front", "ult_right_front", "ult_right",
)


@app.get("/api/diag/{name}")
async def diag(name: str):
    """Rich read-only snapshot dump for the diagnostics page.  Every section is
    individually guarded so partial data still returns."""
    _cfg(name)
    h = _handle(name)
    out: dict = {"name": name}

    try:
        ms = h.snapshot.raw.mower_state
        out["identity"] = {
            k: _diag_jsonable(getattr(ms, k, None))
            for k in ("model", "product_key", "sub_model_id", "model_id", "swversion")
        }
    except Exception as exc:  # noqa: BLE001
        out["identity"] = {"error": str(exc)}

    try:
        dev = h.snapshot.raw.report_data.dev
        sensor = int(getattr(dev, "sensor_status", 0))
        d: dict = {
            "sys_status":        _diag_jsonable(getattr(dev, "sys_status", None)),
            "battery_val":       _diag_jsonable(getattr(dev, "battery_val", None)),
            "charge_state":      _diag_jsonable(getattr(dev, "charge_state", None)),
            "self_check_status": _diag_jsonable(getattr(dev, "self_check_status", None)),
            "sensor_status":     sensor,
            "sensor_status_bin": format(sensor, "032b"),
            # Raw 3-bit groups per the model docstrings.  Bits 9-11 are what the
            # library collapses into a boolean blade_state (ANY nonzero → ON) —
            # the raw 0-7 value is what lets that decode be checked.
            "sensor_bits": {
                "bumper_bits_0_2":            sensor & 0x7,
                "blade_bits_9_11":            (sensor >> 9) & 0x7,
                "ult_left_bits_12_14":        (sensor >> 12) & 0x7,
                "ult_left_front_bits_15_17":  (sensor >> 15) & 0x7,
                "ult_right_front_bits_18_20": (sensor >> 18) & 0x7,
                "ult_right_bits_21_23":       (sensor >> 21) & 0x7,
            },
        }
        decoded: dict = {}
        for prop in _DIAG_SENSOR_ACCESSORS:
            try:
                decoded[prop] = _diag_jsonable(getattr(dev, prop))
            except Exception as exc:  # noqa: BLE001
                decoded[prop] = f"<error: {exc}>"
        d["sensor_decoded_by_lib"] = decoded
        if hasattr(dev, "collector_status"):
            with contextlib.suppress(Exception):
                d["collector_status"] = _diag_jsonable(dev.collector_status)
        out["dev"] = d
    except Exception as exc:  # noqa: BLE001
        out["dev"] = {"error": str(exc)}

    try:
        w = h.snapshot.raw.report_data.work
        out["work"] = {
            k: _diag_jsonable(getattr(w, k, None))
            for k in ("knife_height", "man_run_speed", "mow_percent", "area_mowed",
                      "nav_run_mode", "test_mode_status")
        }
    except Exception as exc:  # noqa: BLE001
        out["work"] = {"error": str(exc)}

    try:
        cw = h.snapshot.raw.report_data.cutter_work_mode_info
        out["cutter"] = {
            "current_cutter_mode": _diag_jsonable(getattr(cw, "current_cutter_mode", None)),
            "current_cutter_rpm":  _diag_jsonable(getattr(cw, "current_cutter_rpm", None)),
            # The state reducer routes a *driver* get_cutter_mode reply into
            # mower_state.cutter_mode/rpm, NOT into report_data — surface both
            # so a reply is visible whichever path this firmware uses.
            "mower_state_cutter_mode": _diag_get(h, "mower_state.cutter_mode"),
            "mower_state_cutter_rpm":  _diag_get(h, "mower_state.cutter_rpm"),
        }
    except Exception as exc:  # noqa: BLE001
        out["cutter"] = {"error": str(exc)}

    try:
        out["maintenance"] = _diag_jsonable(h.snapshot.raw.report_data.maintenance)
    except Exception as exc:  # noqa: BLE001
        out["maintenance"] = {"error": str(exc)}

    try:
        limits = _diag_jsonable(h.snapshot.raw.device_limits)
        out["limits"] = limits if isinstance(limits, dict) else _device_limits(h)
    except Exception as exc:  # noqa: BLE001
        out["limits"] = {"error": str(exc)}

    try:
        last = h.last_report_at
        out["last_report_age_s"] = (
            None if last == 0.0 else round(max(0.0, time.monotonic() - last), 1)
        )
    except Exception:  # noqa: BLE001
        out["last_report_age_s"] = None
    return out


# One probe run at a time, globally — overlapping runs would corrupt each
# other's before/after diffs and double BLE traffic through the same proxy.
_PROBE_LOCK = asyncio.Lock()
_PROBE_WAIT_S = 3.0

# (probe label, h.commands method (None = handle-level baseline), args,
#  {watched label: dotted snapshot path}, read-only rationale).
#
# READ-ONLY AUDIT — every entry verified against pymammotion 0.8.9 source;
# nothing here sets, moves, starts, or toggles state:
#   request_report_snapshot  → todev_report_cfg RPT_START count=1 (telemetry request)
#   get_speed                → MctlDriver(bidire_speed_read_set=DrvSrSpeed(rw=0))
#   get_cutter_mode          → MctlDriver(current_cutter_mode=AppGetCutterWorkMode())
#   get_maintenance          → request_iot_sys(RPT_START, [MAINTAIN, BASESTATION_INFO,
#                              FW_INFO], count=3) — a report subscription request
#   get_device_version_info  → MctlSys(todev_get_dev_fw_info=1)
#   get_device_product_model → MctlSys(device_product_type_info=DeviceProductTypeInfoT(result=1))
#   get_car_light(1126)      → SocMul(get_lamp=GetHeadlamp(get_ids=1126))
#   get_error_code           → MctlSys(bidire_comm_cmd=SysCommCmd(id=5, context=2, rw=1))
#                              — upstream's error-ring dump request (the watchdog
#                              already polls it every ~60 s); requests history only
#   read_animal_avoidance    → MctlNav(nav_sys_param_cmd=NavSysParamMsg(id=13, rw=0))
_PROBES: "list[tuple[str, str | None, tuple, dict[str, str], str]]" = [
    ("request_report_snapshot", None, (), {
        "dev.sys_status":    "report_data.dev.sys_status",
        "dev.battery_val":   "report_data.dev.battery_val",
        "dev.sensor_status": "report_data.dev.sensor_status",
    }, "baseline liveness: one-shot report-cfg request (RPT_START, count=1) — asks the mower to report telemetry, sets nothing"),
    ("get_speed", "get_speed", (), {
        "mower_state.travel_speed": "mower_state.travel_speed",
        "work.man_run_speed":       "report_data.work.man_run_speed",
    }, "DrvSrSpeed(rw=0): explicit read flag, carries no value to apply"),
    ("get_cutter_mode", "get_cutter_mode", (), {
        "report_data.cutter_work_mode_info": "report_data.cutter_work_mode_info",
        "mower_state.cutter_mode":           "mower_state.cutter_mode",
        "mower_state.cutter_rpm":            "mower_state.cutter_rpm",
    }, "AppGetCutterWorkMode(): pure query for current cutter mode/RPM"),
    ("get_maintenance", "get_maintenance", (), {
        "report_data.maintenance": "report_data.maintenance",
    }, "request_iot_sys(RPT_START, [MAINTAIN, BASESTATION_INFO, FW_INFO], count=3): telemetry request only"),
    ("get_device_version_info", "get_device_version_info", (), {
        "mower_state.swversion":            "mower_state.swversion",
        "device_firmwares.device_version":  "device_firmwares.device_version",
        "device_firmwares.main_controller": "device_firmwares.main_controller",
    }, "MctlSys(todev_get_dev_fw_info=1): firmware-version query"),
    ("get_device_product_model", "get_device_product_model", (), {
        "mower_state.model_id":     "mower_state.model_id",
        "mower_state.sub_model_id": "mower_state.sub_model_id",
    }, "DeviceProductTypeInfoT(result=1): product-type query"),
    ("get_car_light(1126)", "get_car_light", (1126,), {
        "mower_state.lamp_info": "mower_state.lamp_info",
    }, "GetHeadlamp(get_ids=1126): headlight readback (the watchdog already sends this every ~6 s)"),
    ("get_error_code", "get_error_code", (), {
        "errors.err_code_list":      "errors.err_code_list",
        "errors.err_code_list_time": "errors.err_code_list_time",
    }, "SysCommCmd(id=5, context=2, rw=1): upstream's fault-history dump request (same one the watchdog polls); requests data, sets nothing"),
    ("read_animal_avoidance", "read_animal_avoidance", (), {
        "mower_state.animal_protection": "mower_state.animal_protection",
    }, "NavSysParamMsg(id=13, rw=0): explicit read of the animal-avoidance setting"),
]


def _diag_nonzero(v) -> bool:
    """True if a watched value is 'populated' (any nonzero / nonempty leaf)."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, dict):
        return any(_diag_nonzero(x) for x in v.values())
    if isinstance(v, (list, tuple)):
        return any(_diag_nonzero(x) for x in v)
    return v is not None


@app.post("/api/diag/{name}/probe")
async def diag_probe(name: str):
    """Capability prober: sequentially send each read-only query in _PROBES,
    waiting ~3 s per probe and diffing the watched snapshot fields around it.
    Rejects a concurrent run with 409.  Total wall clock ≈ 28 s."""
    _cfg(name)
    h = _handle(name)
    if _PROBE_LOCK.locked():
        raise HTTPException(409, "a capability probe run is already in progress")
    async with _PROBE_LOCK:
        started = time.monotonic()
        probes: dict[str, dict] = {}
        for label, method, args, watch, why in _PROBES:
            entry: dict = {"why": why}
            try:
                if method is None:
                    if not hasattr(h, "request_report_snapshot"):
                        entry.update(status="not-in-lib",
                                     detail="DeviceHandle.request_report_snapshot missing from installed pymammotion")
                        probes[label] = entry
                        continue
                elif not hasattr(h.commands, method):
                    entry.update(status="not-in-lib",
                                 detail=f"h.commands.{method} missing from installed pymammotion")
                    probes[label] = entry
                    continue
                before = {k: _diag_get(h, p) for k, p in watch.items()}
                la_before = h.last_report_at
                if method is None:
                    await h.request_report_snapshot()
                else:
                    await h.send_raw(getattr(h.commands, method)(*args))
                await asyncio.sleep(_PROBE_WAIT_S)
                after = {k: _diag_get(h, p) for k, p in watch.items()}
                changed = [k for k in watch if after.get(k) != before.get(k)]
                entry["before"] = before
                entry["after"] = after
                # Did ANY LubaMsg arrive during the wait?  Attribution is fuzzy
                # (a periodic report also advances this), but False is a strong
                # hint the BLE side never answered anything.
                entry["link_alive_during_wait"] = h.last_report_at > la_before
                if changed:
                    entry["status"] = "replied"
                    entry["detail"] = "watched field(s) changed: " + ", ".join(changed)
                else:
                    entry["status"] = "sent-no-change"
                    populated = [k for k, v in after.items() if _diag_nonzero(v)]
                    if label == "get_error_code" and not populated:
                        entry["detail"] = (
                            "sent, no data: the error ring is empty — a mower with no "
                            "recorded faults legitimately answers all-zeros, which this "
                            "diff cannot tell apart from no reply"
                        )
                    elif populated:
                        entry["detail"] = (
                            "no change, but " + ", ".join(populated) + " already held "
                            "nonzero data before the probe — a fresh answer carrying "
                            "identical values is invisible to this diff"
                        )
                    else:
                        entry["detail"] = (
                            "watched fields still empty/zero after the wait — either no "
                            "reply, or the reply lands somewhere this probe does not watch"
                        )
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error"
                entry["detail"] = f"{type(exc).__name__}: {exc}"
            probes[label] = entry
        return {
            "ok": True,
            "mower": name,
            "wait_s_per_probe": _PROBE_WAIT_S,
            "duration_s": round(time.monotonic() - started, 1),
            "note": (
                "All probes are read-only queries. 'sent-no-change' is NOT proof a "
                "command is unsupported — some replies don't touch the watched snapshot "
                "fields, and an answer identical to the current value produces no diff."
            ),
            "probes": probes,
        }


@app.post("/api/reconnect/{name}")
async def reconnect(name: str):
    """Re-run the lifespan connect for one mower.

    Useful when the HC33 was unreachable at startup (lifespan caught the
    exception, leaving the handle's queue/keepalive uninitialized) or when
    the link wedges and we want to force a clean re-handshake.  Both calls
    are idempotent.
    """
    _cfg(name)
    t = state.transports[name]
    h = state.handles[name]
    # manual_op suppresses the watchdog while we deliberately bounce the socket;
    # auto_gave_up is cleared so future drops can re-arm the retry sequence.
    state.manual_op[name] = True
    try:
        with contextlib.suppress(Exception):
            await t.disconnect()
        await t.connect()
        await h.start()
        # Probe the mower so /api/status starts seeing a non-null mower_silent_s
        # within a few seconds.  No-op if the BLE stream is already running.
        with contextlib.suppress(Exception):
            await h.request_report_snapshot()
        state.auto_gave_up[name] = False
        _LOGGER.info("reconnect for %s succeeded", name)
        return {"ok": True}
    finally:
        state.manual_op[name] = False


@app.post("/api/camera/{name}/start")
async def camera_start(name: str):
    cfg = _cfg(name)
    if cfg["iot_id"] is None:
        raise HTTPException(
            400,
            f"Mower {name!r} has no iot_id configured in mowers.toml — camera is unavailable.",
        )
    h = _handle(name)
    http = await _ensure_http()
    is_yuka = DeviceType.is_yuka(name)
    sub = await http.get_stream_subscription(cfg["iot_id"], is_yuka)
    if sub.code != 0 or sub.data is None:
        raise HTTPException(502, f"stream-subscription failed: {sub.msg!r}")
    # Diagnostic: is the channel encrypted this session?  If openEncrypt flips to
    # 1, an unconfigured web client receives undecryptable RTP (packets arrive,
    # zero frames assemble) while the native app — which holds the key — is fine.
    _LOGGER.info(
        "stream-subscription: openEncrypt=%r license=%r cameras=%r uid=%r channel=%r",
        sub.data.openEncrypt,
        sub.data.license,
        [(c.cameraId, bool(c.token)) for c in sub.data.cameras],
        sub.data.uid,
        sub.data.channelName,
    )
    # New-firmware Luba 2X doesn't auto-publish — send the explicit join over BLE.
    cmd = h.commands.device_agora_join_channel_with_position(enter_state=1)
    await h.send_raw(cmd)
    # Kick a continuous report stream so the camera compass gets live heading
    # (RptDevLocation) at ~1 Hz.  Stopped again in camera_stop.
    with contextlib.suppress(Exception):
        await h.send_raw(h.commands.get_report_cfg(count=0))
    d = sub.data
    return {
        "appid":       d.appid,
        "channelName": d.channelName,
        "token":       d.token,
        "uid":         d.uid,
    }


@app.post("/api/camera/{name}/stop")
async def camera_stop(name: str):
    h = _handle(name)
    cmd = h.commands.device_agora_join_channel_with_position(enter_state=0)
    await h.send_raw(cmd)
    # Stop the continuous report stream started in camera_start.
    with contextlib.suppress(Exception):
        await h.send_raw(h.commands.get_report_cfg_stop())
    return {"ok": True}


@app.post("/api/camera/{name}/refresh")
async def camera_refresh(name: str):
    """Ask the mower's encoder to emit a fresh keyframe (IDR).

    A browser that subscribes a beat after the mower starts publishing misses
    the initial keyframe; H.264 then can't decode anything until the next IDR,
    which the Luba won't send on its own.  The browser calls this once it is
    actually subscribed (and again on RECV_VIDEO_DECODE_FAILED) to unstick it.
    """
    h = _handle(name)
    cmd = h.commands.refresh_fpv()
    await h.send_raw(cmd)
    return {"ok": True}


# ── Onboarding / settings ─────────────────────────────────────────────────────
@app.get("/api/onboard/status")
async def onboard_status():
    """What the onboarding page needs to decide its initial state.

    The login step is shown on first run (not yet onboarded) regardless of
    whether EMAIL/PASSWORD env vars exist — setup should be explicit.  Once a
    roster is saved (onboarded), the page skips login and the server logs in
    from secrets.toml / env on demand.  `suggested_email` pre-fills the form
    when we can detect a likely address (from env or a prior secrets.toml).
    """
    email, _pw = persist.load_secrets()
    return {
        "onboarded":       _is_onboarded(),
        "logged_in":       state.http is not None and state.http.login_info is not None,
        "suggested_email": email or "",
        "mower_count":     len(MOWERS),
    }


@app.post("/api/onboard/login")
async def onboard_login(payload: dict = Body(...)):
    """Validate Mammotion credentials and cache the session.  Creds are held in
    memory until /api/onboard/save persists them to secrets.toml."""
    email = (payload.get("email") or "").strip()
    password = payload.get("password") or ""
    if not email or not password:
        raise HTTPException(400, "email and password are required")
    http = MammotionHTTP()
    try:
        resp = await http.login_v2(email, password)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"login error: {exc}"}
    if resp.code != 0:
        return {"ok": False, "error": resp.msg or "login failed"}
    state.http = http
    state.cloud_client = None  # force a fresh Aliyun handshake for this session
    state.onboard_email = email
    state.onboard_password = password
    return {"ok": True}


async def _confirm_pending_shares(http: MammotionHTTP) -> None:
    """Accept any pending device-share invitations on this account.

    A mower shared to a secondary account does NOT appear in any device list
    until the share is confirmed — the phone app confirms silently on login, so
    a server-only account never gets the chance and every enumeration comes
    back empty.  Mirrors PyMammotion client.py's login flow (HTTP-level
    confirm; the Aliyun-side notice list is confirmed separately in
    _cloud_mowers once a gateway session exists).  Never fatal to the scan.
    """
    try:
        shared = await http.get_user_shared_device_page()
        records = (shared.data.records if shared.data else None) or []
        _LOGGER.info("share page: %d record(s): %s", len(records),
                     [(r.device_name, r.is_receiver, r.status) for r in records])
        pending: dict[str, list[int]] = {}
        for r in records:
            if r.is_receiver == 1 and r.status == -1:
                pending.setdefault(r.batch_id, []).append(int(r.record_id))
        for batch_id, record_ids in pending.items():
            await http.confirm_share(batch_id, record_ids)
            _LOGGER.info("accepted pending share (batch=%s, %d device(s))",
                         batch_id, len(record_ids))
    except Exception as exc:  # noqa: BLE001
        _LOGGER.warning("pending-share check failed (continuing): %s", exc)


async def _cloud_mowers() -> list[dict]:
    """Account's mowers (RTK base stations filtered out).

    Primary source is the Aliyun binding list (`list_binding_by_account`) — the
    authoritative list for Luba/Yuka devices, where `/device-server/v1/device/list`
    routinely comes back empty.  Falls back to that endpoint (and the
    shared-device page, for share-receiving secondary accounts) if the gateway
    handshake yields nothing.  Each device carries name + iot_id + product_key,
    so RTK filtering uses both.
    """
    http = await _ensure_http()
    await _confirm_pending_shares(http)
    # (device_name, iot_id, product_key, nickname).  nickname is the friendly
    # name set in the Mammotion app; None when unset or via the fallback path.
    devices: list[tuple[str, str, str, str | None]] = []
    gateway_error: str | None = None

    try:
        cloud = state.cloud_client
        if cloud is None:
            # One-time Aliyun handshake (region → connect → oauth → aep →
            # session).  Needs the MAMMOTION_OAUTH2_* / ALIYUN_* constants from
            # the PyMammotion env/.env; raises clearly if they're missing.
            cloud = CloudIOTGateway(http)
            # get_region + login_by_oauth authenticate to Aliyun with
            # login_info.authorization_code as a THIRD_AUTHCODE.  That code is
            # short-lived: when state.http was minted earlier (e.g. at camera
            # start) the cached code may have expired by scan time, and Aliyun
            # then rejects the region lookup with HTTP 500
            # "openId isEmpty,result_code=100002".  Re-mint a fresh auth code so
            # the handshake always carries a live one; this also refreshes the
            # access token first if it's near expiry.  Non-fatal — if the refresh
            # itself fails we fall through and let the handshake surface the error.
            try:
                await http.refresh_authorization_token()
            except Exception as exc:  # noqa: BLE001
                _LOGGER.warning("auth-code refresh before Aliyun handshake failed: %s", exc)
            login_info = http.login_info
            country = login_info.userInformation.domainAbbreviation
            if cloud.region_response is None:
                await cloud.get_region(country)
            await cloud.connect()
            await cloud.login_by_oauth(country)
            await cloud.aep_handle()
            await cloud.session_by_auth_code()
            state.cloud_client = cloud
        # Aliyun-side share notices: like the HTTP share page, a pending share
        # keeps the device out of the binding list until confirmed.
        try:
            notice = await cloud.get_shared_notice_list()
            if notice.data and notice.data.data:
                pending = [d.record_id for d in notice.data.data if d.status == -1]
                if pending:
                    await cloud.confirm_share(pending)
                    _LOGGER.info("accepted %d pending Aliyun share notice(s)", len(pending))
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("Aliyun share-notice check failed (continuing): %s", exc)
        try:
            await cloud.list_binding_by_account()
        except Exception as exc:  # noqa: BLE001
            # Aliyun sometimes rejects a freshly-minted iotToken outright
            # ("request auth error" minutes after issue).  Upstream's remedy
            # (client.py) is a forced checkOrRefreshSession, then one retry.
            _LOGGER.warning("listBindingByAccount failed (%s) — forcing session refresh + retry", exc)
            await cloud.check_or_refresh_session(force=True)
            await cloud.list_binding_by_account()
        resp = cloud.devices_by_account_response
        rows = resp.data.data if resp and resp.data and resp.data.data else []
        _LOGGER.info("scan source aliyun-binding: %d device(s)", len(rows))
        for d in rows:
            devices.append((d.device_name, d.iot_id, d.product_key or "", d.nick_name or None))
    except Exception as exc:  # noqa: BLE001
        gateway_error = str(exc)
        # Drop the cached session: it may be the reason the listing failed, and
        # keeping it would make every Re-scan reuse the same rejected token.
        state.cloud_client = None
        _LOGGER.warning("Aliyun device enumeration failed: %s", exc)

    if not devices:
        # Fallback: owned device-server list (name + iot_id, no product_key).
        try:
            resp = await http.get_user_device_list()
            rows = resp.data or []
            _LOGGER.info("scan source device-list(owned): %d device(s)", len(rows))
            for d in rows:
                devices.append((d.device_name, d.iot_id, "", None))
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("device-server enumeration failed: %s", exc)

    if not devices:
        # Fallback: the paginated device page.  For a share-RECEIVING secondary
        # account this is where the mower actually appears (owned=0 records,
        # with product_key for RTK filtering) — the Aliyun binding list and the
        # owned list are both empty for such accounts even though the phone app
        # sees and controls the mower fine.
        try:
            page = await http.get_user_device_page()
            recs = (page.data.records if page.data else None) or []
            _LOGGER.info("scan source device-page: %d record(s): %s", len(recs),
                         [(r.device_name, r.owned, r.status) for r in recs])
            for r in recs:
                if r.device_name:
                    devices.append((r.device_name, r.iot_id or "", r.product_key or "", None))
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("device-page enumeration failed: %s", exc)

    if not devices:
        # Last resort for share-receiving accounts: the share records
        # themselves.  status == -1 means a share we failed to confirm above —
        # skip those, they're not usable yet.
        try:
            shared = await http.get_user_shared_device_page()
            recs = (shared.data.records if shared.data else None) or []
            _LOGGER.info("scan source share-page: %d record(s)", len(recs))
            for r in recs:
                if r.device_name and r.status != -1:
                    devices.append((r.device_name, r.iot_id or "", "", None))
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning("shared-device-page enumeration failed: %s", exc)

    if not devices and gateway_error:
        raise HTTPException(502, f"Could not list account devices: {gateway_error}")

    out: list[dict] = []
    seen: set[str] = set()
    for name, iot_id, pk, nickname in devices:
        if not name or name in seen:
            continue
        if DeviceType.is_rtk(name, pk):
            continue  # skip RTK base stations
        seen.add(name)
        out.append({"name": name, "iot_id": iot_id or None, "nickname": nickname})
    return out


@app.get("/api/onboard/scan")
async def onboard_scan():
    """Enumerate account mowers + discover proxies on the LAN, then propose a
    pairing by matching proxy bonded_name == cloud device_name.

    Requires a cloud session: established via /api/onboard/login on first run,
    or auto-logged-in from secrets.toml on the settings path.
    """
    cloud = await _cloud_mowers()
    proxies = await discover(timeout=2.5)

    # The HC33's BLE scan is lazy — bonded_name stays "none" until a TCP client
    # connects (which triggers connect_mower → the scan).  For any proxy that
    # reported "none", open a brief TCP connection to kick the scan, give it a
    # moment to find + cache the mower's name, then re-broadcast.  This makes
    # first-run onboarding work without the user manually connecting first.
    unbonded = [
        p for p in proxies
        if not p.get("bonded_name") or p["bonded_name"] == "none"
    ]
    if unbonded:
        await asyncio.gather(*(
            poke_proxy(p["ip"], int(p.get("proxy_port", 9876))) for p in unbonded
        ))
        # Scan early-exits sub-second on the strong RSSI of a glued-on mower;
        # 4 s covers the GATT-connect tail before the name is readable.
        await asyncio.sleep(4.0)
        proxies = await discover(timeout=2.5)

    # Index bonded proxies by the mower name they're attached to.  "none" still
    # means unbonded after the poke — mower off or out of range.
    bonded = {
        p["bonded_name"]: p
        for p in proxies
        if p.get("bonded_name") and p["bonded_name"] != "none"
    }

    matched: list[dict] = []
    unmatched_mowers: list[dict] = []
    used_chips: set[str] = set()
    for m in cloud:
        p = bonded.get(m["name"])
        if p:
            matched.append({
                "name":      m["name"],
                "nickname":  m.get("nickname"),
                "iot_id":    m["iot_id"],
                "hc33_host": p["ip"],
                "hc33_port": int(p.get("proxy_port", 9876)),
                "chip_id":   p.get("chip_id"),
                "variant":   p.get("variant"),
            })
            used_chips.add(p.get("chip_id"))
        else:
            unmatched_mowers.append(m)

    unmatched_proxies = [p for p in proxies if p.get("chip_id") not in used_chips]
    return {
        "matched":           matched,
        "unmatched_mowers":  unmatched_mowers,   # cloud mowers with no proxy found
        "unmatched_proxies": unmatched_proxies,  # proxies with no/none/foreign bond
    }


@app.post("/api/onboard/save")
async def onboard_save(payload: dict = Body(...)):
    """Persist secrets.toml + mowers.toml and bring the new roster up live.

    Body: {"mowers": [{name, nickname?, iot_id?, hc33_host, hc33_port?}, ...],
            "email"?: str, "password"?: str}
    Credentials fall back to the ones captured at /api/onboard/login.
    """
    rows = payload.get("mowers") or []
    norm: list[dict] = []
    for m in rows:
        name = (m.get("name") or "").strip()
        host = (m.get("hc33_host") or "").strip()
        if not name or not host:
            raise HTTPException(400, f"mower entry missing name or hc33_host: {m!r}")
        norm.append({
            "name":      name,
            "nickname":  ((m.get("nickname") or "").strip() or None),
            "hc33_host": host,
            "hc33_port": int(m.get("hc33_port") or 9876),
            "iot_id":    (m.get("iot_id") or None),
        })
    if not norm:
        raise HTTPException(400, "no mowers to save")

    email = (payload.get("email") or state.onboard_email or "").strip()
    password = payload.get("password") or state.onboard_password or ""
    if email and password:
        persist.save_secrets(email, password)

    persist.save_mowers(norm)
    await _apply_mowers(norm)

    # Clear the in-memory onboarding creds now that they're on disk.
    state.onboard_email = None
    state.onboard_password = None
    return {"ok": True, "count": len(norm)}


# ── WebSocket: joystick + server-side dead-man ───────────────────────────────
@app.websocket("/ws/joystick/{name}")
async def joystick_ws(ws: WebSocket, name: str):
    await ws.accept()
    h = state.handles.get(name)
    if h is None:
        await ws.close(code=1003, reason="unknown mower")
        return
    state.websockets.add(ws)

    stopped = True   # nothing in motion yet
    lock = asyncio.Lock()

    async def send_stop():
        nonlocal stopped
        async with lock:
            if not stopped:
                await h.send_raw(h.commands.stop_and_not_save_task())
                stopped = True

    try:
        while True:
            data = await ws.receive_json()
            # data: {"x": -1..1, "y": -1..1, "force": 0..1}
            x = float(data.get("x", 0.0))
            y = float(data.get("y", 0.0))
            force = float(data.get("force", 0.0))

            # In nipplejs, y is positive UP (joystick pushed away from user).
            # Stick forward → move_forward; stick back → move_back.  Names match
            # actual mower motion — the Stage-1 "inversion" memory was wrong.
            if force < DEAD_ZONE:
                await send_stop()
                continue

            stopped = False
            # Combined linear + angular in ONE command so forward and turning
            # happen simultaneously (arc / diagonal), like the official app —
            # DrvMotionCtrl carries both speeds.  Previously we picked a single
            # axis (abs(y) >= abs(x)) and the move_* helpers zeroed the other,
            # so you could only go straight OR turn.  Feeding both axes through
            # the mower's own transform keeps the scaling identical to the old
            # single-axis helpers (linear + = fwd, angular + = right).
            lp = get_percent(min(abs(y), MAX_LINEAR) * 100)
            ap = get_percent(min(abs(x), MAX_ANGULAR) * 100)
            linear_speed, angular_speed = transform_both_speeds(
                90.0 if y >= 0 else 270.0,   # forward / back
                0.0 if x >= 0 else 180.0,    # right / left
                lp, ap,
            )
            async with lock:
                await h.send_raw(
                    h.commands.send_movement(
                        linear_speed=linear_speed, angular_speed=angular_speed
                    )
                )

    except WebSocketDisconnect:
        _LOGGER.info("joystick ws disconnected for %s", name)
    finally:
        state.websockets.discard(ws)
        # Always stop on disconnect — explicit safety in case the browser closed
        # mid-motion without sending a stick-release frame.
        with contextlib.suppress(Exception):
            await h.send_raw(h.commands.stop_and_not_save_task())


# ── Entry point for `python app.py` ──────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    # timeout_graceful_shutdown caps how long uvicorn waits for in-flight
    # connections (incl. open WebSockets that other shutdown paths missed)
    # before closing them.  Without it, Ctrl-C on Windows can wedge forever.
    uvicorn.run(app, host="0.0.0.0", port=8000, timeout_graceful_shutdown=2)
