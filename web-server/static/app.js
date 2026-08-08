// Luba Remote — client glue.
//
// Flow:
//   1. /api/mowers populates the selector.
//   2. Choosing a mower opens a WebSocket to /ws/joystick/{name} and wires
//      nipplejs to send {x, y, force} frames at ~6.5 Hz.
//   3. Start Camera button POSTs /api/camera/{name}/start, gets Agora token,
//      joins the channel.  Stop Camera reverses it.
//   4. Pause/Resume/Dock/Undock/STOP map to /api/action/{name}/{action}.
//
// The page is an app shell, not a document: on phones one tab's panels are in
// the flow at a time (body[data-tab]), on ≥900px every panel is laid out at
// once.  Tab switching is pure show/hide — nothing here navigates, and no
// mower I/O depends on which tab is showing.
//
// Three invariants this file exists to protect, because the app is a remote
// control for a machine with spinning blades:
//   A. Nothing on the page may end up permanently unresponsive.  Every await is
//      under a deadline, every busy flag is cleared in a finally, and every
//      timer has exactly one owner that can cancel it.
//   B. The joystick may never *look* usable while it cannot send.  The socket's
//      real state drives the zone hint, the DRIVING glow and the System row.
//   C. Losing the link stops the mower.  The server sends stop_and_not_save_task
//      on ws disconnect; we additionally stop on blur / pagehide / hidden and
//      re-assert a zero frame whenever a fresh socket opens idle.

const $ = (id) => document.getElementById(id);

const els = {
  // chrome
  mower:       $("mower"),
  status:      $("status"),
  statusText:  $("status-text"),
  battery:     $("battery"),
  batteryPct:  $("battery-pct"),
  batteryFill: $("battery-fill"),
  mowerStatus: $("mower-status"),
  mowerError:  $("mower-error"),
  mowerErrorText: $("mower-error-text"),
  themeToggle: $("theme-toggle"),
  themeSeg:    $("theme-seg"),
  tabbar:      $("tabbar"),
  tabSystemBadge: $("tab-system-badge"),

  // camera
  video:      $("video"),
  videoFrame: $("video-frame"),
  camLive:    $("cam-live"),
  camStart:   $("cam-start"),
  camStop:    $("cam-stop"),
  compass:        $("compass"),
  compassRing:    $("compass-ring"),
  compassReadout: $("compass-readout"),

  // drive
  joyZone:  $("joystick-zone"),
  joyHint:  $("joy-hint"),
  pause:    $("pause"),
  resume:   $("resume"),
  dock:     $("dock"),
  undock:   $("undock"),
  stop:     $("stop"),
  light:    $("light"),
  lightLabel: $("light-label"),

  // mow
  startJob:      $("start-job"),
  cancelJob:     $("cancel-job"),
  bladesOn:      $("blades-on"),
  bladesOnLabel: $("blades-on-label"),
  bladesOff:     $("blades-off"),
  heightSlider:  $("height-slider"),
  heightVal:     $("height-val"),
  heightReport:  $("height-report"),
  speedSlider:   $("speed-slider"),
  speedVal:      $("speed-val"),
  bladeChip:     $("blade-chip"),
  rpmChip:       $("rpm-chip"),
  progressChip:  $("progress-chip"),
  cutterSeg:     $("cutter-mode-seg"),

  // system
  reconnect:    $("reconnect"),
  sysMower:     $("sys-mower"),
  sysLink:      $("sys-link"),
  sysJoystick:  $("sys-joystick"),
  sysHeartbeat: $("sys-heartbeat"),
  sysBattery:   $("sys-battery"),
  sysState:     $("sys-state"),
  log:       $("log"),
  logDrawer: $("log-drawer"),
  logToggle: $("log-toggle"),

  toastRegion: $("toast-region"),
};

// ── Cheap DOM writes ────────────────────────────────────────────────────────
// The status poll (3 s) and the heading poll (1 Hz) re-render the same handful
// of nodes forever.  Writing a node with the value it already holds is not
// free: textContent replaces the child node, and setAttribute / className /
// classList.remove re-serialise the attribute even when nothing changed — each
// one invalidates style and forces the compositor to repaint whatever sits
// behind it, which on this page includes two fixed backdrop-filter bars.  That
// repaint cost is what makes taps get dropped, so every periodic write goes
// through these guards and only real changes reach the DOM.
const setText = (el, value) => {
  if (!el) return;
  const s = (value === null || value === undefined) ? "" : String(value);
  if (el.textContent !== s) el.textContent = s;
};
const setHidden = (el, hidden) => {
  if (!el) return;
  const h = !!hidden;
  if (el.hidden !== h) el.hidden = h;
};
const setClass = (el, cls, on) => {
  if (!el) return;
  if (el.classList.contains(cls) !== !!on) el.classList.toggle(cls, !!on);
};
const setClassName = (el, value) => {
  if (!el) return;
  if (el.getAttribute("class") !== value) el.setAttribute("class", value);
};
const setAttr = (el, name, value) => {
  if (!el) return;
  if (value === null || value === undefined || value === false) {
    if (el.hasAttribute(name)) el.removeAttribute(name);
    return;
  }
  const s = String(value);
  if (el.getAttribute(name) !== s) el.setAttribute(name, s);
};

// ── Diagnostics log ─────────────────────────────────────────────────────────
// Bounded and deferred.  The old implementation did `log.textContent += …`,
// which is O(total) per line, grows without limit for the life of the tab, and
// then read scrollHeight (a forced synchronous layout) — on every action, every
// poll error and, now, every reconnect attempt.  Lines are buffered and only
// written when the drawer is actually open (#log is display:none otherwise, so
// a closed drawer costs nothing at all).
const LOG_MAX_LINES = 400;
const logPending = [];
let logNodes = 0;

function logDrawerOpen() {
  return els.logDrawer.classList.contains("open");
}

function flushLog() {
  if (!logPending.length || !logDrawerOpen()) return;
  els.log.append(logPending.join("\n") + "\n");
  logPending.length = 0;
  logNodes += 1;
  // Trim from the front so a long session cannot grow the node unboundedly.
  while (logNodes > LOG_MAX_LINES && els.log.firstChild) {
    els.log.removeChild(els.log.firstChild);
    logNodes -= 1;
  }
  els.log.scrollTop = els.log.scrollHeight;
}

const log = (msg) => {
  const t = new Date().toLocaleTimeString();
  logPending.push(`[${t}] ${msg}`);
  // While the drawer is closed the buffer is capped too, so a page left open
  // for days cannot accumulate megabytes of strings.
  if (logPending.length > LOG_MAX_LINES) {
    logPending.splice(0, logPending.length - LOG_MAX_LINES);
  }
  console.log(msg);
  flushLog();
};

// ── Network deadline ────────────────────────────────────────────────────────
// Runs fn(signal) with a hard deadline.  The abort covers the body read too,
// because callers do their r.json()/r.text() *inside* fn — a response whose
// headers arrive and whose body then stalls is the exact shape of hang that
// used to strand a button in aria-busy forever.
async function deadline(ms, fn) {
  const ctl = new AbortController();
  const kill = setTimeout(() => ctl.abort(), ms);
  try {
    return await fn(ctl.signal);
  } finally {
    clearTimeout(kill);
  }
}

const T_ACTION    = 10000;   // /api/action — BLE write + ack
const T_STATUS    = 8000;    // /api/status — must be < the 3 s cadence's patience
const T_HEADING   = 4000;    // /api/heading — 1 Hz, cheap
const T_SET       = 10000;   // /api/set
const T_CAMERA    = 20000;   // /api/camera/*/start — Agora token round-trip
const T_JOIN      = 25000;   // AgoraRTC join()
const T_RECONNECT = 20000;   // /api/reconnect — BLE reconnects are slow

// Map a device_name to its friendly app nickname for display in the log.
// The device_name stays the key everywhere else (selector value, API paths,
// joystick ws) — this is purely cosmetic.  Falls back to the raw name if no
// nickname is configured or mowersList hasn't loaded yet.
const nick = (name) => {
  const m = mowersList.find(m => m.name === name);
  return (m && m.nickname) || name;
};

const STATUS_CLASSES = ["connected", "connecting", "disconnected"];
let statusCls = null;
const setStatus = (text, cls) => {
  setText(els.statusText, text);
  setAttr(els.status, "title", text);   // the chip ellipsizes; full text on hover/long-press
  const next = cls || null;
  if (next === statusCls) return;
  // Remove exactly what we last added (plus the three known ones) so an
  // unexpected availability string can't accumulate on the element.
  for (const c of STATUS_CLASSES) setClass(els.status, c, false);
  if (statusCls && !STATUS_CLASSES.includes(statusCls)) setClass(els.status, statusCls, false);
  statusCls = next;
  if (next) setClass(els.status, next, true);
};

// Render the topbar battery chip.  pct === null → hidden (mower hasn't reported
// yet, or the link is down and we cleared it).  The gauge is inline SVG, not an
// emoji: the fill rect's width tracks the charge and a bolt replaces it while
// charging.  <20% turns it danger-red; charging turns it info-blue.
const LOW_BATTERY_PCT = 20;
const BATTERY_FILL_W = 12;          // px width of #battery-fill at 100% (viewBox units)
const setBattery = (pct, charging) => {
  if (pct === null || pct === undefined) {
    setClass(els.battery, "low", false);
    setClass(els.battery, "charging", false);
    setHidden(els.battery, true);
    setText(els.batteryPct, "");
    return;
  }
  setHidden(els.battery, false);
  setText(els.batteryPct, `${pct}%`);
  const clamped = Math.max(0, Math.min(100, pct));
  setAttr(els.batteryFill, "width", Math.max(1, (clamped / 100) * BATTERY_FILL_W).toFixed(2));
  setClass(els.battery, "charging", !!charging);
  setClass(els.battery, "low", !charging && pct <= LOW_BATTERY_PCT);
};

// Render the mower state chip ("Mowing", "Charging", …) and any fault message
// in the banner under the topbar.  The banner sits OUTSIDE the tab panels so a
// fault is visible whichever tab you are on.  null → hidden.  Cleared when the
// link is down so we never show a stale state.  The full fault text is on the
// title attribute, and tapping the banner expands the clamped message.
const setMowerState = (label, error) => {
  setText(els.mowerStatus, label || "");
  setHidden(els.mowerStatus, !label);
  setText(els.sysState, label || "—");
  setText(els.mowerErrorText, error || "");
  setHidden(els.mowerError, !error);
  setAttr(els.mowerError, "title", error || "Mower fault");
  if (!error) setClass(els.mowerError, "expanded", false);
};

// Visual state of the camera card: "off" → placeholder, "joining" → spinner,
// "live" → video + LIVE chip + accent frame.  Purely cosmetic — it never gates
// the Agora logic.
const setCamUI = (state) => {
  setClass(els.videoFrame, "joining", state === "joining");
  setClass(els.videoFrame, "live", state === "live");
  setHidden(els.camLive, state !== "live");
};

// Cosmetic "driving" affordance on the joystick zone (accent glow + DRIVING
// chip).  Deliberately gated on the socket as well as the heartbeat: lighting
// the zone up while the link is down would be the UI claiming to drive a mower
// it cannot reach.
const setDriving = (on) => {
  setClass(els.joyZone, "active", on);
};
function refreshDriving() {
  setDriving(!!joyTimer && wsIsOpen());
}

let currentMower = null;
let selectGen = 0;         // bumped per selectMower() call — stale runs bail out
let joystick = null;
let agora = null;          // AgoraRTC client when camera is up
let joyTimer = null;       // setInterval re-sending the held joystick state (keeps the mower moving)
let joyState = { x: 0, y: 0, force: 0 };  // latest stick command, re-sent by joyTimer while held

// Joystick socket state.  Declared up here because joystickFailsafeStop() and
// refreshDriving() are reachable from the module-scope boot sequence below.
let ws = null;             // the live socket, or null
let wsMower = null;        // mower the socket belongs to (null → joystick torn down)
let wsGen = 0;             // bumped on every (re)target; stale callbacks bail out
let wsAttempt = 0;         // consecutive failures → backoff index
let wsTimer = null;        // the single pending reconnect timer
let wsConnectGuard = null; // "still CONNECTING?" watchdog for the current socket
let wsLastAttempt = 0;     // Date.now() of the last connect attempt (storm floor)
let wsUiState = null;      // last link state pushed to the DOM

// Polling state.
let statusTimer = null;
let statusMower = null;
let statusInFlight = false;
let headingTimer = null;
let headingMower = null;
let headingInFlight = false;

function wsIsOpen() { return !!ws && ws.readyState === WebSocket.OPEN; }

// ── Tab shell (phone) ───────────────────────────────────────────────────────
// Show/hide only.  Drive is always the boot tab: driving while watching the
// camera is the primary use case and must be one tap away with no scrolling.
// ≥900px ignores all of this — CSS shows every panel and hides the tab bar.
const TABS = ["drive", "mow", "system"];

function setTab(name) {
  if (!TABS.includes(name)) return;
  // Defensive: nipplejs listens on `document`, so a release is still seen when
  // the zone is hidden — but a context switch while driving should stop the
  // mower regardless.  No-op unless the heartbeat is actually running.
  joystickFailsafeStop();
  document.body.dataset.tab = name;
  for (const b of els.tabbar.querySelectorAll("button[data-tab]")) {
    const on = b.dataset.tab === name;
    setAttr(b, "aria-selected", String(on));
    b.tabIndex = on ? 0 : -1;
  }
  window.scrollTo(0, 0);
  if (name === "system") flushLog();
}

els.tabbar.addEventListener("click", (e) => {
  const b = e.target.closest("button[data-tab]");
  if (b) setTab(b.dataset.tab);
});
els.tabbar.addEventListener("keydown", (e) => {
  if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
  const i = TABS.indexOf(document.body.dataset.tab);
  const next = TABS[(i + (e.key === "ArrowRight" ? 1 : TABS.length - 1)) % TABS.length];
  setTab(next);
  els.tabbar.querySelector(`button[data-tab="${next}"]`).focus();
  e.preventDefault();
});
setTab("drive");

// ── Theme (window.Theme comes from theme.js, loaded first in <head>) ────────
// One cycling button in the topbar (system → light → dark) plus an explicit
// three-way segmented control on the System tab; both read the same state.
function syncTheme() {
  const mode = window.Theme.get();
  for (const b of els.themeSeg.querySelectorAll("button[data-theme]")) {
    const on = b.dataset.theme === mode;
    setClass(b, "active", on);
    setAttr(b, "aria-pressed", String(on));
  }
  for (const ico of els.themeToggle.querySelectorAll("[data-theme-ico]")) {
    const hide = ico.dataset.themeIco !== mode;
    if (ico.hasAttribute("hidden") !== hide) ico.toggleAttribute("hidden", hide);
  }
  const label = `Theme: ${mode} (showing ${window.Theme.effective()})`;
  setAttr(els.themeToggle, "title", label);
  setAttr(els.themeToggle, "aria-label", label);
}
els.themeToggle.onclick = () => window.Theme.cycle();
els.themeSeg.addEventListener("click", (e) => {
  const b = e.target.closest("button[data-theme]");
  if (b) window.Theme.set(b.dataset.theme);
});
document.documentElement.addEventListener("themechange", syncTheme);
syncTheme();

// Read a design token so JS-driven visuals (the nipplejs stick) stay on the
// palette instead of hardcoding a colour.
const token = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// ── Compass ─────────────────────────────────────────────────────────────────
// Fixed correction applied to the mower's reported heading before it drives the
// compass. Leave 0 unless the compass reads offset from reality (e.g. if the
// firmware's heading is magnetic or grid-relative rather than true north); then
// set this to the number of degrees to add so a known bearing reads correctly.
const COMPASS_OFFSET_DEG = 0;

// Rotate the heading-up compass. `orientation` is degrees (0-359, N=0), or null.
// The forward arrow is fixed pointing up; we rotate the ring by -heading so the
// N marker shows where geographic north is relative to the camera's view.
let compassDeg = null;
function setCompass(orientation) {
  if (orientation === null || orientation === undefined) {
    compassDeg = null;
    setText(els.compassReadout, "—");
    return;
  }
  const deg = (((orientation + COMPASS_OFFSET_DEG) % 360) + 360) % 360;
  // The ring has `transition: transform` and `will-change: transform`, so a
  // re-write at 1 Hz restarts a composited animation even when the heading has
  // not moved.  Only write when it actually changed.
  if (deg !== compassDeg) {
    compassDeg = deg;
    els.compassRing.style.transform = `rotate(${-deg}deg)`;
  }
  setText(els.compassReadout, `${Math.round(deg)}°`);
}

async function pollHeading(name) {
  if (name !== currentMower || !agora) return;   // only while this mower's camera is up
  if (headingInFlight) return;                   // never let 1 Hz ticks stack up
  headingInFlight = true;
  try {
    const h = await deadline(T_HEADING, async (signal) => {
      const r = await fetch(`/api/heading/${encodeURIComponent(name)}`, { signal });
      if (!r.ok) return null;
      return await r.json();
    });
    if (!h || name !== currentMower) return;
    setCompass(h.orientation);
  } catch (_) { /* transient — next tick retries */ } finally {
    headingInFlight = false;
  }
}

const HEADING_MS = 1000;

function scheduleHeading(delay) {
  if (headingTimer) { clearTimeout(headingTimer); headingTimer = null; }
  if (!headingMower || document.hidden) return;
  headingTimer = setTimeout(headingTick, delay);
}

async function headingTick() {
  headingTimer = null;
  const name = headingMower;
  if (!name) return;
  await pollHeading(name);
  // Re-arm only if this poll is still the current one — a mower switch or a
  // camera stop in flight must not resurrect the loop.
  if (headingMower === name) scheduleHeading(HEADING_MS);
}

function startCompass(name) {
  headingMower = name;
  setHidden(els.compass, false);
  scheduleHeading(0);                                    // paint immediately
}

function stopCompass() {
  headingMower = null;
  if (headingTimer) { clearTimeout(headingTimer); headingTimer = null; }
  setHidden(els.compass, true);
  setText(els.compassReadout, "—");
  compassDeg = null;
}

// ── Mower list ──────────────────────────────────────────────────────────────
// Camera availability is per-mower (depends on iot_id being set in mowers.toml).
// We cache the /api/mowers payload so selectMower() can read it without re-fetching.
let mowersList = [];

async function loadMowers() {
  log("loadMowers: fetching /api/mowers…");
  let list = null;
  try {
    list = await deadline(T_ACTION, async (signal) => {
      const r = await fetch("/api/mowers", { cache: "no-store", signal });
      log(`loadMowers: HTTP ${r.status}`);
      if (!r.ok) {
        setStatus(`mower list failed: HTTP ${r.status}`, "disconnected");
        return null;
      }
      return await r.json();
    });
  } catch (e) {
    log(`loadMowers: fetch error: ${e}`);
    setStatus("mower list failed to load", "disconnected");
    return;
  }
  if (!list) return;
  mowersList = list;
  log(`loadMowers: got ${mowersList.length} mower(s)`);
  els.mower.innerHTML = "";
  for (const m of mowersList) {
    const opt = document.createElement("option");
    opt.value = m.name;                       // device_name stays the key
    const label = m.nickname || m.name;       // show the friendly app name when set
    opt.textContent = m.camera ? label : `${label} (no camera)`;
    els.mower.appendChild(opt);
  }
  if (mowersList.length > 0) {
    await selectMower(mowersList[0].name);
  } else {
    setStatus("no mowers configured");
    setText(els.sysMower, "none configured");
    stopStatusPolling();
    teardownJoystickWs();
  }
}

async function selectMower(name) {
  // Re-entrancy guard.  selectMower awaits network calls; without a generation
  // token a slow first run resumes *after* a second one finished and re-points
  // the joystick socket at the mower the user just switched away from.
  const gen = ++selectGen;
  const prev = currentMower;
  currentMower = name;
  setStatus(`connecting ${name}…`, "connecting");
  setText(els.sysMower, nick(name));
  lightOn = false;          // unknown on a fresh mower — assume off
  setLightLabel();
  disarmBlades();           // an arm confirmed after a switch would hit the wrong mower
  // Per-mower slider state: let telemetry seed this mower's height slider once.
  heightTouchedFor = null;
  heightDragging = false;

  // Driving comes up FIRST and never waits on the network.  This used to sit
  // behind `await reconnectMower(...)` (a POST with a 20 s deadline) plus a
  // camera-stop POST with no deadline at all, so on a slow BLE reconnect the
  // joystick simply did not exist for tens of seconds after load — the stick
  // swallowed every drag and then abruptly started working.
  startJoystick(name);
  startStatusPolling(name);

  await stopCamera({ silent: true, target: prev || name });   // previous mower's camera
  if (gen !== selectGen) return;                              // superseded mid-teardown

  const meta = mowersList.find(m => m.name === name);
  const cameraAvailable = meta && meta.camera;
  els.camStart.disabled = !cameraAvailable;
  els.camStop.disabled = true;
  if (!cameraAvailable) {
    log(`(${nick(name)}: no iot_id configured → camera disabled)`);
  }

  // Idempotent — also recovers any mower the lifespan failed to reach at boot.
  // Fire-and-forget: the server registers every configured handle at startup,
  // so the joystick socket and the status poll are valid before this returns.
  reconnectMower(name, { silent: true });
}

els.mower.onchange = (e) => { selectMower(e.target.value); };

// ── Reconnect + status polling ──────────────────────────────────────────────
async function reconnectMower(name, { silent } = {}) {
  if (!silent) log(`reconnecting ${nick(name)}…`);
  // Abort guard: a dead server must not leave the Reconnect button spinning
  // forever (BLE reconnects are slow, so this is generous).
  try {
    const ok = await deadline(T_RECONNECT, async (signal) => {
      const r = await fetch(`/api/reconnect/${encodeURIComponent(name)}`, { method: "POST", signal });
      if (!r.ok) {
        const body = await r.text();
        log(`reconnect failed: ${r.status} ${body}`);
        setStatus(`${name}: reconnect failed`, "disconnected");
        return false;
      }
      if (!silent) log(`reconnect ok (${nick(name)})`);
      return true;
    });
    // Refresh status immediately.  Outside the deadline block on purpose: it
    // has its own, so a slow status read can never hold the Reconnect button
    // busy past the reconnect deadline.
    if (ok) await pollStatus(name);
    return ok;
  } catch (e) {
    log(`reconnect threw: ${e}`);
    setStatus(`${name}: reconnect error`, "disconnected");
    return false;
  }
}

// Thresholds for the "mower silent" indicator (seconds since last LubaMsg).
const MOWER_SLOW_S   = 5;
const MOWER_SILENT_S = 10;

async function pollStatus(name) {
  if (name !== currentMower) return;   // stale tick after mower switch
  // One status read at a time.  The old setInterval fired blind every 3 s, so a
  // link slower than the cadence stacked requests that then landed out of order
  // and made the chips flap.  The nudge re-polls (light, blades, height) piled
  // on top of that.
  if (statusInFlight) return;
  statusInFlight = true;
  try {
    const s = await deadline(T_STATUS, async (signal) => {
      const r = await fetch(`/api/status/${encodeURIComponent(name)}`, { signal });
      if (!r.ok) return null;
      return await r.json();
    });
    if (!s || name !== currentMower) return;

    // Battery reflects the last decoded report; hide it whenever the link to
    // the mower isn't healthy so we never show a stale charge.
    const live = s.availability === "connected";
    const silent = s.mower_silent_s;
    setBattery(live ? s.battery : null, s.charging);
    setMowerState(live ? s.status : null, live ? s.error : null);

    // System tab's connection detail — the same truth as the topbar chips, but
    // spelled out (and readable when the chip has ellipsized).
    setText(els.sysMower, nick(name));
    setText(els.sysLink, s.auto_retrying
      ? "auto-reconnecting…"
      : (s.auto_gave_up ? "gave up — press Reconnect" : String(s.availability)));
    setClassName(els.sysLink, `sys-val ${live ? "ok" : "bad"}`);
    setText(els.sysHeartbeat,
      (silent === null || silent === undefined) ? "—" : `${silent.toFixed(1)} s ago`);
    setText(els.sysBattery,
      (live && typeof s.battery === "number")
        ? `${s.battery}%${s.charging ? " · charging" : ""}`
        : "—");

    // Surface the manual-reconnect prompt on the button itself and badge the
    // System tab, so the nudge is visible from Drive/Mow too (warn-tinted when
    // the auto-retry loop has given up).
    const needsAttention = !live && !!s.auto_gave_up;
    setClass(els.reconnect, "attention", needsAttention);
    setHidden(els.tabSystemBadge, !needsAttention);

    // Reconcile the headlight toggle with the mower's actual state — the
    // firmware auto-offs the light, so trust the server's probe over our
    // optimistic guess.  null means "not probed yet"; keep the local guess.
    // Never overwrite a flip whose POST is still in flight.
    if (typeof s.light_on === "boolean" && s.light_on !== lightOn
        && !isPending("light-on", "light-off")) {
      lightOn = s.light_on;
      setLightLabel();
    }

    // Scale the settings sliders to this model's real limits (available once
    // the mower has reported its model info).
    if (s.limits && limitsFor !== name) {
      limitsFor = name;
      els.heightSlider.min = s.limits.blade_height.min;
      els.heightSlider.max = s.limits.blade_height.max;
      els.speedSlider.min  = s.limits.working_speed.min;
      els.speedSlider.max  = s.limits.working_speed.max;
    }
    // Blade-height telemetry.  The slider is a COMMAND input: telemetry seeds
    // it once per mower and never moves it again after the user touches it —
    // the old snap-the-slider-every-poll behavior made every set look like it
    // "jumped back" whenever the mower served a stale reading.  The mower's
    // own reading lives in the separate #height-report readout instead:
    // "adjusting…" while the height motor runs, then "mower: N mm", tinted by
    // whether it matches the slider.
    if (typeof s.blade_height === "number") {
      if (heightTouchedFor !== name && !heightDragging) {
        if (Number(els.heightSlider.value) !== s.blade_height) {
          els.heightSlider.value = s.blade_height;
        }
        setText(els.heightVal, `${s.blade_height} mm`);
      }
      if (s.blade_adjusting) {
        setText(els.heightReport, "adjusting…");
        setClassName(els.heightReport, "setting-sub adjusting");
      } else {
        setText(els.heightReport, `mower: ${s.blade_height} mm`);
        const match = Number(els.heightSlider.value) === s.blade_height;
        setClassName(els.heightReport, "setting-sub" + (match ? " match" : " differs"));
      }
    } else {
      setText(els.heightReport, "");
    }

    // Blade telemetry: the chip shows the mower's own "disc rotating" sensor
    // bit — ground truth, so a commanded-but-refused blades-on shows OFF here.
    // The pulse only runs when the disc is genuinely spinning.
    if (typeof s.blades_on === "boolean") {
      setText(els.bladeChip, `Blades: ${s.blades_on ? "ON" : "OFF"}`);
      setClass(els.bladeChip, "blades-live", s.blades_on);
    } else {
      setText(els.bladeChip, "Blades: —");
      setClass(els.bladeChip, "blades-live", false);
    }
    const rpm = s.cutter_rpm;
    const rpmShow = typeof rpm === "number" && rpm > 0;
    setHidden(els.rpmChip, !rpmShow);
    if (rpmShow) setText(els.rpmChip, `${rpm} rpm`);
    // Mark the active blade-speed preset from the mower's read-back.
    if (typeof s.cutter_mode === "number") {
      for (const b of els.cutterSeg.querySelectorAll(".seg-btn")) {
        const on = Number(b.dataset.mode) === s.cutter_mode;
        setClass(b, "active", on);
        setAttr(b, "aria-pressed", String(on));
      }
    }
    // Job progress chip (only meaningful mid-job).
    const pct = s.mow_percent;
    const pctShow = typeof pct === "number" && pct > 0 && pct <= 100;
    setHidden(els.progressChip, !pctShow);
    if (pctShow) setText(els.progressChip, `Job ${pct}%`);

    // Layer 1: HC33 TCP/HaLow socket.  If this is bad, BLE is irrelevant.
    if (s.availability !== "connected") {
      if (s.auto_retrying) {
        setStatus(`${nick(name)}: auto-reconnecting…`, "connecting");
      } else if (s.auto_gave_up) {
        setStatus(`${nick(name)}: gave up — click Reconnect`, "disconnected");
      } else {
        setStatus(`${nick(name)}: HC33 ${s.availability}`, s.availability);
      }
      return;
    }

    // Layer 2: have we ever heard back from the mower over BLE?
    if (silent === null) {
      setStatus(`${name}: HC33 ok · waiting for mower`, "connecting");
    } else if (silent > MOWER_SILENT_S) {
      setStatus(`${name}: mower silent ${silent.toFixed(0)}s — is it on?`, "disconnected");
    } else if (silent > MOWER_SLOW_S) {
      setStatus(`${name}: mower slow ${silent.toFixed(0)}s`, "connecting");
    } else {
      setStatus(`${name}: mower ok (${silent.toFixed(1)}s)`, "connected");
    }
  } catch (_) { /* network blip — next tick will retry */ } finally {
    statusInFlight = false;
  }
}

// Self-scheduling poll rather than setInterval: the next tick is only armed
// once the previous one has settled, so a slow link stretches the cadence
// instead of queueing work, and exactly one timer can ever be outstanding.
const STATUS_MS = 3000;

function startStatusPolling(name) {
  statusMower = name;
  scheduleStatus(0);
}

function stopStatusPolling() {
  statusMower = null;
  if (statusTimer) { clearTimeout(statusTimer); statusTimer = null; }
}

function scheduleStatus(delay) {
  if (statusTimer) { clearTimeout(statusTimer); statusTimer = null; }
  if (!statusMower || document.hidden) return;   // hidden → paused, resumed on return
  statusTimer = setTimeout(statusTick, delay);
}

async function statusTick() {
  statusTimer = null;
  const name = statusMower;
  if (!name) return;
  await pollStatus(name);
  if (statusMower === name) scheduleStatus(STATUS_MS);
}

// Fired-and-forgotten follow-up polls (light / blades / height read-backs).
// Guarded on the mower so a switch mid-wait can't repaint with another mower's
// telemetry, and pollStatus's in-flight lock keeps them from stacking.
function repoll(target, delays) {
  for (const ms of delays) {
    setTimeout(() => { if (currentMower === target) pollStatus(target); }, ms);
  }
}

els.reconnect.onclick = async () => {
  if (!currentMower || pending.has("reconnect")) return;   // in-flight guard
  pending.add("reconnect");
  setBusy(els.reconnect, true);
  let ok = false;
  try {
    ok = await reconnectMower(currentMower, {});
  } catch (e) {
    log(`reconnect handler threw: ${e}`);
  } finally {
    pending.delete("reconnect");
    setBusy(els.reconnect, false);
    flashResult(els.reconnect, ok);
    toast(ok ? "Reconnected" : "Reconnect failed", ok);
  }
  // A fresh HC33 link is also the moment to re-check the joystick socket.
  ensureJoystickWs();
};

// ── Joystick link (WebSocket) ───────────────────────────────────────────────
// Mobile browsers close WebSockets aggressively — screen lock, tab background,
// Wi-Fi→cellular roam, bfcache.  The old code only logged onclose, and
// sendJoystick silently dropped every frame afterwards, so the stick looked
// perfectly normal while commanding nothing until the page was reloaded.  That
// is the "works every 10th time" report.
//
// Recovery rules:
//   * exactly one pending retry timer, ever (wsTimer) — no storms;
//   * exponential backoff with jitter, reset by any user-visible event;
//   * a connect that never completes is force-closed after WS_CONNECT_MS so it
//     cannot sit in CONNECTING forever and block every later attempt;
//   * every callback checks the generation token, so a socket for a mower the
//     user has switched away from can never be revived or adopted;
//   * nothing reconnects while the document is hidden (that is the failsafe's
//     territory) — visibilitychange→visible re-arms it.
const WS_BACKOFF_MS   = [500, 1000, 2000, 4000, 8000, 15000];
const WS_DEAD_BACKOFF = 30000;   // close code 1003 = server doesn't know this mower
const WS_ATTEMPT_FLOOR_MS = 400; // two connect attempts can never be closer than this
const WS_CONNECT_MS   = 8000;    // give up on a half-open upgrade
const WS_STALE_MS     = 20000;   // hidden longer than this → cycle rather than trust

const ZERO_FRAME = { x: 0, y: 0, force: 0 };

function wsUrl(name) {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws/joystick/${encodeURIComponent(name)}`;
}

// The single place the socket's real state reaches the UI.  Cached, so the 6.5
// Hz command path can call it freely without touching the DOM.
const JOY_HINT = {
  open:       "Hold & drag to drive",
  connecting: "Connecting to mower…",
  down:       "Link lost — reconnecting…",
  missing:    "Joystick unavailable",
  idle:       "Hold & drag to drive",
};
function linkState() {
  if (!wsMower) return "idle";
  if (!nippleReady) return "missing";
  if (!ws) return "down";
  if (ws.readyState === WebSocket.OPEN) return "open";
  if (ws.readyState === WebSocket.CONNECTING) return "connecting";
  return "down";
}
function setLinkUI() {
  const st = linkState();
  if (st === wsUiState) return;
  wsUiState = st;
  const label = {
    idle: "—", open: "connected", connecting: "connecting…",
    down: "lost — reconnecting…", missing: "library not loaded",
  }[st];
  setText(els.sysJoystick, label);
  setClassName(els.sysJoystick, "sys-val" + (st === "open" ? " ok" : (st === "idle" ? "" : " bad")));
  setText(els.joyHint, JOY_HINT[st]);
  // The zone must not read as usable while it cannot send.
  setAttr(els.joyZone, "aria-disabled", st === "open" || st === "idle" ? null : "true");
  refreshDriving();
}

function scheduleWsRetry(gen, delayOverride) {
  if (gen !== wsGen || !wsMower) return;
  if (wsTimer) return;                 // storm guard: one outstanding retry only
  let d = delayOverride;
  if (d === undefined) {
    d = WS_BACKOFF_MS[Math.min(wsAttempt, WS_BACKOFF_MS.length - 1)] + Math.random() * 250;
    wsAttempt += 1;
  }
  wsTimer = setTimeout(() => { wsTimer = null; connectJoystickWs(gen); }, d);
}

function connectJoystickWs(gen) {
  if (gen !== wsGen || !wsMower) return;                    // stale target
  if (!nippleReady) { setLinkUI(); return; }
  if (ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) return;
  if (document.hidden) { setLinkUI(); return; }             // resumed on visible
  if (navigator.onLine === false) { setLinkUI(); return; }  // resumed on `online`

  const now = Date.now();
  if (now - wsLastAttempt < WS_ATTEMPT_FLOOR_MS) {
    scheduleWsRetry(gen, WS_ATTEMPT_FLOOR_MS);
    return;
  }
  wsLastAttempt = now;

  const name = wsMower;
  let sock;
  try {
    sock = new WebSocket(wsUrl(name));
  } catch (e) {
    log(`joystick ws create failed: ${e}`);
    scheduleWsRetry(gen);
    return;
  }
  ws = sock;
  setLinkUI();

  // A WebSocket upgrade that never completes (captive portal, roamed Wi-Fi)
  // would otherwise leave readyState CONNECTING indefinitely, and every later
  // ensureJoystickWs() would see "already connecting" and do nothing.
  if (wsConnectGuard) clearTimeout(wsConnectGuard);
  wsConnectGuard = setTimeout(() => {
    wsConnectGuard = null;
    if (sock === ws && sock.readyState === WebSocket.CONNECTING) {
      log("joystick ws connect timed out — retrying");
      try { sock.close(); } catch (_) { /* onclose drives the retry */ }
    }
  }, WS_CONNECT_MS);

  sock.onopen = () => {
    if (wsConnectGuard) { clearTimeout(wsConnectGuard); wsConnectGuard = null; }
    if (gen !== wsGen || sock !== ws) { try { sock.close(); } catch (_) {} return; }
    wsAttempt = 0;
    log(`joystick ws open (${nick(name)})`);
    setLinkUI();
    // Assert "stopped" on a fresh socket unless the stick is genuinely held —
    // the server treats force < DEAD_ZONE as stop_and_not_save_task and no-ops
    // when it is already stopped, so this is free and closes the window where a
    // reconnect could inherit an unknown motion state.
    if (!joyTimer) { try { sock.send(JSON.stringify(ZERO_FRAME)); } catch (_) {} }
  };

  sock.onclose = (ev) => {
    if (wsConnectGuard) { clearTimeout(wsConnectGuard); wsConnectGuard = null; }
    if (sock !== ws) return;              // a superseded socket finishing up
    ws = null;
    setLinkUI();                          // drops the DRIVING glow immediately
    if (gen !== wsGen) return;            // torn down on purpose — do not revive
    // The heartbeat is deliberately left running: the server stops the mower on
    // disconnect, and if the user is still holding the stick a successful
    // reconnect must resume motion (nipplejs only emits "move" on movement, so
    // clearing joyTimer here would leave a held stick permanently dead).
    if (ev && ev.code === 1003) {
      log(`joystick ws refused (unknown mower ${name}) — backing off`);
      scheduleWsRetry(gen, WS_DEAD_BACKOFF);
      return;
    }
    if (wsAttempt === 0) log(`joystick ws closed (${ev ? ev.code : "?"}) — reconnecting`);
    scheduleWsRetry(gen);
  };

  sock.onerror = () => { if (sock === ws && wsAttempt === 0) log("joystick ws error"); };
}

// "Try now" entry point for the events that mean the network situation just
// changed.  Cheap and idempotent — the attempt floor inside connectJoystickWs
// keeps a burst of events from becoming a burst of sockets.
function ensureJoystickWs({ force = false } = {}) {
  if (!wsMower || document.hidden) return;
  if (!force && ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) return;
  if (force && ws) {
    // A socket can report OPEN long after the connection underneath it died
    // (classic Wi-Fi roam zombie); there is no server-side pong to probe with,
    // so after a real network transition we cycle rather than trust it.
    const sock = ws;
    ws = null;
    sock.onopen = sock.onclose = sock.onerror = null;
    try { sock.close(); } catch (_) {}
  }
  wsAttempt = 0;                                 // a user-visible event: retry now
  if (wsTimer) { clearTimeout(wsTimer); wsTimer = null; }
  connectJoystickWs(wsGen);
}

// Rate-limited nudge from the command path: while the user is actually holding
// a dead stick we want to reconnect fast, but not once per 150 ms frame.
let lastNudge = 0;
function nudgeJoystickWs() {
  const now = Date.now();
  if (now - lastNudge < 2000) return;
  lastNudge = now;
  ensureJoystickWs();
}

// Retire the current socket for good: stop the outgoing mower on its own
// socket, detach every callback, cancel every timer, and bump the generation so
// no in-flight callback or pending retry can resurrect it.
function teardownJoystickWs() {
  wsGen += 1;
  wsMower = null;
  wsAttempt = 0;
  if (wsTimer) { clearTimeout(wsTimer); wsTimer = null; }
  if (wsConnectGuard) { clearTimeout(wsConnectGuard); wsConnectGuard = null; }
  const sock = ws;
  ws = null;
  if (sock) {
    sock.onopen = sock.onclose = sock.onerror = sock.onmessage = null;
    try {
      if (sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify(ZERO_FRAME));
    } catch (_) {}
    try { sock.close(); } catch (_) {}
  }
  setLinkUI();
}

// ── Joystick ───────────────────────────────────────────────────────────────
// The mower self-limits each speed command — it moves a short distance/time
// then stops — so one frame per stick movement isn't enough to keep it going.
// nipplejs only emits "move" when the stick actually MOVES (verified: no
// repeat-while-held option in 0.10.1), so a motionless held stick used to fall
// silent and the mower would halt ("hold it up and it stops").  Fix: latch the
// current stick state and RE-SEND it on an interval while held.  REPEAT_MS must
// be shorter than the mower's per-command window; 150 ms (~6.5 Hz) is the rate
// that already sustained motion during active drags.
const REPEAT_MS = 150;   // ~6.5 Hz

// nipplejs comes from a CDN.  A phone on the mower's LAN with no route to the
// internet loads the page fine and then has no joystick at all — intermittently,
// depending on whether the script was cached.  Detect it, say so, and retry in
// the background instead of throwing out of selectMower (which used to take the
// status poll and the camera wiring down with it).
const NIPPLE_SRC = "https://cdn.jsdelivr.net/npm/nipplejs@0.10.1/dist/nipplejs.min.js";
let nippleReady = typeof nipplejs !== "undefined";
let nippleLoading = false;
let nippleTries = 0;

function loadNipple() {
  if (nippleReady || nippleLoading || nippleTries >= 5) return;
  nippleLoading = true;
  nippleTries += 1;
  const s = document.createElement("script");
  s.src = NIPPLE_SRC;
  s.async = true;
  s.onload = () => {
    nippleLoading = false;
    nippleReady = typeof nipplejs !== "undefined";
    log(nippleReady ? "joystick library loaded" : "joystick library loaded but empty");
    if (nippleReady && wsMower) {
      mountStick();
      setLinkUI();
      connectJoystickWs(wsGen);
    }
  };
  s.onerror = () => {
    nippleLoading = false;
    log(`joystick library failed to load (attempt ${nippleTries}) — retrying`);
    setTimeout(loadNipple, Math.min(30000, 2000 * nippleTries));
  };
  document.head.appendChild(s);
}

function mountStick() {
  if (!nippleReady || joystick) return;
  // Dynamic mode: the stick is created under the finger on each touch, so the
  // initial touch is always neutral (force 0) and motion is measured relative
  // to where you touched.  Static mode pins the stick to the centre and treats
  // the touch point's offset from centre as an instant command — on a phone
  // your thumb lands in the lower half, so it jumped straight to "back/down".
  // dynamicPage recomputes the zone offset per interaction so a scrolled or
  // transformed page can't skew the coordinates either.
  try {
    joystick = nipplejs.create({
      zone: els.joyZone,
      mode: "dynamic",
      dynamicPage: true,
      color: token("--accent") || "currentColor",   // CSS re-tints it on theme flip
      size: 180,
    });
  } catch (e) {
    joystick = null;
    log(`joystick create failed: ${e}`);
    return;
  }

  joystick.on("move", (_evt, data) => {
    // nipplejs's data.vector has y positive UP (it negates internally).
    // angle.radian uses screen-clockwise-from-right, so sin(angle) for "up"
    // gives -1 — wrong sign for our server.  Stick with vector.
    joyState = {
      x: data.vector.x,
      y: data.vector.y,
      force: Math.min(data.force, 1),
    };
    if (!joyTimer) {
      sendJoystick(joyState);                                           // first frame immediately
      joyTimer = setInterval(() => sendJoystick(joyState), REPEAT_MS);  // then heartbeat the held state
    }
    refreshDriving();
  });

  joystick.on("end", () => {
    if (joyTimer) { clearInterval(joyTimer); joyTimer = null; }
    joyState = { x: 0, y: 0, force: 0 };
    sendJoystick(joyState);                                             // explicit stop on release
    refreshDriving();
  });
}

function startJoystick(name) {
  // Stop whatever is moving BEFORE the socket goes away, so the outgoing mower
  // gets an explicit zero frame rather than relying on the server's
  // disconnect handler alone.
  if (joyTimer) { clearInterval(joyTimer); joyTimer = null; }
  joyState = { x: 0, y: 0, force: 0 };
  setDriving(false);
  teardownJoystickWs();          // sends the zero frame, then closes + bumps wsGen

  if (joystick) { try { joystick.destroy(); } catch (_) {} joystick = null; }

  wsMower = name;
  wsUiState = null;              // force one repaint of the link row for the new mower
  setLinkUI();
  connectJoystickWs(wsGen);

  if (!nippleReady) { loadNipple(); return; }
  mountStick();
}

// Returns true iff the frame actually went out.  A dropped frame is never
// silent any more: the zone hint and the System row already show the link is
// down, and an attempt to command motion also raises a (throttled) toast and
// asks for an immediate reconnect.
let lastLinkToast = 0;
function sendJoystick(payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(payload));
      return true;
    } catch (e) {
      log(`joystick send failed: ${e}`);
    }
  }
  if (payload && payload.force > 0) {
    const now = Date.now();
    if (now - lastLinkToast > 4000) {
      lastLinkToast = now;
      toast("Joystick link lost — reconnecting", false);
    }
    nudgeJoystickWs();
  }
  setLinkUI();
  return false;
}

// Safety: if the tab loses focus or is hidden while the stick is held (alt-tab,
// the pointer leaves the window so "end" never fires, or the phone screen locks
// / the browser is backgrounded), kill the re-send heartbeat and command a stop.
// Without this the mower could keep driving on the last speed while you're not
// looking.  Registered once at load; joyTimer/joyState are module-scope.
//
// Note the ordering contract with the reconnect logic below: the failsafe owns
// the hidden/blur direction and never touches the socket (it needs it to send
// the stop); the reconnect logic owns the visible direction and never touches
// the heartbeat.
function joystickFailsafeStop() {
  if (!joyTimer) return;                 // only act if we're actively driving
  clearInterval(joyTimer);
  joyTimer = null;
  joyState = { x: 0, y: 0, force: 0 };
  sendJoystick(joyState);
  setDriving(false);
}
window.addEventListener("blur", joystickFailsafeStop);
window.addEventListener("pagehide", () => {
  joystickFailsafeStop();
  disarmBlades();
});

let hiddenSince = 0;
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    joystickFailsafeStop();   // FIRST: stop the mower before anything else
    disarmBlades();           // an arm that survives a screen lock is not a confirmation
    hiddenSince = Date.now();
    // Polling pauses; the socket is left alone so the stop frame above can go
    // out and so a brief backgrounding doesn't cost a reconnect.
    if (statusTimer) { clearTimeout(statusTimer); statusTimer = null; }
    if (headingTimer) { clearTimeout(headingTimer); headingTimer = null; }
    return;
  }
  // Back in front of the user.  Anything longer than a glance means the socket
  // may be a zombie, so cycle it rather than trust readyState.
  const away = hiddenSince ? Date.now() - hiddenSince : 0;
  hiddenSince = 0;
  ensureJoystickWs({ force: away > WS_STALE_MS });
  scheduleStatus(0);
  scheduleHeading(0);
  flushLog();
});

window.addEventListener("pageshow", (e) => {
  // bfcache restore: the socket is always dead, whatever readyState claims.
  ensureJoystickWs({ force: !!e.persisted });
  scheduleStatus(0);
  scheduleHeading(0);
});
window.addEventListener("online", () => {
  log("network back online");
  ensureJoystickWs({ force: true });
  scheduleStatus(0);
});
window.addEventListener("offline", () => {
  log("network offline");
  setLinkUI();
});

// ── Actions ─────────────────────────────────────────────────────────────────
// Raw POST for a named mower action.  Returns true iff the server said OK;
// failures are logged to the diagnostics drawer.  The 10 s abort guard means a
// hung request can never leave a button stuck in its busy state — and it covers
// the body read as well as the headers, so a stalled response body cannot
// outlive it either.
async function action(name) {
  const target = currentMower;
  if (!target) return false;
  log(`action: ${name}`);
  try {
    return await deadline(T_ACTION, async (signal) => {
      const r = await fetch(`/api/action/${encodeURIComponent(target)}/${name}`, { method: "POST", signal });
      if (!r.ok) {
        log(`action ${name} failed: ${r.status} ${await r.text()}`);
        return false;
      }
      return true;
    });
  } catch (e) {
    log(`action ${name} threw: ${e}`);
    return false;
  }
}

// ── Action feedback (busy spinner, de-dupe, settle flash, toasts) ───────────
// One wrapper (runAction) gives every action button the same lifecycle:
//   - the action name goes into `pending` so a double-tap can't double-fire
//     the POST (the reported blades bug);
//   - the button shows an inline spinner (aria-busy; theme.css swaps the icon
//     slot for a spinner and ignores clicks) while the request is out;
//   - on settle it flashes green/red and a small toast appears above the
//     STOP bar.  Failures are already logged by action().
//
// `pending` + aria-busy are the two ways a control can be bricked, so the rule
// is absolute: whatever is added to `pending` or given aria-busy is released in
// a `finally`, and every awaited call inside is under a deadline.  A thrown
// handler, an aborted fetch and a mower switch mid-flight all land in the same
// cleanup path.
//
// The STOP button is deliberately exempt from the pointer-lock half of that:
// it is a .btn-stop, not a .btn, and style.css re-asserts pointer-events on it
// while busy.  An emergency control must never stop accepting presses; the
// `pending` set alone de-dupes it.

const pending = new Set();               // action names with a POST in flight
const isPending = (...names) => names.some((n) => pending.has(n));

function setBusy(btn, on) {
  if (!btn) return;
  if (on) setAttr(btn, "aria-busy", "true");
  else setAttr(btn, "aria-busy", null);
}

function flashResult(btn, ok) {
  if (!btn) return;
  btn.classList.remove("flash-ok", "flash-fail");
  void btn.offsetWidth;                  // restart the CSS animation
  const cls = ok ? "flash-ok" : "flash-fail";
  btn.classList.add(cls);
  setTimeout(() => btn.classList.remove(cls), 700);
}

// Toast: bottom-centre, above the STOP bar, auto-dismisses.  The icon is inline
// SVG (static markup, never user data — the message itself is a text node).
const TOAST_ICON = {
  ok: '<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6.5 9.3 17.2 4 11.9"/></svg>',
  err: '<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 7.5v5.2M12 16.4v.01"/></svg>',
};
function toast(msg, ok = true) {
  const t = document.createElement("div");
  t.className = `toast ${ok ? "toast-ok" : "toast-fail"}`;
  t.innerHTML = ok ? TOAST_ICON.ok : TOAST_ICON.err;
  t.appendChild(document.createTextNode(msg));
  els.toastRegion.appendChild(t);
  setTimeout(() => {
    t.classList.add("out");
    setTimeout(() => t.remove(), 300);   // after the fade-out animation
  }, 2500);
}

const ACTION_LABELS = {
  "pause": "Paused", "resume": "Resumed", "dock": "Docking", "undock": "Undocking",
  "stop": "STOP sent", "start-job": "Mowing started", "cancel-job": "Job cancelled",
  "blades-on": "Blades on", "blades-off": "Blades off",
  "light-on": "Light on", "light-off": "Light off",
};

async function runAction(name, btn, { withToast = true } = {}) {
  if (!currentMower || pending.has(name)) return false;   // in-flight de-dupe
  pending.add(name);
  setBusy(btn, true);
  let ok = false;
  try {
    ok = await action(name);
  } catch (e) {
    // action() already swallows its own failures; this covers anything the
    // wrapper itself could throw so the button can never stay busy.
    log(`runAction ${name} threw: ${e}`);
  } finally {
    pending.delete(name);
    setBusy(btn, false);
    flashResult(btn, ok);
    if (withToast) toast(ok ? (ACTION_LABELS[name] || name) : `${name} failed`, ok);
  }
  return ok;
}

els.pause.onclick  = () => { runAction("pause", els.pause); };
els.resume.onclick = () => { runAction("resume", els.resume); };
els.dock.onclick   = () => { runAction("dock", els.dock); };
els.undock.onclick = () => { runAction("undock", els.undock); };
// STOP also cuts the blades: stop_and_not_save_task halts motion/task, but a
// manually-enabled blade state is separate — an emergency stop should end both.
// Both POSTs fire in parallel; repeat presses while one is in flight are
// no-ops via `pending` (the STOP button itself is never pointer-locked).
//
// It also kills the joystick heartbeat.  Without that, hitting STOP with one
// hand while the other still holds the stick let the 150 ms re-send command
// motion again ~150 ms after the stop landed.
els.stop.onclick   = () => {
  joystickFailsafeStop();
  disarmBlades();
  runAction("stop", els.stop);
  runAction("blades-off", els.bladesOff, { withToast: false });
};

// Headlight toggle.  The press is optimistic (flip + send immediately) for a
// snappy feel; pollStatus then reconciles `lightOn` against the mower's real
// state, which the server re-probes every ~6 s (the firmware auto-offs the
// light).  The label shows what pressing will *do*: "Light On" when we believe
// it's off, "Light Off" when it's on.
let lightOn = false;
function setLightLabel() {
  setText(els.lightLabel, lightOn ? "Light Off" : "Light On");
}
els.light.onclick = async () => {
  if (isPending("light-on", "light-off")) return;   // don't stack a double-tap
  const target = currentMower;
  if (!target) return;                              // else the flip sticks with no POST
  const want = !lightOn;
  lightOn = want;              // optimistic flip for instant feedback
  setLightLabel();
  const ok = await runAction(want ? "light-on" : "light-off", els.light);
  // Revert the optimistic flip when the command didn't land.  The mower may
  // never report light_on (it is null until probed), so waiting for telemetry
  // to correct the label could mean waiting forever.
  if (!ok && currentMower === target && lightOn === want) {
    lightOn = !want;
    setLightLabel();
  }
  // The server fires a get_car_light read-back right after the set; poll a
  // couple of times soon so the toggle reconciles to the mower's real state
  // (confirming success, or reverting on failure) within ~1-2 s instead of
  // waiting for the regular 3 s cadence.  Guard against a mower switch mid-wait.
  repoll(target, [1200, 2500]);
};

// ── Mowing: job control, blades, settings ───────────────────────────────────
els.startJob.onclick  = () => { runAction("start-job", els.startJob); };
els.cancelJob.onclick = () => { runAction("cancel-job", els.cancelJob); };

// Blades On takes a second confirming tap within 3.5 s — blades while
// manual-driving is the one genuinely dangerous button on this page.  The
// mower's own lift/tilt/bumper protections still apply regardless of what we
// send; the mower may also refuse the command when it deems it unsafe.
//
// The armed state is torn down by anything that breaks the "two taps, same
// intent, same machine, right now" contract: the 3.5 s timer, STOP, a mower
// switch, and the page being hidden or unloaded.
let bladesArmTimer = null;
function disarmBlades() {
  if (bladesArmTimer) { clearTimeout(bladesArmTimer); bladesArmTimer = null; }
  setClass(els.bladesOn, "armed", false);
  setText(els.bladesOnLabel, "Blades On");
}
els.bladesOn.onclick = () => {
  if (isPending("blades-on")) return;     // POST already in flight — no re-arm
  if (!bladesArmTimer) {
    setClass(els.bladesOn, "armed", true);
    setText(els.bladesOnLabel, "Tap to confirm");
    bladesArmTimer = setTimeout(disarmBlades, 3500);
    return;
  }
  disarmBlades();
  bladesActionWithConfirm("blades-on");
};
els.bladesOff.onclick = () => {
  if (isPending("blades-off")) return;    // in-flight guard (double-fire bug)
  disarmBlades();
  bladesActionWithConfirm("blades-off");
};

// Send a blades action, then re-poll quickly so the Blades chip reconciles to
// the mower's real sensor state (spin-up takes a moment; the mower may also
// refuse) instead of waiting for the regular 3 s cadence.
async function bladesActionWithConfirm(name) {
  const target = currentMower;
  await runAction(name, name === "blades-on" ? els.bladesOn : els.bladesOff);
  repoll(target, [1200, 2500, 4500]);
}

// Settings sliders.  'input' previews the value; 'change' (release) applies it
// via POST /api/set, which clamps to the mower's model limits and echoes back
// what it actually applied.  Slider bounds come from /api/status's `limits`
// once the mower has reported its model (per-mower, so a switch re-scales).
let limitsFor = null;        // mower name the current slider bounds belong to
let heightDragging = false;  // don't let the 3 s poll fight the user's thumb
// Mower whose height slider the user has taken over.  Once set, telemetry
// stops writing the slider position for that mower (it only updates the
// separate "mower: N mm" readout) — otherwise a stale reading snaps the thumb
// back a second after every adjustment, which is what made the control look
// broken.  Reset on mower switch so a newly selected mower seeds normally.
let heightTouchedFor = null;

async function applySetting(setting, value, labelEl, unit) {
  const target = currentMower;
  if (!target) return;
  try {
    const applied = await deadline(T_SET, async (signal) => {
      const r = await fetch(`/api/set/${encodeURIComponent(target)}/${setting}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value }),
        signal,
      });
      if (!r.ok) {
        log(`set ${setting} failed: ${r.status} ${await r.text()}`);
        return null;
      }
      return (await r.json()).applied;
    });
    if (applied === null || applied === undefined) {
      toast(`Set ${setting.replace("_", " ")} failed`, false);
      return;
    }
    setText(labelEl, `${applied} ${unit}`);
    log(`set ${setting} = ${applied} ${unit}`);
    toast(`${setting === "blade_height" ? "Height" : "Speed"} ${applied} ${unit}`);
  } catch (e) {
    log(`set ${setting} threw: ${e}`);
    toast(`Set ${setting.replace("_", " ")} failed`, false);
  }
}

els.heightSlider.oninput = () => {
  heightDragging = true;
  heightTouchedFor = currentMower;   // the slider is the user's from now on
  setText(els.heightVal, `${els.heightSlider.value} mm`);
};
els.heightSlider.onchange = () => {
  heightDragging = false;
  const target = currentMower;
  applySetting("blade_height", Number(els.heightSlider.value), els.heightVal, "mm");
  // Height changes are mechanical (the motor takes a few seconds).  Re-poll so
  // the "mower: N mm" readout tracks adjusting… → the settled value.
  repoll(target, [1500, 3500, 6000]);
};
els.speedSlider.oninput = () => {
  setText(els.speedVal, `${Number(els.speedSlider.value).toFixed(2)} m/s`);
};
els.speedSlider.onchange = () => {
  applySetting("speed", Number(els.speedSlider.value), els.speedVal, "m/s");
};

// Blade-speed preset (Eco/Normal/Fast → cutter_mode 1/0/2).  Optimistically
// highlight the tapped preset; pollStatus reconciles with the mower's
// read-back (the server fires get_cutter_mode right after the set).
els.cutterSeg.addEventListener("click", async (e) => {
  const btn = e.target.closest(".seg-btn");
  if (!btn || !currentMower || pending.has("cutter_mode")) return;   // de-dupe
  for (const b of els.cutterSeg.querySelectorAll(".seg-btn")) {
    setClass(b, "active", b === btn);
    setAttr(b, "aria-pressed", String(b === btn));
  }
  const target = currentMower;
  const label = btn.textContent.trim();
  pending.add("cutter_mode");
  setBusy(btn, true);
  try {
    // Deadline added: this fetch had none, so a hung request left the preset
    // aria-busy (pointer-events: none) and "cutter_mode" pending forever —
    // the segment could never be pressed again without a reload.
    const ok = await deadline(T_SET, async (signal) => {
      const r = await fetch(`/api/set/${encodeURIComponent(target)}/cutter_mode`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: Number(btn.dataset.mode) }),
        signal,
      });
      if (!r.ok) {
        log(`set cutter_mode failed: ${r.status} ${await r.text()}`);
        return false;
      }
      return true;
    });
    if (!ok) { toast("Blade speed failed", false); return; }
    log(`set blade speed: ${label}`);
  } catch (err) {
    log(`set cutter_mode threw: ${err}`);
    toast("Blade speed failed", false);
  } finally {
    pending.delete("cutter_mode");
    setBusy(btn, false);
    repoll(target, [1500]);
  }
});

// ── Camera ──────────────────────────────────────────────────────────────────
// Throttled so a storm of decode-failure events can't flood the BLE link.
let lastKeyframeReq = 0;
async function requestKeyframe() {
  const target = currentMower;
  if (!target) return;
  const now = Date.now();
  if (now - lastKeyframeReq < 3000) return;
  lastKeyframeReq = now;
  try {
    await deadline(T_ACTION, (signal) =>
      fetch(`/api/camera/${encodeURIComponent(target)}/refresh`, { method: "POST", signal }));
  } catch (e) {
    /* keyframe poke is best-effort */
  }
}

// Re-derive the camera buttons from the mower that is current *now*.  Used by
// the bail-out paths in startCameraInner: a start that loses a race with a
// mower switch must not leave "Start camera" disabled for the mower the user
// actually ended up on.
function syncCameraButtons() {
  const meta = mowersList.find(m => m.name === currentMower);
  els.camStart.disabled = !(meta && meta.camera) || !!agora;
  els.camStop.disabled = !agora;
}

async function startCamera() {
  if (!currentMower || pending.has("cam-start")) return;   // in-flight guard
  pending.add("cam-start");
  setBusy(els.camStart, true);
  try {
    await startCameraInner();
  } catch (e) {
    log(`camera start threw: ${e}`);
  } finally {
    pending.delete("cam-start");
    setBusy(els.camStart, false);
  }
}

async function startCameraInner() {
  const target = currentMower;
  els.camStart.disabled = true;
  setCamUI("joining");
  log(`starting camera for ${nick(target)}…`);
  let client = null;
  try {
    const tok = await deadline(T_CAMERA, async (signal) => {
      const r = await fetch(`/api/camera/${encodeURIComponent(target)}/start`, { method: "POST", signal });
      if (!r.ok) {
        const body = await r.text();
        log(`camera start failed: ${r.status} ${body}`);
        return null;
      }
      return await r.json();
    });
    if (!tok) { els.camStart.disabled = false; setCamUI("off"); return; }
    if (target !== currentMower) { setCamUI("off"); syncCameraButtons(); return; }  // switched mid-flight
    log(`got Agora token (channel=${tok.channelName})`);
    if (typeof AgoraRTC === "undefined") {
      log("Agora SDK not loaded — check this device's internet access");
      toast("Camera library unavailable", false);
      els.camStart.disabled = false;
      setCamUI("off");
      return;
    }
    // The mowers now publish H.265/HEVC.  The SFU forwards HEVC regardless of
    // the SDP, so the client must negotiate H.265 or every frame is "(none
    // matched)".  Requires the browser to support HEVC decode (Safari/iOS does
    // natively; Chrome needs the OS HEVC decoder, e.g. Windows "HEVC Video
    // Extensions", and may need chrome://flags HEVC WebRTC enabled).
    client = AgoraRTC.createClient({ mode: "rtc", codec: "h265" });
    agora = client;
    client.on("user-published", async (user, mediaType) => {
      if (client !== agora) return;             // stale client after a switch
      log(`user-published uid=${user.uid} ${mediaType}`);
      try {
        await client.subscribe(user, mediaType);
      } catch (e) {
        log(`subscribe failed: ${e}`);
        return;
      }
      if (mediaType === "video") {
        user.videoTrack.play("video");
        // We've likely missed the publisher's initial keyframe; demand a fresh
        // IDR now that we're subscribed, or the decoder stays stuck on "waiting".
        requestKeyframe();
      }
      if (mediaType === "audio") user.audioTrack.play();
    });
    client.on("user-unpublished", (u) => log(`user-unpublished uid=${u.uid}`));
    // 1005 = RECV_VIDEO_DECODE_FAILED — a missed/lost keyframe; ask for another.
    client.on("exception", (evt) => {
      if (client === agora && evt && evt.code === 1005) requestKeyframe();
    });
    // join() has no abort signal, so race it: a join that never settles used to
    // strand cam-start in `pending` + aria-busy with no way back but a reload.
    await Promise.race([
      client.join(tok.appid, tok.channelName, tok.token, tok.uid),
      new Promise((_, rej) => setTimeout(() => rej(new Error("Agora join timed out")), T_JOIN)),
    ]);
    if (target !== currentMower || client !== agora) {   // switched while joining
      if (agora === client) agora = null;
      try { await client.leave(); } catch (_) {}
      setCamUI("off");
      syncCameraButtons();
      return;
    }
    log("joined Agora channel");
    els.camStop.disabled = false;
    setCamUI("live");
    startCompass(target);   // begin polling heading for the overlay compass
  } catch (e) {
    log(`camera start threw: ${e}`);
    if (client) {
      if (agora === client) agora = null;
      try { await client.leave(); } catch (_) {}
    }
    els.camStart.disabled = false;
    setCamUI("off");
  }
}

// `target` names the mower whose server-side session to tear down.  It used to
// read currentMower, which selectMower had already re-pointed at the *new*
// mower — so switching mowers stopped the wrong camera and left the old one
// streaming on the server.
async function stopCamera({ silent, target } = {}) {
  stopCompass();
  setCamUI("off");
  if (agora) {
    const client = agora;
    agora = null;
    try { await client.leave(); } catch (_) {}
    els.video.innerHTML = "";
  }
  const name = target === undefined ? currentMower : target;
  if (!name) {
    els.camStart.disabled = false;
    els.camStop.disabled = true;
    return;
  }
  try {
    // Deadline added: this had none, and selectMower awaited it — a hung stop
    // blocked the whole mower switch, including bringing the joystick up.
    await deadline(T_CAMERA, (signal) =>
      fetch(`/api/camera/${encodeURIComponent(name)}/stop`, { method: "POST", signal }));
    if (!silent) log("camera stopped");
  } catch (e) {
    if (!silent) log(`camera stop threw: ${e}`);
  }
  els.camStart.disabled = false;
  els.camStop.disabled = true;
}

els.camStart.onclick = startCamera;
els.camStop.onclick  = async () => {
  if (pending.has("cam-stop")) return;    // in-flight guard
  pending.add("cam-stop");
  setBusy(els.camStop, true);
  try {
    await stopCamera({});
  } catch (e) {
    log(`camera stop handler threw: ${e}`);
  } finally {
    pending.delete("cam-stop");
    setBusy(els.camStop, false);
  }
};

// ── UI chrome (cosmetic only — no mower I/O) ────────────────────────────────
// Log drawer: collapsed by default on phones, open by default on desktop where
// vertical space is cheap.  The <pre> scrolls internally either way.
els.logToggle.onclick = () => {
  const open = els.logDrawer.classList.toggle("open");
  setAttr(els.logToggle, "aria-expanded", String(open));
  if (open) flushLog();                                 // paint whatever buffered while closed
};
if (window.matchMedia("(min-width: 900px)").matches) {
  els.logDrawer.classList.add("open");
  setAttr(els.logToggle, "aria-expanded", "true");
}

// Fault banner is clamped to two lines; tap toggles the full message.
els.mowerError.onclick = () => els.mowerError.classList.toggle("expanded");

// ── Boot ────────────────────────────────────────────────────────────────────
if (!nippleReady) {
  log("nipplejs missing at boot (CDN unreachable?) — retrying in the background");
  loadNipple();
}
setLinkUI();
loadMowers();
