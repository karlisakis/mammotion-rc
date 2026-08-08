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
  sysHeartbeat: $("sys-heartbeat"),
  sysBattery:   $("sys-battery"),
  sysState:     $("sys-state"),
  log:       $("log"),
  logDrawer: $("log-drawer"),
  logToggle: $("log-toggle"),

  toastRegion: $("toast-region"),
};

const log = (msg) => {
  const t = new Date().toLocaleTimeString();
  els.log.textContent += `[${t}] ${msg}\n`;
  els.log.scrollTop = els.log.scrollHeight;
  console.log(msg);
};

// Map a device_name to its friendly app nickname for display in the log.
// The device_name stays the key everywhere else (selector value, API paths,
// joystick ws) — this is purely cosmetic.  Falls back to the raw name if no
// nickname is configured or mowersList hasn't loaded yet.
const nick = (name) => {
  const m = mowersList.find(m => m.name === name);
  return (m && m.nickname) || name;
};

const setStatus = (text, cls) => {
  els.statusText.textContent = text;
  els.status.title = text;   // the chip ellipsizes; full text on hover/long-press
  els.status.classList.remove("connected", "connecting", "disconnected");
  if (cls) els.status.classList.add(cls);
};

// Render the topbar battery chip.  pct === null → hidden (mower hasn't reported
// yet, or the link is down and we cleared it).  The gauge is inline SVG, not an
// emoji: the fill rect's width tracks the charge and a bolt replaces it while
// charging.  <20% turns it danger-red; charging turns it info-blue.
const LOW_BATTERY_PCT = 20;
const BATTERY_FILL_W = 12;          // px width of #battery-fill at 100% (viewBox units)
const setBattery = (pct, charging) => {
  els.battery.classList.remove("low", "charging");
  if (pct === null || pct === undefined) {
    els.battery.hidden = true;
    els.batteryPct.textContent = "";
    return;
  }
  els.battery.hidden = false;
  els.batteryPct.textContent = `${pct}%`;
  const clamped = Math.max(0, Math.min(100, pct));
  els.batteryFill.setAttribute("width", Math.max(1, (clamped / 100) * BATTERY_FILL_W).toFixed(2));
  if (charging) els.battery.classList.add("charging");
  else if (pct <= LOW_BATTERY_PCT) els.battery.classList.add("low");
};

// Render the mower state chip ("Mowing", "Charging", …) and any fault message
// in the banner under the topbar.  The banner sits OUTSIDE the tab panels so a
// fault is visible whichever tab you are on.  null → hidden.  Cleared when the
// link is down so we never show a stale state.  The full fault text is on the
// title attribute, and tapping the banner expands the clamped message.
const setMowerState = (label, error) => {
  els.mowerStatus.textContent = label || "";
  els.mowerStatus.hidden = !label;
  els.sysState.textContent = label || "—";
  els.mowerErrorText.textContent = error || "";
  els.mowerError.hidden = !error;
  els.mowerError.title = error || "Mower fault";
  if (!error) els.mowerError.classList.remove("expanded");
};

// Visual state of the camera card: "off" → placeholder, "joining" → spinner,
// "live" → video + LIVE chip + accent frame.  Purely cosmetic — it never gates
// the Agora logic.
const setCamUI = (state) => {
  els.videoFrame.classList.toggle("joining", state === "joining");
  els.videoFrame.classList.toggle("live", state === "live");
  els.camLive.hidden = state !== "live";
};

// Cosmetic "driving" affordance on the joystick zone (accent glow + DRIVING
// chip).  Toggled exactly where the command heartbeat starts/stops; it never
// gates the joystick commands themselves.
const setDriving = (on) => {
  els.joyZone.classList.toggle("active", on);
};

let currentMower = null;
let ws = null;
let joystick = null;
let agora = null;          // AgoraRTC client when camera is up
let statusTimer = null;    // setInterval handle for status polling
let headingTimer = null;   // setInterval handle for the compass heading poll
let joyTimer = null;       // setInterval re-sending the held joystick state (keeps the mower moving)
let joyState = { x: 0, y: 0, force: 0 };  // latest stick command, re-sent by joyTimer while held

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
    b.setAttribute("aria-selected", String(on));
    b.tabIndex = on ? 0 : -1;
  }
  window.scrollTo(0, 0);
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
    b.classList.toggle("active", on);
    b.setAttribute("aria-pressed", String(on));
  }
  for (const ico of els.themeToggle.querySelectorAll("[data-theme-ico]")) {
    ico.toggleAttribute("hidden", ico.dataset.themeIco !== mode);
  }
  const label = `Theme: ${mode} (showing ${window.Theme.effective()})`;
  els.themeToggle.title = label;
  els.themeToggle.setAttribute("aria-label", label);
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
function setCompass(orientation) {
  if (orientation === null || orientation === undefined) {
    els.compassReadout.textContent = "—";
    return;
  }
  const deg = (((orientation + COMPASS_OFFSET_DEG) % 360) + 360) % 360;
  els.compassRing.style.transform = `rotate(${-deg}deg)`;
  els.compassReadout.textContent = `${Math.round(deg)}°`;
}

async function pollHeading(name) {
  if (name !== currentMower || !agora) return;   // only while this mower's camera is up
  try {
    const r = await fetch(`/api/heading/${encodeURIComponent(name)}`);
    if (!r.ok || name !== currentMower) return;
    const h = await r.json();
    setCompass(h.orientation);
  } catch (_) { /* transient — next tick retries */ }
}

function startCompass(name) {
  if (headingTimer) clearInterval(headingTimer);
  els.compass.hidden = false;
  pollHeading(name);                                     // paint immediately
  headingTimer = setInterval(() => pollHeading(name), 1000);
}

function stopCompass() {
  if (headingTimer) { clearInterval(headingTimer); headingTimer = null; }
  els.compass.hidden = true;
  els.compassReadout.textContent = "—";
}

// ── Mower list ──────────────────────────────────────────────────────────────
// Camera availability is per-mower (depends on iot_id being set in mowers.toml).
// We cache the /api/mowers payload so selectMower() can read it without re-fetching.
let mowersList = [];

async function loadMowers() {
  log("loadMowers: fetching /api/mowers…");
  try {
    const r = await fetch("/api/mowers", { cache: "no-store" });
    log(`loadMowers: HTTP ${r.status}`);
    if (!r.ok) { setStatus(`mower list failed: HTTP ${r.status}`, "disconnected"); return; }
    mowersList = await r.json();
    log(`loadMowers: got ${mowersList.length} mower(s)`);
  } catch (e) {
    log(`loadMowers: fetch error: ${e}`);
    setStatus("mower list failed to load", "disconnected");
    return;
  }
  els.mower.innerHTML = "";
  for (const m of mowersList) {
    const opt = document.createElement("option");
    opt.value = m.name;                       // device_name stays the key
    const label = m.nickname || m.name;       // show the friendly app name when set
    opt.textContent = m.camera ? label : `${label} (no camera)`;
    els.mower.appendChild(opt);
  }
  if (mowersList.length > 0) {
    currentMower = mowersList[0].name;
    await selectMower(currentMower);
  } else {
    setStatus("no mowers configured");
    els.sysMower.textContent = "none configured";
  }
}

async function selectMower(name) {
  currentMower = name;
  setStatus(`connecting ${name}…`, "connecting");
  els.sysMower.textContent = nick(name);
  lightOn = false;          // unknown on a fresh mower — assume off
  setLightLabel();
  // Per-mower slider state: let telemetry seed this mower's height slider once.
  heightTouchedFor = null;
  heightDragging = false;
  await stopCamera({ silent: true });   // if a previous mower's camera was up

  const meta = mowersList.find(m => m.name === name);
  const cameraAvailable = meta && meta.camera;
  els.camStart.disabled = !cameraAvailable;
  els.camStop.disabled = true;
  if (!cameraAvailable) {
    log(`(${nick(name)}: no iot_id configured → camera disabled)`);
  }

  // Idempotent — also recovers any mower the lifespan failed to reach at boot.
  await reconnectMower(name, { silent: true });

  startJoystick(name);
  startStatusPolling(name);
}

els.mower.onchange = (e) => selectMower(e.target.value);

// ── Reconnect + status polling ──────────────────────────────────────────────
async function reconnectMower(name, { silent } = {}) {
  if (!silent) log(`reconnecting ${nick(name)}…`);
  // Abort guard: a dead server must not leave the Reconnect button spinning
  // forever (BLE reconnects are slow, so this is generous).
  const ctl = new AbortController();
  const kill = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch(`/api/reconnect/${encodeURIComponent(name)}`, { method: "POST", signal: ctl.signal });
    if (!r.ok) {
      const body = await r.text();
      log(`reconnect failed: ${r.status} ${body}`);
      setStatus(`${name}: reconnect failed`, "disconnected");
      return false;
    }
    if (!silent) log(`reconnect ok (${nick(name)})`);
    await pollStatus(name);   // refresh status immediately
    return true;
  } catch (e) {
    log(`reconnect threw: ${e}`);
    setStatus(`${name}: reconnect error`, "disconnected");
    return false;
  } finally {
    clearTimeout(kill);
  }
}

// Thresholds for the "mower silent" indicator (seconds since last LubaMsg).
const MOWER_SLOW_S   = 5;
const MOWER_SILENT_S = 10;

async function pollStatus(name) {
  if (name !== currentMower) return;   // stale tick after mower switch
  try {
    const r = await fetch(`/api/status/${encodeURIComponent(name)}`);
    if (!r.ok) return;
    const s = await r.json();
    if (name !== currentMower) return;

    // Battery reflects the last decoded report; hide it whenever the link to
    // the mower isn't healthy so we never show a stale charge.
    const live = s.availability === "connected";
    const silent = s.mower_silent_s;
    setBattery(live ? s.battery : null, s.charging);
    setMowerState(live ? s.status : null, live ? s.error : null);

    // System tab's connection detail — the same truth as the topbar chips, but
    // spelled out (and readable when the chip has ellipsized).
    els.sysMower.textContent = nick(name);
    els.sysLink.textContent = s.auto_retrying
      ? "auto-reconnecting…"
      : (s.auto_gave_up ? "gave up — press Reconnect" : String(s.availability));
    els.sysLink.className = `sys-val ${live ? "ok" : "bad"}`;
    els.sysHeartbeat.textContent =
      (silent === null || silent === undefined) ? "—" : `${silent.toFixed(1)} s ago`;
    els.sysBattery.textContent =
      (live && typeof s.battery === "number")
        ? `${s.battery}%${s.charging ? " · charging" : ""}`
        : "—";

    // Surface the manual-reconnect prompt on the button itself and badge the
    // System tab, so the nudge is visible from Drive/Mow too (warn-tinted when
    // the auto-retry loop has given up).
    const needsAttention = !live && !!s.auto_gave_up;
    els.reconnect.classList.toggle("attention", needsAttention);
    els.tabSystemBadge.hidden = !needsAttention;

    // Reconcile the headlight toggle with the mower's actual state — the
    // firmware auto-offs the light, so trust the server's probe over our
    // optimistic guess.  null means "not probed yet"; keep the local guess.
    if (typeof s.light_on === "boolean" && s.light_on !== lightOn) {
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
        els.heightSlider.value = s.blade_height;
        els.heightVal.textContent = `${s.blade_height} mm`;
      }
      if (s.blade_adjusting) {
        els.heightReport.textContent = "adjusting…";
        els.heightReport.className = "setting-sub adjusting";
      } else {
        els.heightReport.textContent = `mower: ${s.blade_height} mm`;
        const match = Number(els.heightSlider.value) === s.blade_height;
        els.heightReport.className = "setting-sub" + (match ? " match" : " differs");
      }
    } else {
      els.heightReport.textContent = "";
    }

    // Blade telemetry: the chip shows the mower's own "disc rotating" sensor
    // bit — ground truth, so a commanded-but-refused blades-on shows OFF here.
    // The pulse only runs when the disc is genuinely spinning.
    if (typeof s.blades_on === "boolean") {
      els.bladeChip.textContent = `Blades: ${s.blades_on ? "ON" : "OFF"}`;
      els.bladeChip.classList.toggle("blades-live", s.blades_on);
    } else {
      els.bladeChip.textContent = "Blades: —";
      els.bladeChip.classList.remove("blades-live");
    }
    const rpm = s.cutter_rpm;
    els.rpmChip.hidden = !(typeof rpm === "number" && rpm > 0);
    if (!els.rpmChip.hidden) els.rpmChip.textContent = `${rpm} rpm`;
    // Mark the active blade-speed preset from the mower's read-back.
    if (typeof s.cutter_mode === "number") {
      for (const b of els.cutterSeg.querySelectorAll(".seg-btn")) {
        const on = Number(b.dataset.mode) === s.cutter_mode;
        b.classList.toggle("active", on);
        b.setAttribute("aria-pressed", String(on));
      }
    }
    // Job progress chip (only meaningful mid-job).
    const pct = s.mow_percent;
    els.progressChip.hidden = !(typeof pct === "number" && pct > 0 && pct <= 100);
    if (!els.progressChip.hidden) els.progressChip.textContent = `Job ${pct}%`;

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
  } catch (_) { /* network blip — next tick will retry */ }
}

function startStatusPolling(name) {
  if (statusTimer) clearInterval(statusTimer);
  statusTimer = setInterval(() => pollStatus(name), 3000);
}

els.reconnect.onclick = async () => {
  if (!currentMower || pending.has("reconnect")) return;   // in-flight guard
  pending.add("reconnect");
  setBusy(els.reconnect, true);
  let ok = false;
  try {
    ok = await reconnectMower(currentMower, {});
  } finally {
    pending.delete("reconnect");
    setBusy(els.reconnect, false);
    flashResult(els.reconnect, ok);
    toast(ok ? "Reconnected" : "Reconnect failed", ok);
  }
};

// ── Joystick ───────────────────────────────────────────────────────────────
function startJoystick(name) {
  if (ws) { ws.close(); ws = null; }
  if (joystick) { joystick.destroy(); joystick = null; }
  if (joyTimer) { clearInterval(joyTimer); joyTimer = null; }   // don't leak a re-send timer across mower switches
  setDriving(false);

  const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${wsProto}//${location.host}/ws/joystick/${encodeURIComponent(name)}`);
  ws.onopen  = () => log(`joystick ws open (${nick(name)})`);
  ws.onclose = () => log(`joystick ws closed (${nick(name)})`);
  ws.onerror = () => log(`joystick ws error`);

  // Dynamic mode: the stick is created under the finger on each touch, so the
  // initial touch is always neutral (force 0) and motion is measured relative
  // to where you touched.  Static mode pins the stick to the centre and treats
  // the touch point's offset from centre as an instant command — on a phone
  // your thumb lands in the lower half, so it jumped straight to "back/down".
  // dynamicPage recomputes the zone offset per interaction so a scrolled or
  // transformed page can't skew the coordinates either.
  joystick = nipplejs.create({
    zone: els.joyZone,
    mode: "dynamic",
    dynamicPage: true,
    color: token("--accent") || "currentColor",   // CSS re-tints it on theme flip
    size: 180,
  });

  // The mower self-limits each speed command — it moves a short distance/time
  // then stops — so one frame per stick movement isn't enough to keep it going.
  // nipplejs only emits "move" when the stick actually MOVES (verified: no
  // repeat-while-held option in 0.10.1), so a motionless held stick used to fall
  // silent and the mower would halt ("hold it up and it stops").  Fix: latch the
  // current stick state and RE-SEND it on an interval while held.  REPEAT_MS must
  // be shorter than the mower's per-command window; 150 ms (~6.5 Hz) is the rate
  // that already sustained motion during active drags.
  const REPEAT_MS = 150;   // ~6.5 Hz

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
      setDriving(true);
    }
  });

  joystick.on("end", () => {
    if (joyTimer) { clearInterval(joyTimer); joyTimer = null; }
    joyState = { x: 0, y: 0, force: 0 };
    sendJoystick(joyState);                                             // explicit stop on release
    setDriving(false);
  });
}

function sendJoystick(payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

// Safety: if the tab loses focus or is hidden while the stick is held (alt-tab,
// the pointer leaves the window so "end" never fires, or the phone screen locks
// / the browser is backgrounded), kill the re-send heartbeat and command a stop.
// Without this the mower could keep driving on the last speed while you're not
// looking.  Registered once at load; joyTimer/joyState are module-scope.
function joystickFailsafeStop() {
  if (!joyTimer) return;                 // only act if we're actively driving
  clearInterval(joyTimer);
  joyTimer = null;
  joyState = { x: 0, y: 0, force: 0 };
  sendJoystick(joyState);
  setDriving(false);
}
window.addEventListener("blur", joystickFailsafeStop);
window.addEventListener("pagehide", joystickFailsafeStop);
document.addEventListener("visibilitychange", () => {
  if (document.hidden) joystickFailsafeStop();
});

// ── Actions ─────────────────────────────────────────────────────────────────
// Raw POST for a named mower action.  Returns true iff the server said OK;
// failures are logged to the diagnostics drawer.  The 10 s abort guard means a
// hung request can never leave a button stuck in its busy state.
async function action(name) {
  if (!currentMower) return false;
  log(`action: ${name}`);
  const ctl = new AbortController();
  const kill = setTimeout(() => ctl.abort(), 10000);
  try {
    const r = await fetch(`/api/action/${encodeURIComponent(currentMower)}/${name}`, { method: "POST", signal: ctl.signal });
    if (!r.ok) {
      log(`action ${name} failed: ${r.status} ${await r.text()}`);
      return false;
    }
    return true;
  } catch (e) {
    log(`action ${name} threw: ${e}`);
    return false;
  } finally {
    clearTimeout(kill);
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
// The STOP button is deliberately exempt from the pointer-lock half of that:
// it is a .btn-stop, not a .btn, and style.css re-asserts pointer-events on it
// while busy.  An emergency control must never stop accepting presses; the
// `pending` set alone de-dupes it.

const pending = new Set();               // action names with a POST in flight
const isPending = (...names) => names.some((n) => pending.has(n));

function setBusy(btn, on) {
  if (!btn) return;
  if (on) btn.setAttribute("aria-busy", "true");
  else btn.removeAttribute("aria-busy");
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
  } finally {
    pending.delete(name);
    setBusy(btn, false);
    flashResult(btn, ok);
    if (withToast) toast(ok ? (ACTION_LABELS[name] || name) : `${name} failed`, ok);
  }
  return ok;
}

els.pause.onclick  = () => runAction("pause", els.pause);
els.resume.onclick = () => runAction("resume", els.resume);
els.dock.onclick   = () => runAction("dock", els.dock);
els.undock.onclick = () => runAction("undock", els.undock);
// STOP also cuts the blades: stop_and_not_save_task halts motion/task, but a
// manually-enabled blade state is separate — an emergency stop should end both.
// Both POSTs fire in parallel; repeat presses while one is in flight are
// no-ops via `pending` (the STOP button itself is never pointer-locked).
els.stop.onclick   = () => {
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
  els.lightLabel.textContent = lightOn ? "Light Off" : "Light On";
}
els.light.onclick = async () => {
  if (isPending("light-on", "light-off")) return;   // don't stack a double-tap
  lightOn = !lightOn;          // optimistic flip for instant feedback
  setLightLabel();
  const target = currentMower;
  await runAction(lightOn ? "light-on" : "light-off", els.light);
  // The server fires a get_car_light read-back right after the set; poll a
  // couple of times soon so the toggle reconciles to the mower's real state
  // (confirming success, or reverting on failure) within ~1-2 s instead of
  // waiting for the regular 3 s cadence.  Guard against a mower switch mid-wait.
  setTimeout(() => { if (currentMower === target) pollStatus(target); }, 1200);
  setTimeout(() => { if (currentMower === target) pollStatus(target); }, 2500);
};

// ── Mowing: job control, blades, settings ───────────────────────────────────
els.startJob.onclick  = () => runAction("start-job", els.startJob);
els.cancelJob.onclick = () => runAction("cancel-job", els.cancelJob);

// Blades On takes a second confirming tap within 3.5 s — blades while
// manual-driving is the one genuinely dangerous button on this page.  The
// mower's own lift/tilt/bumper protections still apply regardless of what we
// send; the mower may also refuse the command when it deems it unsafe.
let bladesArmTimer = null;
function disarmBlades() {
  if (bladesArmTimer) { clearTimeout(bladesArmTimer); bladesArmTimer = null; }
  els.bladesOn.classList.remove("armed");
  els.bladesOnLabel.textContent = "Blades On";
}
els.bladesOn.onclick = () => {
  if (isPending("blades-on")) return;     // POST already in flight — no re-arm
  if (!bladesArmTimer) {
    els.bladesOn.classList.add("armed");
    els.bladesOnLabel.textContent = "Tap to confirm";
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
  for (const ms of [1200, 2500, 4500]) {
    setTimeout(() => { if (currentMower === target) pollStatus(target); }, ms);
  }
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
  if (!currentMower) return;
  try {
    const r = await fetch(`/api/set/${encodeURIComponent(currentMower)}/${setting}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    });
    if (!r.ok) {
      log(`set ${setting} failed: ${r.status} ${await r.text()}`);
      toast(`Set ${setting.replace("_", " ")} failed`, false);
      return;
    }
    const { applied } = await r.json();
    labelEl.textContent = `${applied} ${unit}`;
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
  els.heightVal.textContent = `${els.heightSlider.value} mm`;
};
els.heightSlider.onchange = () => {
  heightDragging = false;
  const target = currentMower;
  applySetting("blade_height", Number(els.heightSlider.value), els.heightVal, "mm");
  // Height changes are mechanical (the motor takes a few seconds).  Re-poll so
  // the "mower: N mm" readout tracks adjusting… → the settled value.
  for (const ms of [1500, 3500, 6000]) {
    setTimeout(() => { if (currentMower === target) pollStatus(target); }, ms);
  }
};
els.speedSlider.oninput = () => {
  els.speedVal.textContent = `${Number(els.speedSlider.value).toFixed(2)} m/s`;
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
    b.classList.toggle("active", b === btn);
    b.setAttribute("aria-pressed", String(b === btn));
  }
  const target = currentMower;
  const label = btn.textContent.trim();
  pending.add("cutter_mode");
  setBusy(btn, true);
  try {
    const r = await fetch(`/api/set/${encodeURIComponent(target)}/cutter_mode`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value: Number(btn.dataset.mode) }),
    });
    if (!r.ok) {
      log(`set cutter_mode failed: ${r.status} ${await r.text()}`);
      toast("Blade speed failed", false);
      return;
    }
    log(`set blade speed: ${label}`);
  } catch (err) {
    log(`set cutter_mode threw: ${err}`);
    toast("Blade speed failed", false);
  } finally {
    pending.delete("cutter_mode");
    setBusy(btn, false);
    setTimeout(() => { if (currentMower === target) pollStatus(target); }, 1500);
  }
});

// ── Camera ──────────────────────────────────────────────────────────────────
// Throttled so a storm of decode-failure events can't flood the BLE link.
let lastKeyframeReq = 0;
async function requestKeyframe() {
  if (!currentMower) return;
  const now = Date.now();
  if (now - lastKeyframeReq < 3000) return;
  lastKeyframeReq = now;
  try {
    await fetch(`/api/camera/${encodeURIComponent(currentMower)}/refresh`, { method: "POST" });
  } catch (e) {
    /* keyframe poke is best-effort */
  }
}

async function startCamera() {
  if (!currentMower || pending.has("cam-start")) return;   // in-flight guard
  pending.add("cam-start");
  setBusy(els.camStart, true);
  try {
    await startCameraInner();
  } finally {
    pending.delete("cam-start");
    setBusy(els.camStart, false);
  }
}

async function startCameraInner() {
  els.camStart.disabled = true;
  setCamUI("joining");
  log(`starting camera for ${nick(currentMower)}…`);
  try {
    const r = await fetch(`/api/camera/${encodeURIComponent(currentMower)}/start`, { method: "POST" });
    if (!r.ok) {
      const body = await r.text();
      log(`camera start failed: ${r.status} ${body}`);
      els.camStart.disabled = false;
      setCamUI("off");
      return;
    }
    const tok = await r.json();
    log(`got Agora token (channel=${tok.channelName})`);
    // The mowers now publish H.265/HEVC.  The SFU forwards HEVC regardless of
    // the SDP, so the client must negotiate H.265 or every frame is "(none
    // matched)".  Requires the browser to support HEVC decode (Safari/iOS does
    // natively; Chrome needs the OS HEVC decoder, e.g. Windows "HEVC Video
    // Extensions", and may need chrome://flags HEVC WebRTC enabled).
    agora = AgoraRTC.createClient({ mode: "rtc", codec: "h265" });
    agora.on("user-published", async (user, mediaType) => {
      log(`user-published uid=${user.uid} ${mediaType}`);
      await agora.subscribe(user, mediaType);
      if (mediaType === "video") {
        user.videoTrack.play("video");
        // We've likely missed the publisher's initial keyframe; demand a fresh
        // IDR now that we're subscribed, or the decoder stays stuck on "waiting".
        requestKeyframe();
      }
      if (mediaType === "audio") user.audioTrack.play();
    });
    agora.on("user-unpublished", (u) => log(`user-unpublished uid=${u.uid}`));
    // 1005 = RECV_VIDEO_DECODE_FAILED — a missed/lost keyframe; ask for another.
    agora.on("exception", (evt) => {
      if (evt && evt.code === 1005) requestKeyframe();
    });
    await agora.join(tok.appid, tok.channelName, tok.token, tok.uid);
    log("joined Agora channel");
    els.camStop.disabled = false;
    setCamUI("live");
    startCompass(currentMower);   // begin polling heading for the overlay compass
  } catch (e) {
    log(`camera start threw: ${e}`);
    els.camStart.disabled = false;
    setCamUI("off");
  }
}

async function stopCamera({ silent } = {}) {
  stopCompass();
  setCamUI("off");
  if (agora) {
    try { await agora.leave(); } catch (_) {}
    agora = null;
    els.video.innerHTML = "";
  }
  if (!currentMower) {
    els.camStart.disabled = false;
    els.camStop.disabled = true;
    return;
  }
  try {
    await fetch(`/api/camera/${encodeURIComponent(currentMower)}/stop`, { method: "POST" });
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
  els.logToggle.setAttribute("aria-expanded", String(open));
  if (open) els.log.scrollTop = els.log.scrollHeight;   // jump to newest lines
};
if (window.matchMedia("(min-width: 900px)").matches) {
  els.logDrawer.classList.add("open");
  els.logToggle.setAttribute("aria-expanded", "true");
}

// Fault banner is clamped to two lines; tap toggles the full message.
els.mowerError.onclick = () => els.mowerError.classList.toggle("expanded");

// ── Boot ────────────────────────────────────────────────────────────────────
loadMowers();
