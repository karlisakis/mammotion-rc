/* Theme boot + toggle, shared by every page.
 *
 * Load this SYNCHRONOUSLY in <head>, before any stylesheet paints, or the page
 * flashes the OS theme before switching to the stored one.
 *
 * Three states, deliberately: "system" (follow the OS, the default), "light",
 * "dark". A binary toggle can't express "follow the OS", which is the state
 * most people actually want.
 */
(function () {
  var KEY = "luba-theme";           // "system" | "light" | "dark"
  var root = document.documentElement;

  function stored() {
    try { return localStorage.getItem(KEY) || "system"; } catch (_) { return "system"; }
  }

  function systemDark() {
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  }

  /* Resolve to the concrete theme actually being shown. */
  function effective(mode) {
    return mode === "system" ? (systemDark() ? "dark" : "light") : mode;
  }

  function apply(mode) {
    if (mode === "system") root.removeAttribute("data-theme");
    else root.setAttribute("data-theme", mode);
    // Keep the browser chrome (iOS status bar, Android address bar) in step.
    // Read the token back rather than duplicating hex values here.
    var bg = getComputedStyle(root).getPropertyValue("--bg").trim();
    var meta = document.querySelector('meta[name="theme-color"]');
    if (!meta) {
      meta = document.createElement("meta");
      meta.setAttribute("name", "theme-color");
      document.head.appendChild(meta);
    }
    if (bg) meta.setAttribute("content", bg);
    root.dispatchEvent(new CustomEvent("themechange", {
      detail: { mode: mode, effective: effective(mode) },
    }));
  }

  apply(stored());

  // Follow the OS live while in "system" mode.
  if (window.matchMedia) {
    var mq = window.matchMedia("(prefers-color-scheme: dark)");
    var onChange = function () { if (stored() === "system") apply("system"); };
    if (mq.addEventListener) mq.addEventListener("change", onChange);
    else if (mq.addListener) mq.addListener(onChange);
  }

  window.Theme = {
    get: stored,
    effective: function () { return effective(stored()); },
    set: function (mode) {
      if (["system", "light", "dark"].indexOf(mode) === -1) return;
      try { localStorage.setItem(KEY, mode); } catch (_) { /* private mode */ }
      apply(mode);
    },
    /* Cycle for a single-button control: system → light → dark → system. */
    cycle: function () {
      var next = { system: "light", light: "dark", dark: "system" }[stored()] || "system";
      window.Theme.set(next);
      return next;
    },
  };
})();
