# Link reliability — joystick latency and BLE congestion

Notes from diagnosing "buttons work 1 tap in 20, joystick has insane delay and
works every 10th time" on a real mower (Luba-VA / HM442) driven through an HC33
over Wi-Fi. The causes turned out to be split across three layers, which is why
it presented as general flakiness rather than one clean failure.

Measured baseline while diagnosing: `/api/status` 22 ms, BLE command
round-trip 25 ms, clean server logs. The link itself was never the problem.

## 1. The joystick receive loop blocked on the BLE send (server) — fixed

`joystick_ws` awaited `h.send_raw()` inline inside its `ws.receive_json()`
loop. That send serialises on `HC33ProxyTransport._send_lock` and ends in
`writer.drain()`, which blocks for as long as the HC33 applies TCP
backpressure (the firmware stops reading while the mower's BLE TX buffers are
full). While blocked, the handler is **not receiving** — so the browser's
150 ms frames pile up, and when the send finally completes they are drained
**in order**, one blocking send at a time.

The server therefore replays stick positions from seconds ago, and if the
link's sustainable rate ever dips below the 6.5 frames/s the browser produces,
the backlog never clears and lag grows without bound. Simulated with a 500 ms
stall: 3.0 s of stick input took **10.17 s to replay, 7.2 s of lag and
growing**.

Fix: the receive loop now only decodes into a **single-slot mailbox** and never
touches BLE; a dedicated sender task drains that slot. A frame superseded while
a send is in flight is *dropped, not queued* — for a control input the newest
frame is the only one that matters. Same input after the fix: 3.02 s wall,
worst receive gap 151 ms, and the frames actually sent were strictly
newest-available.

Deriving the speeds and serialising the protobuf also moved into the sender, so
that work is no longer done for frames that get dropped.

## 2. Telemetry competing with control (server) — fixed

Every probe shares the one BLE link with joystick frames, and `_send_lock` is
strict FIFO, so a probe that arrived first delays a joystick frame by its full
duration.

Telemetry frames per minute contending with control:

| | before | after |
|---|---|---|
| at rest, UI open | 34.2 | 34.2 |
| at rest, no UI | 34.2 | **14.2** |
| while driving | 34+ | **12** |

Changes: probes are held off entirely while the stick is held; a telemetry send
defers to any already-queued control send (bounded at 600 ms so a wedged
control send can never starve the keep-alives the firmware's 30 s idle timeout
depends on); the blade-height readback became single-flight per mower instead
of one task per POST.

Also fixed: the first fault ever reported triggered a full cloud login plus CSV
download **inline in `/api/status`**; it is now a background one-shot. And a
transient send error used to propagate out and close the joystick socket
entirely — one blip cost the whole stick until the mower was re-selected.

## 3. Rendering cost (client) — fixed

The visual redesign put `backdrop-filter: saturate() blur()` on three
always-visible fixed layers (top bar, tab bar, STOP bar) plus the LIVE badge
and the compass. Each forces the compositor to read back and blur everything
behind it on every frame it is on screen — and the LIVE badge's backdrop was
the video, so it re-blurred on every decoded frame. Together they consumed most
of a phone's frame budget, which is what dropped taps.

Every permanent animation was also repainting (`box-shadow` pulses,
`background-position` shimmer), and nipplejs writes the stick's `transform`
inline per pointermove — which the browser cannot auto-promote, so every move
re-rasterised the zone underneath including its repeating radial gradient.

See the performance rules at the top of `static/theme.css`; they exist to stop
this recurring.

## Still open — not fixed here

**`web-server/hc33_proxy.py`**

1. `_send_lock` has no priority notion. The app-level gate can stop telemetry
   *stacking* but cannot preempt a send already in flight. A real
   `send(..., priority=)` or two-queue scheduler would close the residual gap.
2. `send()` has no timeout on `connect()`. `asyncio.open_connection` to an
   unreachable HC33 blocks for the OS TCP connect timeout while holding
   `_connect_lock`; every subsequent send and the auto-reconnect stack behind
   it.
3. `_last_send_monotonic` is set **before** acquiring `_send_lock`, so under
   contention it records when a send was queued, not when it went out —
   misreporting keep-alive debounce timing.
4. `_reader_loop` awaits `on_message()` inline, so a slow state reducer stalls
   socket reads → TCP backpressure toward the HC33 → the firmware's
   `client_.write()` blocks inside `loop()`.

**Firmware**

5. `tcp_proxy.cpp::send_frame_` ignores both `client_.write()` return values. A
   short write silently truncates a length-prefixed frame; the server then
   parses a bogus length and blocks on `readexactly` until the 30 s idle
   timeout drops the client. It should be atomic — write fully or drop the
   whole frame.
6. `last_rx_ms_` only advances when a frame is successfully written **to BLE**.
   A client that is sending fine but backpressured for 30 s gets dropped and
   the BLE link torn down — plausibly a direct contributor to the observed
   intermittency.
7. `try_write_pending_` blocks `loop()` for up to 18 ms per stalled frame, and
   `drain_notify_queue_` cannot run meanwhile, so inbound notifications back up
   and consume more mbufs — the very pool whose exhaustion caused the stall.
8. `tcp_rate_limit.cpp` caps inbound at 100 pps / 50 burst per direction.
   Control traffic is far below that, but confirm the Agora video path does not
   share the same netif; if it does it will blow the bucket and drop the
   control flow's ACKs.

**Elsewhere in `app.py`:** `/api/camera/{name}/start` sends
`get_report_cfg(count=0)` with `timeout=10000` and nothing renews it, so the
compass report stream lapses after ~10 s and the heading goes stale. It is
self-limiting, which is good for congestion — either renew it every ~8 s or
accept the staleness.

## Instrumentation

`_LinkStats` per mower now counts control vs telemetry frames, control send
avg/max ms, `slow_sends` (>150 ms), `stale_dropped` (superseded joystick
frames — a healthy link drops none), `telemetry_yields` and `send_errors`.
Exposed as `link_tx` on `/api/diag/{name}`, plus one log line per minute per
mower, emitted only when there was control traffic or something was slow.

Access logging for `/api/status`, `/api/heading` and `/static/` is filtered —
those alone overflowed the add-on's ~100-line log buffer in under a minute,
which is why the first attempt to debug this had no evidence to work from.
