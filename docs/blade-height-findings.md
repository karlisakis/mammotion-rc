# Cutting height on Luba-VA (HM442) over the BLE proxy — findings

## RESOLVED

**`todev_knife_height_set` is accepted and silently ignored by this firmware.
Cutting height must be sent as `DrvMowCtrlByHand.cut_knife_height` instead.**

Verified on hardware (`Luba-VAML9KK3`, fw `2.3.27.23`, undocked on grass,
`sys_status=11`, reports live at <1 s):

| Wire path | Sent | Result |
|---|---|---|
| `MctlDriver.todev_knife_height_set` | 40 | `knife_height` stayed **50** for 24 s — no motion, no event, no error |
| `DrvMowCtrlByHand(main_ctrl=0, cut_knife_ctrl=0, cut_knife_height=40)` | 40 | **50 → 49 → 41 → 40** — motor travelled, settled in ~12 s |
| Same, via the UI's default path | 60 | **39 → 48 → 60**, `is_start` true during travel |

Blades stayed off throughout every run (`blade_bits=0`, `cutter_rpm=0`) — the
`main_ctrl=0, cut_knife_ctrl=0` form is upstream's *stop-blades* command, which
happens to carry the height field.

Height is therefore **not** job-only, and there is no dock interlock: the live
set works fine, just through the other message. The documented driver-layer
command appears to be Luba-1 legacy on this platform — consistent with
upstream's HA integration, which routes everything Luba-2-and-newer through
`operate_on_device`.

**Caveat:** `DrvMowCtrlByHand` is the *manual-mowing* control message. Sending
`main_ctrl=0` while a job is running has not been tested and could plausibly
interrupt manual-mow state; prefer changing height when the mower is idle.

Also settled by the same run: real limits for this model are still unknown
(HM442 has no `device_config` entry — the served limits are the Yuka fallback,
`blade_height 0/0`), but the hardware accepted 40, 50 and 60, and reported
intermediate values (49, 48, 41, 39) during travel, so the **motor is
continuously positioned, not detented** — the 5 mm UI step is safe and
hypothesis "35 is an illegal detent" is dead.

---


Investigation notes from debugging "the cutting-height slider does nothing"
against a real mower (`Luba-VAML9KK3`) driven through the HC33 proxy with
PyMammotion 0.8.9. Blade **speed** (`set_cutter_mode`) works on the same link,
which is what makes height interesting: same envelope, same receiver, same
transport — but height must drive a physical lift motor.

Line references are to PyMammotion 0.8.9.

## What is established

**Units.** `DrvKnifeHeight.knife_height` is absolute millimetres, signed
int32 — not an index, step, or percentage. Corroborated by the per-model mm
tables in `utility/device_config.py`, by `client.py` using `-10` as a sentinel
(only meaningful in a signed absolute field), and by the readback field
`RptWork.knife_height` sharing the units.

**This model has no limits entry.** `DeviceType.LUBA_VA = (15, "Luba-VA",
"HM442")` — the name classifies fine (and ranks above the Luba-2 gate, so
NAV-addressed commands route correctly), but **HM442 appears nowhere in
`device_config.py`**. Blade-height ranges across the models that *are* listed
span min 20–60 and max 55–100, so nothing can be inferred. `_device_limits()`
therefore returns None and the UI falls back to hardcoded bounds. The mower
reporting 50 mm is the one hard constraint: its real range includes 50.

**Step granularity is unknown.** No step/increment exists anywhere in the
library — `DeviceLimits` carries only min/max. The UI's 5 mm step is a guess.
If the hardware uses 10 mm detents from 30 (30/40/50/60/70), then 35 is
unreachable and may be silently dropped or rounded — a live hypothesis.

**There is no rejection message.** The driver layer is fire-and-forget: no
proto message in the tree carries a per-command NACK for driver writes. A
silently-refused height set is indistinguishable from a lost packet at the
protocol level. The *only* inbound evidence of acceptance is
`MctlDriver.toapp_knife_status_change` → `DrvKnifeChangeReport{is_start,
start_height, end_height, cur_height}` — a motor-in-motion progress stream,
routed by `state_reducer.py:559-564` into `events.blade_height_event`.

**PyMammotion drops two relevant read-backs.** `_update_driver_data`
(`state_reducer.py:533-565`) has no case for `bidire_knife_height_report`
(field 4, the device's height read-back) or `toapp_knife_status` (field 5).
If the mower answers a set with either, the library discards it and the caller
sees nothing. `SysMowInfo.knife_height` is likewise parsed then ignored
(`device.py:238-241`).

## Two defects this investigation found in *our* code

1. **Fossilised readback.** `_current_blade_height()` preferred
   `events.blade_height_event.cur_height`, but nothing in the library ever
   clears that event. A single historical knife event — even one caused by the
   phone app days earlier — pins the readout permanently and masks the live
   value. Fixed: the event is now used only while `is_start` is true (motor
   actually moving); otherwise the live report wins.

2. **Stale by design.** `report_data.work.knife_height` *does* refresh from the
   normal BLE report stream (`RIT_WORK` is in the `get_report_cfg`
   subscription) — an earlier comment in `app.py` claiming otherwise was wrong.
   But the poll cadence is mode-dependent (`ble_loop.py:61-66`): continuous
   while active, **60 s** docked-charging, **300 s** docked-full or idle. And
   `/api/status` only reads the snapshot; it never requests a fresh report. So
   a height set while idle could legitimately show the old value for five
   minutes. Fixed: a height set now schedules `request_report_snapshot()` at
   +1 s / +5 s / +12 s to cover motor travel.

Both defects made every manual test ambiguous, which is why they were fixed
before any protocol hypothesis was tested.

## Hypotheses as they stood before the hardware test (all now settled)

1. ~~**Firmware precondition — the lift motor won't run while docked/charging.**~~
   **DEAD** — the successful runs above were on an undocked, idle mower, and
   the *failing* driver-path run was under identical conditions.
   This is the one difference in kind between cutter mode (a stored parameter;
   nothing moves) and height (an actuator). An interlock while seated on
   charging contacts would produce exactly "accepted, no error, nothing
   happens". No source evidence either way — the library gates nothing. Test
   undocked, on grass.
2. ~~**Illegal detent.**~~ **DEAD** — the motor reports intermediate values
   (49, 48, 41, 39) while travelling, so positioning is continuous.
3. **Luba-2 uses a different path.** ✅ **CONFIRMED — this was the cause.** Upstream's HA integration branches: Luba 1
   uses the sys-layer `set_blade_control`, while **Luba 2 and newer use
   `operate_on_device` → `DrvMowCtrlByHand`**, which carries `cut_knife_height`.
   Our blade on/off currently uses the Luba-1 path. Counter-evidence: the
   driver layer carries a complete live-adjust triplet (write / read-back /
   motion-progress), which only makes sense if standalone height changes are
   supported. Worth probing with `main_ctrl=0, cut_knife_ctrl=0` (the
   no-blade-spin form) — but verify on hardware that it does not spin blades.
4. ~~**Job parameter only.**~~ **DEAD** — the standalone live set works. Height is a first-class field in `NavReqCoverPath`,
   `NavPlanJobSet`, `NavStartJob` and `SysJobPlan`. `modify_route_information`
   (`sub_cmd=3`) is the least destructive way to write it as a job param.

Excluded: value clamped to 0 by our own code (the log line shows what was
applied); command lost on the wire (cutter mode proves the path); NAV
misrouting (LUBA_VA classifies correctly).

## How to test

Use `/static/diag.html` → **Message trace**. Run `noop` first as a control to
see what the mower pushes unprompted, then `set_blade_height` with a candidate
value. The verdict line is whether any `toapp_knife_status_change` arrives:

- **knife event + deck moves** → works; any earlier failure was staleness.
- **knife event + deck does not move** → firmware accepted then aborted;
  suspect an interlock (hypothesis 1).
- **no knife event at all** → the command is being ignored outright; move on to
  hypotheses 2–4.

Always note `dev.sys_status` and `dev.charge_state` alongside each run — a
result recorded while docked proves nothing about the undocked case.
