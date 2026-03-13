#!/usr/bin/env python3
import argparse
import serial
import time
import math
import sys
from dataclasses import dataclass
import random
from typing import Dict, List, Optional, Tuple, Any

# ============================================================
# Shared Utility
# ============================================================

def resolve_port(port_arg: Optional[str], default="/dev/ttyUSB0") -> str:
    if port_arg is None:
        return default
    s = str(port_arg).strip()
    if s.isdigit():
        return f"/dev/ttyUSB{s}"
    return s

def parse_bw_to_code(bw: str) -> int:
    s = str(bw).lower().strip()
    if s in ("7", "8", "9"):
        return int(s)
    s = s.replace("khz", "k").replace(" ", "")
    if s.endswith("k"):
        s = s[:-1]
    if s in ("125", "125000"):
        return 7
    if s in ("250", "250000"):
        return 8
    if s in ("500", "500000"):
        return 9
    raise ValueError(f"Unsupported --bw {bw}. Use 7/8/9 or 125/250/500 (kHz).")

def parse_robots(expr: str) -> List[int]:
    parts = expr.replace(" ", ",").split(",")
    out = set()
    for p in parts:
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-")
            for x in range(int(a), int(b) + 1):
                out.add(x)
        else:
            out.add(int(p))
    return sorted(out)

def write_cmd(ser: serial.Serial, cmd: str, verbose: bool):
    if verbose:
        print(f"[TX] {cmd}")
    ser.write((cmd + "\r\n").encode())

def read_line(ser: serial.Serial):
    return ser.readline().decode(errors="ignore").strip()

def now_ms(start):
    return (time.monotonic() - start) * 1000.0

def at_collect(ser: serial.Serial, cmd: str, wait_s: float, verbose: bool):
    write_cmd(ser, cmd, verbose)
    end = time.time() + wait_s
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l and verbose:
            print("[AT]", l)

def at_expect_ok(ser: serial.Serial, cmd: str, wait_s: float, verbose: bool) -> bool:
    write_cmd(ser, cmd, verbose)
    end = time.time() + wait_s
    got_ok = False
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if not l:
            continue
        if verbose:
            print("[AT]", l)
        if l == "OK":
            got_ok = True
            break
    return got_ok

def wait_ready(ser: serial.Serial, timeout_s: float, verbose: bool) -> bool:
    end = time.time() + timeout_s
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l:
            if verbose:
                print("[RST]", l)
            if "+READY" in l:
                return True
    return False

def drain_uart(ser: serial.Serial, seconds: float, verbose: bool):
    end = time.time() + seconds
    while time.time() < end:
        l = ser.readline().decode(errors="ignore").strip()
        if l and verbose:
            print("[DRAIN]", l)

def sleep_until(deadline_mono: float, busy_tail_s: float = 0.002):
    while True:
        now = time.monotonic()
        remain = deadline_mono - now
        if remain <= 0:
            return
        if remain > busy_tail_s:
            time.sleep(remain - busy_tail_s)
        else:
            pass

# ============================================================
# Local Pose / Peer Table
# ============================================================

def get_local_pose(args) -> Dict[str, int]:
    """
    Minimal version:
    use CLI args as the local pose source.
    Later you can replace this with real localization data.
    """
    return {
        "x": int(args.x),
        "y": int(args.y),
        "heading": int(args.heading),
        "status": int(args.status),
    }

def update_peer_table(
    peer_table: Dict[int, Dict[str, Any]],
    peer_id: int,
    *,
    role: str,
    x: int,
    y: int,
    heading: int,
    status: int,
    seq: int,
    rssi: int,
    snr: int,
    src_addr: int,
):
    peer_table[peer_id] = {
        "role": role,
        "x": x,
        "y": y,
        "heading": heading,
        "status": status,
        "seq": seq,
        "last_update_mono": time.monotonic(),
        "rssi": rssi,
        "snr": snr,
        "src_addr": src_addr,
    }

def print_peer_table(
    peer_table: Dict[int, Dict[str, Any]],
    stale_timeout_s: float,
    expected_ids: Optional[List[int]] = None,
    master_id: Optional[int] = None,
):
    print("---- PEER TABLE ----")
    now = time.monotonic()
    id_set = set(peer_table.keys())
    if expected_ids:
        id_set.update(expected_ids)
    if master_id is not None:
        id_set.add(master_id)

    if not id_set:
        print("(empty)")
        print("--------------------")
        return

    for peer_id in sorted(id_set):
        p = peer_table.get(peer_id)
        if p is None:
            role = "master" if (master_id is not None and peer_id == master_id) else "robot"
            age = 9999.0
            seq_text = "--"
            x = 0
            y = 0
            heading = 0
            status_val = 0
            rssi = 0
            snr = 0
        else:
            role = p["role"]
            age = now - p["last_update_mono"]
            seq_val = p.get("seq", None)
            seq_text = f"{seq_val:02X}" if isinstance(seq_val, int) else "--"
            x = p["x"]
            y = p["y"]
            heading = p["heading"]
            status_val = p["status"]
            rssi = p["rssi"]
            snr = p["snr"]

        stale = "STALE" if age > stale_timeout_s else "OK"
        print(
            f"ID {peer_id:>3} [{role:^6}] "
            f"x={x} y={y} hdg={heading} st={status_val} "
            f"seq={seq_text} RSSI={rssi} SNR={snr} age={age:.1f}s {stale}"
        )
    print("--------------------")

# ============================================================
# Shared Packet Format
# ============================================================

def make_master_pos_field(master_id: int, seq: int, x: int, y: int, heading: int, status: int) -> str:
    return f"P:{master_id}:{seq:02X}:{x}:{y}:{heading}:{status}"

def parse_master_pos_field(field: str):
    """
    P:<id>:<seq_hex>:<x>:<y>:<heading>:<status>
    """
    if not field.startswith("P:"):
        return None
    try:
        parts = field.split(":")
        if len(parts) != 7:
            return None
        return {
            "id": int(parts[1]),
            "seq": int(parts[2], 16),
            "x": int(parts[3]),
            "y": int(parts[4]),
            "heading": int(parts[5]),
            "status": int(parts[6]),
        }
    except:
        return None

def make_beacon(
    frame_id: int,
    uuid: str,
    payload: str,
    offer_uuid: Optional[str] = None,
    offer_id: Optional[int] = None,
) -> str:
    """
    Beacon format:
    BCN,<frame>,<uuid>,<offer>,<payload>
    """
    offer_str = ""
    if offer_uuid and offer_id is not None:
        offer_str = f"{offer_uuid}:{offer_id}"
    return f"BCN,{frame_id:04d},{uuid or ''},{offer_str},{payload}"

def parse_beacon(data: str):
    """
    Beacon format:
    BCN,<frame>,<uuid>,<offer>,P:<master_id>:<seq>:<x>:<y>:<heading>:<status>
    """
    if not data.startswith("BCN,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) < 5:
            return None

        res = {
            "frame": int(parts[1]),
            "uuid": parts[2],
            "offer_uuid": None,
            "offer_id": None,
            "master_pos": None,
        }

        offer_field = parts[3]
        if offer_field and ":" in offer_field:
            try:
                o_parts = offer_field.split(":", 1)
                if len(o_parts) == 2:
                    res["offer_uuid"] = o_parts[0]
                    res["offer_id"] = int(o_parts[1])
            except (ValueError, IndexError):
                pass

        res["master_pos"] = parse_master_pos_field(parts[4])
        return res
    except (ValueError, IndexError):
        return None

def make_pos_payload(robot_id: int, seq: int, x: int, y: int, heading: int, status: int) -> str:
    """
    Robot broadcast position packet:
    POS,<id>,<seq_hex>,<x>,<y>,<heading>,<status>
    """
    return f"POS,{robot_id},{seq:02X},{x},{y},{heading},{status}"

def parse_pos_payload(data: str):
    if not data.startswith("POS,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) != 7:
            return None
        return {
            "id": int(parts[1]),
            "seq": int(parts[2], 16),
            "x": int(parts[3]),
            "y": int(parts[4]),
            "heading": int(parts[5]),
            "status": int(parts[6]),
        }
    except:
        return None

def make_uuid_payload(uuid: str, data16: str) -> str:
    return f"{uuid}:{data16}"

def parse_uuid_payload(data: str):
    if ":" not in data:
        return None
    try:
        u, d = data.split(":", 1)
        if len(u) != 4:
            return None
        return {"uuid": u, "data": d}
    except:
        return None

# ============================================================
# Stats / Radio Helpers
# ============================================================

@dataclass
class Stats:
    n: int = 0
    min_v: float = float("inf")
    max_v: float = float("-inf")
    sum_v: float = 0.0

    def add(self, v):
        self.n += 1
        self.sum_v += v
        if v < self.min_v:
            self.min_v = v
        if v > self.max_v:
            self.max_v = v

    def avg(self):
        return self.sum_v / self.n if self.n else 0

    def fmt(self):
        if self.n == 0:
            return "n=0"
        return f"n={self.n} min={self.min_v:.1f}ms avg={self.avg():.1f}ms max={self.max_v:.1f}ms"

def auto_slot(frame, base, offset, robots, margin, slot_offset_stats):
    vals = [slot_offset_stats[r].max_v for r in robots if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else 0.0

    denom = len(robots) - 1
    if denom <= 0:
        return 0
    budget = frame - margin - base - offset - worst_offset
    if budget <= 0:
        return 0
    return budget / denom

def auto_frame(slot, base, offset, robots, margin, slot_offset_stats):
    vals = [slot_offset_stats[r].max_v for r in robots if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else 0.0
    num_robots = len(robots)
    return base + (num_robots - 1) * slot + offset + worst_offset + margin

def auto_max_robots(frame, slot, base, offset, margin, slot_offset_stats):
    vals = [slot_offset_stats[r].max_v for r in slot_offset_stats if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else 0.0
    budget = frame - margin - base - offset - worst_offset
    if budget <= 0:
        return 1
    return int(budget / slot) + 1

def lora_airtime_seconds(sf, bw_hz, cr, preamble, payload_bytes):
    bw = bw_hz
    tsym = (2 ** sf) / bw
    de = 1 if (sf >= 11 and bw == 125000) else 0
    ih = 0
    crc = 1
    payload_symb_nb = 8 + max(
        math.ceil(
            (8 * payload_bytes - 4 * sf + 28 + 16 * crc - 20 * ih)
            / (4 * (sf - 2 * de))
        ) * (cr + 4),
        0,
    )
    t_preamble = (preamble + 4.25) * tsym
    t_payload = payload_symb_nb * tsym
    return t_preamble + t_payload

def init_radio(ser: serial.Serial, my_id: int, args):
    verbose = not args.quiet

    # Robust modem handshake:
    # 1) Try AT once.
    # 2) If no response, force ATZ and wait READY.
    # 3) Retry AT. If still no response, exit with an explicit error.
    if not at_expect_ok(ser, "AT", 0.5, verbose):
        if verbose:
            print("[WRN] No response to AT. Trying ATZ recovery...")
        write_cmd(ser, "ATZ", verbose)
        if not wait_ready(ser, 4.0, verbose):
            print("[ERR] Modem did not become ready after ATZ.")
            sys.exit(2)
        if not at_expect_ok(ser, "AT", 0.6, verbose):
            print("[ERR] Modem not responding to AT after ATZ recovery.")
            sys.exit(2)

    write_cmd(ser, "AT+OPMODE=1", verbose)
    time.sleep(0.6)

    need_reset = False
    while True:
        l = ser.readline().decode(errors="ignore").strip()
        if not l:
            break
        if verbose:
            print("[AT]", l)
        if "Need RESET" in l:
            need_reset = True

    if need_reset:
        write_cmd(ser, "ATZ", verbose)
        wait_ready(ser, 4.0, verbose)

    bw_code = parse_bw_to_code(args.bw)
    at_collect(ser, f"AT+ADDRESS={my_id}", 0.5, verbose)
    at_collect(ser, f"AT+BAND={args.band}", 0.5, verbose)
    at_collect(ser, f"AT+PARAMETER={args.sf},{bw_code},{args.cr},{args.preamble}", 0.6, verbose)
    at_collect(ser, f"AT+CRFOP={args.crfop}", 0.5, verbose)
    at_collect(ser, "AT+PARAMETER=?", 0.8, verbose)
    drain_uart(ser, 0.8, verbose)

def init_radio_master(ser, args):
    init_radio(ser, args.master, args)

def init_radio_robot(ser, args):
    init_radio(ser, args.robotid, args)

# ============================================================
# Common RX Parser
# ============================================================

def parse_rcv(line: str) -> Optional[Tuple[int, int, str, int, int]]:
    if not line.startswith("+RCV="):
        return None
    try:
        p = line[5:].split(",")
        if len(p) < 5:
            return None
        src = int(p[0])
        ln = int(p[1])
        data = ",".join(p[2:-2])
        rssi = int(p[-2])
        snr = int(p[-1])
        return src, ln, data, rssi, snr
    except:
        return None

# ============================================================
# Master Logic
# ============================================================

def run_server(args):
    verbose = not args.quiet
    robots = parse_robots(args.robots)
    server_id = args.master

    if args.frame is None:
        num_robots = len(robots)
        jitter_s = args.assumed_jitter_ms / 1000.0
        args.frame = args.base_delay + (num_robots - 1) * args.slot + args.tx_offset + jitter_s + args.margin
        if verbose:
            print(f"[AUTO-FRAME] Calculated frame duration: {args.frame:.4f}s for N={num_robots} robots")
            print(f"             (base={args.base_delay} slot={args.slot} off={args.tx_offset} jitter={jitter_s:.4f} guard={args.margin})")

    port = resolve_port(args.port)
    bw_code = parse_bw_to_code(args.bw)

    if verbose:
        print(
            f"[CFG] PORT={port} BAUD={args.baud} MASTER={args.master} "
            f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
            f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
            f"frame={args.frame:.4f}s robots={args.robots}"
        )

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_master(ser, args)

    per_expected: Dict[int, int] = {r: 0 for r in robots}
    per_ok: Dict[int, int] = {r: 0 for r in robots}
    per_mismatch: Dict[int, int] = {r: 0 for r in robots}

    robot_order = {rid: i for i, rid in enumerate(sorted(robots))}
    slot_offset_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    beacon_to_rx_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    last_heard_mono: Dict[int, float] = {r: 0.0 for r in robots}
    joined_at_frame: Dict[int, int] = {r: -1 for r in robots}

    auto_id_registry: Dict[str, int] = {}
    pending_offer_uuid: Optional[str] = None
    pending_offer_id: Optional[int] = None
    pending_offer_ttl: int = 0

    my_uuid = getattr(args, "my_uuid", None)
    if my_uuid is None:
        my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        setattr(args, "my_uuid", my_uuid)

    peer_table: Dict[int, Dict[str, Any]] = {}

    frame_id = 0
    next_frame_start = time.monotonic()

    print("TDMA MASTER START")

    while True:
        # Add micro randomization to avoid persistent phase-lock beacon collisions.
        jitter_break = random.uniform(-0.002, 0.002)
        start = next_frame_start + jitter_break
        sleep_until(start, busy_tail_s=0.002)
        next_frame_start = start + args.frame - jitter_break

        frame_id += 1
        if frame_id > 9999:
            frame_id = 1

        pose = get_local_pose(args)
        beacon_payload = make_uuid_payload(my_uuid, args.data16)
        beacon = make_beacon(
            frame_id=frame_id,
            uuid=my_uuid,
            payload=beacon_payload,
            offer_uuid=pending_offer_uuid,
            offer_id=pending_offer_id,
        )

        write_cmd(ser, f"AT+SEND=0,{len(beacon)},{beacon}", verbose=not args.quiet)

        # Update peer table for master itself
        update_peer_table(
            peer_table,
            server_id,
            role="master",
            x=pose["x"],
            y=pose["y"],
            heading=pose["heading"],
            status=pose["status"],
            seq=(frame_id % 256),
            rssi=0,
            snr=0,
            src_addr=server_id,
        )

        if pending_offer_ttl > 0:
            pending_offer_ttl -= 1
            if pending_offer_ttl <= 0:
                pending_offer_uuid = None
                pending_offer_id = None

        listen_end = start + args.frame - 0.015

        while time.monotonic() < listen_end:
            if ser.in_waiting == 0:
                time.sleep(0.001)
                continue

            line = read_line(ser)
            r = parse_rcv(line)
            if not r:
                continue

            src, ln, data, rssi, snr = r

            # Other master's beacon
            b = parse_beacon(data)
            if b:
                other_frame = b["frame"]
                other_uuid = b["uuid"]

                if verbose:
                    print(f"[!] WARNING: Heard BCN{other_frame:04d}@{other_uuid} from ID {src}! Dual-server conflict detected.")

                should_yield = False
                if src < server_id:
                    should_yield = True
                elif src == server_id:
                    if other_uuid and other_uuid < my_uuid:
                        should_yield = True

                if should_yield:
                    print(f"====== Yielding Master Role to ID {src} (UUID: {other_uuid}). Resetting and Re-Joining... ======")
                    ser.close()
                    args.robotid = None
                    run_auto_id(args)
                    return
                else:
                    if verbose:
                        print(f"[*] I have higher priority (my ID: {server_id}, UUID: {my_uuid}). Ignoring imposter ID {src}.")
                continue

            # JOIN request
            if isinstance(data, str) and data.startswith("JOIN:") and len(data) >= 9:
                uuid_str = str(data[5:9])
                if verbose:
                    print(f"[*] Heard JOIN request from UUID: {uuid_str}")

                if uuid_str in auto_id_registry:
                    assigned_id = auto_id_registry[uuid_str]
                else:
                    dead_uuids = []
                    for reg_uuid, reg_id in auto_id_registry.items():
                        if time.monotonic() - last_heard_mono.get(reg_id, 0) > args.lease_timeout_s:
                            dead_uuids.append(reg_uuid)

                    for dead in dead_uuids:
                        freed_id = auto_id_registry.pop(dead)
                        if verbose:
                            print(
                                f"[*] Reclaiming inactive ID {freed_id} from UUID {dead} "
                                f"(Silent for >{args.lease_timeout_s:.1f}s)"
                            )
                        per_expected[freed_id] = 0
                        per_ok[freed_id] = 0
                        per_mismatch[freed_id] = 0
                        slot_offset_stats[freed_id] = Stats()
                        beacon_to_rx_stats[freed_id] = Stats()
                        joined_at_frame[freed_id] = -1
                        last_heard_mono[freed_id] = 0.0

                    assigned_id = None
                    used_ids = set(auto_id_registry.values())
                    used_ids.add(args.master)

                    for r_id in robots:
                        if (
                            joined_at_frame.get(r_id, -1) != -1
                            and time.monotonic() - last_heard_mono.get(r_id, 0) <= args.lease_timeout_s
                        ):
                            used_ids.add(r_id)

                    for r_id in robots:
                        if r_id not in used_ids:
                            assigned_id = r_id
                            break

                    if assigned_id is None:
                        if verbose:
                            print("[!] ERROR: Request received but no available IDs in pool!")
                        continue

                    auto_id_registry[uuid_str] = assigned_id
                    last_heard_mono[assigned_id] = time.monotonic()
                    joined_at_frame[assigned_id] = frame_id

                    if verbose:
                        print(f"[*] Allocated ID {assigned_id} to UUID {uuid_str}")

                pending_offer_uuid = uuid_str
                pending_offer_id = assigned_id
                pending_offer_ttl = 3
                continue

            if pending_offer_id is not None and src == pending_offer_id:
                if verbose:
                    print(f"[*] Robot {src} joined successfully. Clearing persistent offer.")
                pending_offer_uuid = None
                pending_offer_id = None
                pending_offer_ttl = 0

            if src not in robots or src == server_id:
                continue

            # PER/peer tracking is payload-agnostic here:
            # any packet from the expected source in the expected slot counts.
            msg = parse_uuid_payload(data)

            slot_index = robot_order[src]
            t = now_ms(start)
            expected = (args.base_delay + slot_index * args.slot + args.tx_offset) * 1000.0
            slot_offset = t - expected

            last_heard_mono[src] = time.monotonic()
            if joined_at_frame[src] == -1:
                joined_at_frame[src] = frame_id

            update_peer_table(
                peer_table,
                src,
                role="robot",
                x=0,
                y=0,
                heading=0,
                status=0,
                seq=(frame_id % 256),
                rssi=rssi,
                snr=snr,
                src_addr=src,
            )

            if True:
                slot_offset_stats[src].add(slot_offset)
                beacon_to_rx_stats[src].add(t)
                if frame_id > joined_at_frame[src] + args.warmup:
                    per_ok[src] += 1
                if args.verbose_log:
                    if msg:
                        payload_info = f"uuid={msg['uuid']} data={msg['data']}"
                    else:
                        payload_info = f"raw={data[:64]}..."
                    print(
                        f"[RX OK] frame={frame_id} from={src} "
                        f"t={t:.1f}ms exp={expected:.1f}ms slot_offset={slot_offset:.1f}ms "
                        f"{payload_info}"
                    )

        for r in robots:
            if r != server_id and joined_at_frame[r] != -1:
                if frame_id > joined_at_frame[r] + args.warmup:
                    per_expected[r] += 1

        if frame_id % args.print_interval == 0:
            print("\n====== PER SUMMARY ======")
            total_ok = 0
            total_e = 0
            for r in robots:
                if joined_at_frame[r] == -1 or r == server_id:
                    continue
                e = per_expected[r]
                ok = per_ok[r]
                per = (e - ok) / e if e else 0
                print(f"Robot {r}: OK={ok}/{e} PER={per*100:.3f}% mismatch={per_mismatch[r]}")
                print("  BEACON_TO_RX:", beacon_to_rx_stats[r].fmt())
                print("  SLOT_OFFSET :", slot_offset_stats[r].fmt())
                total_ok += ok
                total_e += e

            gper = 1 - total_ok / total_e if total_e else 0
            print(f"GLOBAL PER = {gper*100:.3f}%")

            active_robots = [r for r in robots if joined_at_frame[r] != -1 and r != server_id]
            if args.auto_calc and active_robots:
                rec_slot = auto_slot(args.frame, args.base_delay, args.tx_offset, active_robots, args.margin, slot_offset_stats)
                rec_frame = auto_frame(args.slot, args.base_delay, args.tx_offset, active_robots, args.margin, slot_offset_stats)
                rec_max = auto_max_robots(args.frame, args.slot, args.base_delay, args.tx_offset, args.margin, slot_offset_stats)

                print("\n---- AUTO CALC ----")
                print(f"Recommended slot <= {rec_slot*1000:.1f} ms")
                print(f"Minimum frame needed ≈ {rec_frame*1000:.1f} ms")
                print(f"Max robots supportable ≈ {rec_max}")

                pos_payload_len = len(make_uuid_payload(my_uuid, args.data16))

                bw_hz = (
                    500000 if str(args.bw) in ("9", "500", "500k", "500000")
                    else 250000 if str(args.bw) in ("8", "250", "250k", "250000")
                    else 125000
                )

                airtime_s = lora_airtime_seconds(
                    sf=args.sf,
                    bw_hz=bw_hz,
                    cr=args.cr,
                    preamble=args.preamble,
                    payload_bytes=pos_payload_len,
                )

                offset_min_s = float("inf")
                offset_max_s = float("-inf")
                for r in active_robots:
                    if slot_offset_stats[r].n > 0:
                        offset_min_s = min(offset_min_s, slot_offset_stats[r].min_v)
                        offset_max_s = max(offset_max_s, slot_offset_stats[r].max_v)

                offset_span_s = 0.0
                if math.isfinite(offset_max_s) and math.isfinite(offset_min_s):
                    offset_span_s = max(0.0, (offset_max_s - offset_min_s) / 1000.0)

                guard_s = 0.010
                min_slot_s = airtime_s + offset_span_s + guard_s
                print(f"Minimum slot needed ≥ {min_slot_s*1000:.1f} ms")
                print("-------------------")

            print_peer_table(
                peer_table,
                args.peer_timeout_s,
                expected_ids=robots,
                master_id=server_id,
            )
            print("========================\n")

# ============================================================
# Client Logic
# ============================================================

def run_client(args):
    verbose = not args.quiet

    if args.frame is None:
        if args.robots:
            robots = parse_robots(args.robots)
            num_robots = len(robots)
            jitter_s = args.assumed_jitter_ms / 1000.0
            args.frame = args.base_delay + (num_robots - 1) * args.slot + args.tx_offset + jitter_s + args.margin
            if verbose:
                print(f"[AUTO-FRAME] Calculated frame duration: {args.frame:.4f}s for N={num_robots} robots")
        else:
            args.frame = 1.5
            if verbose:
                print(f"[WRN] --frame not provided and --robots missing. Using default {args.frame}s")

    port = resolve_port(args.port)
    bw_code = parse_bw_to_code(args.bw)

    if verbose:
        print(
            f"[CFG] PORT={port} BAUD={args.baud} MY_ID={args.robotid} MASTER={args.master} "
            f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
            f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
            f"rx_delay={args.rx_delay_ms:.1f}ms busy_tail={args.busy_tail_ms:.1f}ms"
        )

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_robot(ser, args)

    if verbose:
        print("TDMA Robot (broadcast-pos mode) running...")

    last_frame = None
    rx_delay_s = args.rx_delay_ms / 1000.0
    busy_tail_s = max(0.0, args.busy_tail_ms / 1000.0)

    base_failover_s = args.frame * 5.0
    stagger_delay_s = max(0.0, (args.robotid - 1) * 6.0)
    failover_timeout_s = base_failover_s + stagger_delay_s

    last_beacon_mono = time.monotonic()
    peer_table: Dict[int, Dict[str, Any]] = {}

    while True:
        if args.auto_role and (time.monotonic() - last_beacon_mono) > failover_timeout_s:
            if verbose:
                print(f"[!] No beacons for {failover_timeout_s:.1f}s (Staggered for ID {args.robotid}). Server died! Commencing Failover...")

            ser.reset_input_buffer()
            ser.close()
            print("====== Switching to SERVER mode (Failover) ======")

            args.master = 1

            if verbose and args.robots:
                print(f"[*] Keep robot pool for server: {args.robots}")

            run_server(args)
            return

        if args.late_drop_ms is not None:
            late_drop_s = args.late_drop_ms / 1000.0
        else:
            late_drop_s = min(0.03, args.slot * 0.3)

        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue

        r = parse_rcv(line)
        if not r:
            continue

        src, ln, data, rssi, snr = r

        # 1) Beacon handling
        b = parse_beacon(data)
        if b:
            last_beacon_mono = time.monotonic()

            frame = b["frame"]
            master_pos = b["master_pos"]
            if master_pos:
                update_peer_table(
                    peer_table,
                    master_pos["id"],
                    role="master",
                    x=master_pos["x"],
                    y=master_pos["y"],
                    heading=master_pos["heading"],
                    status=master_pos["status"],
                    seq=master_pos["seq"],
                    rssi=rssi,
                    snr=snr,
                    src_addr=src,
                )
            else:
                # Payload format may vary; still keep master alive using beacon frame SN.
                update_peer_table(
                    peer_table,
                    args.master,
                    role="master",
                    x=0,
                    y=0,
                    heading=0,
                    status=0,
                    seq=(frame % 256),
                    rssi=rssi,
                    snr=snr,
                    src_addr=src,
                )
            if frame == last_frame:
                continue
            last_frame = frame

            read_m = time.monotonic()
            beacon_rx_m = read_m - rx_delay_s

            robots = parse_robots(args.robots)
            robot_order = {rid: i for i, rid in enumerate(sorted(robots))}
            if args.robotid not in robot_order:
                if verbose:
                    print(f"[ERR] robotid {args.robotid} not in robots list {args.robots}")
                time.sleep(1)
                continue

            slot_index = robot_order[args.robotid]
            tx_time = beacon_rx_m + args.base_delay + slot_index * args.slot + args.tx_offset
            now_m = time.monotonic()

            if read_m > tx_time:
                if verbose:
                    diff_ms = (read_m - tx_time) * 1000.0
                    print(f"[!] WARNING: Slot is unreachable! Processing finished {diff_ms:.1f}ms AFTER target TX time.")
                    print(f"    Possible fixes: Increase --base-delay (current: {args.base_delay}s) or decrease --rx-delay-ms (current: {args.rx_delay_ms}ms)")

            if now_m > tx_time + late_drop_s:
                if verbose:
                    late_ms = (now_m - beacon_rx_m) * 1000.0
                    print(f"[SKIP] late frame={frame} now-beacon={late_ms:.1f}ms (threshold={late_drop_s*1000:.1f}ms)")
                continue

            sleep_until(tx_time, busy_tail_s=busy_tail_s)

            payload_ascii = make_uuid_payload(args.my_uuid, args.data16)

            # Broadcast to all
            write_cmd(ser, f"AT+SEND=0,{len(payload_ascii)},{payload_ascii}", verbose)

            # Update local self-entry (optional but useful)
            update_peer_table(
                peer_table,
                args.robotid,
                role="robot",
                x=0,
                y=0,
                heading=0,
                status=0,
                seq=(frame % 256),
                rssi=0,
                snr=0,
                src_addr=args.robotid,
            )

            if frame % args.print_interval == 0:
                print_peer_table(
                    peer_table,
                    args.peer_timeout_s,
                    expected_ids=robots,
                    master_id=args.master,
                )

            if verbose:
                after_ms = (time.monotonic() - beacon_rx_m) * 1000.0
                print(f"[SENT] frame={frame} after_beacon={after_ms:.1f}ms RSSI={rssi} SNR={snr} (rx_delay={args.rx_delay_ms:.1f}ms)")
            continue

        # 2) Robot POS broadcast handling
        msg = parse_uuid_payload(data)
        if msg:
            update_peer_table(
                peer_table,
                src,
                role="robot",
                x=0,
                y=0,
                heading=0,
                status=0,
                seq=0,
                rssi=rssi,
                snr=snr,
                src_addr=src,
            )

            if verbose and args.verbose_log:
                print(
                    f"[PEER] src={src} uuid={msg['uuid']} data={msg['data']} "
                    f"RSSI={rssi} SNR={snr}"
                )
            continue

# ============================================================
# Discovery / Auto Role / Auto ID
# ============================================================

def listen_for_beacon(ser: serial.Serial, timeout_s: float, verbose: bool) -> bool:
    drain_uart(ser, 0.5, verbose)

    actual_timeout = timeout_s + random.uniform(0, 2.0)

    if verbose:
        print(f"[*] Listening for existing master beacons for {actual_timeout:.1f}s (jittered)...")

    end = time.time() + actual_timeout
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue

        r = parse_rcv(line)
        if not r:
            continue

        src, ln, data, rssi, snr = r
        b = parse_beacon(data)
        if b:
            if verbose:
                print(f"[!] Master detected! (Beacon from {src}: BCN{b['frame']:04d}@{b['uuid']} RSSI={rssi})")
            return True

    if verbose:
        print(f"[*] No master detected after {actual_timeout:.1f}s.")
    return False

def dynamic_run(args):
    port = resolve_port(args.port)
    ser = serial.Serial(port, args.baud, timeout=0.1)

    init_radio_robot(ser, args)
    heard_beacon = listen_for_beacon(ser, args.listen_timeout, not args.quiet)
    ser.close()

    if heard_beacon:
        print("====== Switching to CLIENT mode ======")
        run_client(args)
    else:
        print("====== Switching to SERVER mode ======")
        run_server(args)

def run_auto_id(args):
    my_uuid = getattr(args, "my_uuid", None)
    if my_uuid is None:
        my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        setattr(args, "my_uuid", my_uuid)

    port = resolve_port(args.port)

    args.robotid = random.randint(30000, 60000)

    if not args.quiet:
        print(f"[*] Starting Auto-ID process. My UUID is {my_uuid}")
        print(f"[*] Initial radio setup with temporary ID {args.robotid}")

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_robot(ser, args)

    heard_beacon = listen_for_beacon(ser, args.listen_timeout, not args.quiet)

    if not heard_beacon:
        args.robotid = 1
        ser.close()
        if not args.quiet:
            print("====== Switching to SERVER mode (ID 1) ======")
        run_server(args)
        return

    assigned_id = None
    rx_delay_s = args.rx_delay_ms / 1000.0
    busy_tail_s = max(0.0, args.busy_tail_ms / 1000.0)
    join_cooldown_frames = random.randint(1, 3)

    if not args.quiet:
        print("[*] Master detected. Waiting for Beacons to send JOIN request...")

    while assigned_id is None:
        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue

        r = parse_rcv(line)
        if not r:
            continue

        _, _, data, _, _ = r
        b = parse_beacon(data)
        if not b:
            continue

        frame = b["frame"]
        offer_uuid = b["offer_uuid"]
        offered_id = b["offer_id"]

        if offer_uuid == my_uuid:
            try:
                assigned_id = offered_id
                if not args.quiet:
                    print(f"[!] SUCCESS! Master assigned me ID {assigned_id}. Applying configuration...")
                break
            except Exception as e:
                if not args.quiet:
                    print(f"[WRN] Failed to apply ID offer: {e}")

        if join_cooldown_frames > 0:
            join_cooldown_frames -= 1
            continue

        read_m = time.monotonic()
        beacon_rx_m = read_m - rx_delay_s
        robot_list = parse_robots(args.robots)
        num_robots = len(robot_list)

        join_tx_time = beacon_rx_m + args.base_delay + (num_robots) * args.slot + args.tx_offset
        sleep_until(join_tx_time, busy_tail_s=busy_tail_s)

        join_payload = f"JOIN:{my_uuid}"
        write_cmd(ser, f"AT+SEND={args.master},{len(join_payload)},{join_payload}", not args.quiet)

        join_cooldown_frames = random.randint(2, 5)

    args.auto_role = True
    ser.close()

    args.robotid = assigned_id
    print(f"====== Transitioning to Normal CLIENT Mode as ID {assigned_id} ======")
    run_client(args)

# ============================================================
# Main Entry Point
# ============================================================

def parse_args():
    ap = argparse.ArgumentParser("TDMA System (Master/Robot) - broadcast position mode")

    group = ap.add_mutually_exclusive_group(required=False)
    group.add_argument("--server", action="store_true", help="Run as TDMA Master")
    group.add_argument("--client", action="store_true", help="Run as TDMA Robot")
    group.add_argument("--auto-role", action="store_true", help="Listen for beacons. If none found, become server. Otherwise, become client.")
    group.add_argument("--auto-id", action="store_true", help="Start as unassigned client, request ID dynamically from Master.")

    # Common radio
    ap.add_argument("--port", default=None, help="Serial port: 0->/dev/ttyUSB0 or /dev/ttyUSBX")
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--master", type=int, default=1, help="Network ID of the Master")
    ap.add_argument("--band", type=int, default=915000000)
    ap.add_argument("--crfop", type=int, default=22)
    ap.add_argument("--sf", type=int, default=5)
    ap.add_argument("--bw", default="500")
    ap.add_argument("--cr", type=int, default=1)
    ap.add_argument("--preamble", type=int, default=12)
    ap.add_argument("--slot", type=float, default=0.05)
    ap.add_argument("--base-delay", type=float, default=0.10)
    ap.add_argument("--tx-offset", type=float, default=0.008)
    ap.add_argument("--data16", default="v1QDALr//QIJgFg6", help="Payload data part, e.g. v1QDALr//QIJgFg6")
    ap.add_argument("--quiet", action="store_true", help="Suppress verbose logging")

    # Timing / summary
    ap.add_argument("--robots", default="1-10", help="Comma-separated list (e.g. 2-5)")
    ap.add_argument("--frame", type=float, default=None, help="Frame duration (seconds). If None, calculated automatically.")
    ap.add_argument("--warmup", type=int, default=10, help="Warmup frame ignore count (server)")
    ap.add_argument("--print-interval", type=int, default=10, help="Print summary every X frames (server)")
    ap.add_argument("--margin", type=float, default=0.015, help="Time margin for auto calculations")
    ap.add_argument("--assumed-jitter-ms", type=float, default=1.8, help="Expected timing jitter/error (ms) for auto-frame calculation")
    ap.add_argument("--auto-calc", action="store_true", help="Auto parameter recommendations (server)")
    ap.add_argument("--verbose-log", action="store_true", help="Verbose RX debug logging")
    ap.add_argument("--peer-timeout-s", type=float, default=5.0, help="Peer stale timeout for table display")
    ap.add_argument("--lease-timeout-s", type=float, default=30.0, help="Auto-ID lease reclaim timeout in seconds")

    # Client / auto
    ap.add_argument("--robotid", type=int, required=False, help="Node ID. Required for --client or --auto-role")
    ap.add_argument("--rx-delay-ms", type=float, default=166.0, help="RF to serial latency compensation (client)")
    ap.add_argument("--busy-tail-ms", type=float, default=2.0, help="Precise timing busy-wait tail (client)")
    ap.add_argument("--listen-timeout", type=float, default=6.0, help="Wait time in seconds to detect an existing master")
    ap.add_argument("--late-drop-ms", type=float, default=None, help="Threshold to skip late frames. Defaults to min(30ms, slot*0.3)")

    # Local pose (minimal version)
    ap.add_argument("--x", type=int, default=0, help="Local X position")
    ap.add_argument("--y", type=int, default=0, help="Local Y position")
    ap.add_argument("--heading", type=int, default=0, help="Local heading")
    ap.add_argument("--status", type=int, default=0, help="Local status / battery / state code")

    return ap.parse_args()

def main():
    args = parse_args()

    setattr(args, "my_uuid", f"{random.randint(0, 0xFFFF):04X}")

    if args.server:
        if not args.robots:
            print("[ERR] --robots is required when running as --server")
            sys.exit(2)
        run_server(args)
    elif args.client:
        if not args.robotid or not args.robots:
            print("[ERR] Both --robotid and --robots are required when running as --client")
            sys.exit(2)
        if args.robotid <= 0 or args.robotid > 65535:
            print("[ERR] --robotid must be 1..65535 (LoRa Address Range)")
            sys.exit(2)
        run_client(args)
    elif args.auto_role:
        if not args.robotid or not args.robots:
            print("[ERR] Both --robotid and --robots are required when running as --auto-role")
            sys.exit(2)
        dynamic_run(args)
    elif args.auto_id:
        if not args.robots:
            print("[ERR] --robots is required when running as --auto-id")
            sys.exit(2)
        run_auto_id(args)
    else:
        if not args.quiet:
            print("[INFO] No mode specified. Defaulting to --auto-id")
        run_auto_id(args)

if __name__ == "__main__":
    main()
