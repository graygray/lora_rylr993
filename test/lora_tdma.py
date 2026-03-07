#!/usr/bin/env python3
import argparse
import serial
import time
import math
import sys
from dataclasses import dataclass
import random
from typing import Dict, List, Optional, Tuple

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

# ============================================================
# Master Utilities & Logic
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

def parse_payload_hex(data_hex: str):
    if not data_hex or len(data_hex) < 3:
        return None
    try:
        # Format: 1 hex char ID (0-F), 2 hex char Seq (00-FF)
        rid = int(data_hex[0:1], 16)
        fid = int(data_hex[1:3], 16)
        data_part = data_hex[3:]
        return rid, fid, data_part
    except:
        return None

def auto_slot(frame, base, offset, robots, margin, jitter_stats):
    vals = [jitter_stats[r].max_v for r in robots if math.isfinite(jitter_stats[r].max_v)]
    worst_j = max(vals) / 1000.0 if vals else 0.0
    
    max_id = max(robots) if robots else 1
    denom = max_id - 1
    if denom <= 0:
        return 0
    budget = frame - margin - base - offset - worst_j
    if budget <= 0:
        return 0
    return budget / denom

def auto_frame(slot, base, offset, robots, margin, jitter_stats):
    vals = [jitter_stats[r].max_v for r in robots if math.isfinite(jitter_stats[r].max_v)]
    worst_j = max(vals) / 1000.0 if vals else 0.0
    max_id = max(robots) if robots else 1
    return base + (max_id - 1)*slot + offset + worst_j + margin

def auto_max_robots(frame, slot, base, offset, margin, jitter_stats):
    vals = [jitter_stats[r].max_v for r in jitter_stats if math.isfinite(jitter_stats[r].max_v)]
    worst_j = max(vals) / 1000.0 if vals else 0.0
    budget = frame - margin - base - offset - worst_j
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

def init_radio_master(ser, args):
    verbose = not args.quiet
    write_cmd(ser, "AT", verbose)
    time.sleep(0.3)
    write_cmd(ser, "AT+OPMODE=1", verbose)
    time.sleep(0.5)
    write_cmd(ser, "ATZ", verbose)
    time.sleep(2.0)
    write_cmd(ser, f"AT+ADDRESS={args.master}", verbose)
    time.sleep(0.5)
    write_cmd(ser, f"AT+BAND={args.band}", verbose)
    time.sleep(0.5)
    bw_code = parse_bw_to_code(args.bw)
    write_cmd(ser, f"AT+PARAMETER={args.sf},{bw_code},{args.cr},{args.preamble}", verbose)
    time.sleep(0.5)
    write_cmd(ser, f"AT+CRFOP={args.crfop}", verbose)
    time.sleep(0.5)

def run_server(args):
    verbose = not args.quiet
    robots = parse_robots(args.robots)
    
    if args.frame is None:
        # Formula from user: min_frame = base + (N-1)*slot + tx_offset + jitter_max + guard
        # Using jitter_max = 0.0018 (constant from image) and margin as guard (0.03)
        max_id = max(robots) if robots else 1
        args.frame = args.base_delay + (max_id - 1) * args.slot + args.tx_offset + 0.0018 + args.margin
        if verbose:
            print(f"[AUTO-FRAME] Calculated frame duration: {args.frame:.4f}s for N={max_id} robots")
            print(f"             (base={args.base_delay} slot={args.slot} off={args.tx_offset} jitter=0.0018 guard={args.margin})")

    port = resolve_port(args.port)
    bw_code = parse_bw_to_code(args.bw)

    if verbose:
        print(f"[CFG] PORT={port} BAUD={args.baud} MASTER={args.master} "
              f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
              f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
              f"frame={args.frame:.4f}s robots={args.robots}")

    ser = serial.Serial(port, args.baud, timeout=0.1)

    init_radio_master(ser, args)

    per_expected: Dict[int, int] = {r: 0 for r in robots}
    per_ok: Dict[int, int] = {r: 0 for r in robots}
    per_mismatch: Dict[int, int] = {r: 0 for r in robots}

    jitter_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    rtt_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    last_heard_frame: Dict[int, int] = {r: 0 for r in robots}
    joined_at_frame: Dict[int, int] = {r: -1 for r in robots}

    frame_id = 0
    
    # Auto-ID Registry (UUID string -> assigned Robot ID integer)
    auto_id_registry: Dict[str, int] = {}
    pending_offer: str = "" # e.g., "_A3F9:5"
    
    print("TDMA MASTER START")

    # Ensure we use the same UUID allocated during Auto-ID/Main
    my_uuid = getattr(args, 'my_uuid', None)
    if my_uuid is None:
        my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        setattr(args, 'my_uuid', my_uuid)
    
    # Aggregated relay data from robots (ID -> Payload Data)
    relay_pool: Dict[int, str] = {}
    
    # Stable frame timing logic
    next_frame_start = time.monotonic()
    
    while True:
        # Increase jitter (0-150ms) to break phase-lock in dual-master collisions
        jitter_break = random.uniform(0, 0.150)
        start = next_frame_start + jitter_break
        sleep_until(start, busy_tail_s=0.002)
        
        # Calculate next expected frame start for stability
        next_frame_start = start + args.frame - jitter_break
        
        frame_id += 1
        
        # Build aggregated data string: +2data+3data (stripping robot sequence)
        relay_str = ""
        for rid in sorted(relay_pool.keys()):
            relay_str += f"+{rid:1x}{relay_pool[rid]}"
        
        if frame_id > 9999:
            frame_id = frame_id % 10000
        beacon = f"BCN{frame_id:04d}@{my_uuid}{pending_offer}{relay_str}"
        
        # LoRa RYLR993 / AT+SEND typically has a 242-byte limit
        if len(beacon) > 242:
            if not args.quiet:
                print(f"[WRN] Beacon length ({len(beacon)}) exceeds 242 limit! Truncating aggregated data.")
            beacon = beacon[:242]

        write_cmd(ser, f"AT+SEND=0,{len(beacon)},{beacon}", verbose=not args.quiet)
        pending_offer = "" # Clear the offer after sending it once
        relay_pool = {}    # Clear relay pool for the new frame

        listen_end = start + args.frame - 0.015 # Extra margin for processing
        received = set()

        while time.monotonic() < listen_end:
            # Use non-blocking check to avoid 100ms timeout lag
            if ser.in_waiting == 0:
                time.sleep(0.001)
                continue

            line = read_line(ser)
            r = parse_rcv(line)
            if not r:
                continue
            
            src, ln, data, rssi, snr = r

            # Dual-server conflict resolution:
            # If we are the Server but we hear a Beacon from someone else...
            if data.startswith("BCN") and len(data) >= 7:
                try:
                    other_frame = int(data[3:7])
                    other_uuid = ""
                    if "@" in data:
                        # BCNxxxx@UUID...
                        at_idx = data.find("@")
                        other_uuid = data[at_idx+1 : at_idx+5]

                    if verbose:
                        print(f"[!] WARNING: Heard BCN{other_frame:04d}@{other_uuid} from ID {src}! Dual-server conflict detected.")
                    
                    # Tie-breaker logic:
                    # 1. Lower ID wins (1 is highest priority).
                    # 2. If IDs are the same, lower UUID wins.
                    # We yield if:
                    #   - The other ID is lower than ours (e.g. ID 1 vs ID 2)
                    #   - The other ID is THE SAME but their UUID is lower (lexicographical)
                    
                    should_yield = False
                    if src < args.robotid: 
                        should_yield = True
                    elif src == args.robotid:
                        if other_uuid and other_uuid < my_uuid:
                            should_yield = True
                    
                    if should_yield:
                        print(f"====== Yielding Master Role to ID {src} (UUID: {other_uuid}). Resetting and Re-Joining... ======")
                        ser.close()
                        # Reset ID so we don't try to reuse "ID 1" as a client
                        args.robotid = None
                        run_auto_id(args)
                        return
                    else:
                        if verbose:
                            print(f"[*] I have higher priority (my ID: {args.robotid}, UUID: {my_uuid}). Ignoring imposter ID {src}.")
                except ValueError:
                    pass
                continue

            # Auto-ID Process: Listen for JOIN requests
            # JOIN payloads look like: JOIN:A3F9
            # The length is typically 9 chars: JOIN (4) + : (1) + UUID (4)
            if isinstance(data, str) and data.startswith("JOIN:") and len(data) >= 9:
                uuid_str = str(data[5:9])
                if verbose:
                    print(f"[*] Heard JOIN request from UUID: {uuid_str}")
                
                # Check if already assigned
                if uuid_str in auto_id_registry:
                    assigned_id = auto_id_registry[uuid_str]
                else:
                    # Cleanup dead leases: If an ID hasn't been heard from in >15 frames, release it!
                    # We need a list to avoid modifying dict while iterating
                    dead_uuids = []
                    for reg_uuid, reg_id in auto_id_registry.items():
                        if frame_id - last_heard_frame.get(reg_id, 0) > 15:
                            dead_uuids.append(reg_uuid)
                    
                    for dead in dead_uuids:
                        freed_id = auto_id_registry.pop(dead)
                        if verbose:
                            print(f"[*] Reclaiming inactive ID {freed_id} from UUID {dead} (Silent for >15 frames)")
                        # Reset stats for the reclaimed ID
                        per_expected[freed_id] = 0
                        per_ok[freed_id] = 0
                        per_mismatch[freed_id] = 0
                        jitter_stats[freed_id] = Stats()
                        rtt_stats[freed_id] = Stats()
                        joined_at_frame[freed_id] = -1
                        last_heard_frame[freed_id] = 0

                    # Find lowest available ID from args.robots
                    # Example: if robots=[1,2,3,4,5], we need to find one that is NOT in registry
                    # but wait, robots list defines total capacity. 
                    # Let's say all IDs in robots list (except master) are available pool.
                    assigned_id = None
                    used_ids = set(auto_id_registry.values())
                    used_ids.add(args.master) # Server takes its own ID
                    
                    # Also consider IDs used if we've heard traffic from them recently
                    # (This handles robots that were already running if the Master restarts)
                    for r_id in robots:
                        if frame_id - last_heard_frame.get(r_id, -100) <= 15:
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
                    
                    # IMPORTANT: Initialize the lease timer so we don't immediately revoke it next frame
                    last_heard_frame[assigned_id] = frame_id
                    joined_at_frame[assigned_id] = frame_id
                    
                    if verbose:
                        print(f"[*] Allocated ID {assigned_id} to UUID {uuid_str}")
                        
                # Queue the offer for the next beacon
                pending_offer = f"_{uuid_str}:{assigned_id}"
                continue

            if src not in robots or src == args.robotid:
                continue

            t = now_ms(start)
            expected = (args.base_delay + (src-1)*args.slot + args.tx_offset) * 1000.0
            jitter = t - expected

            pp = parse_payload_hex(data)
            if not pp:
                if args.verbose_log:
                    print(f"[RX BAD] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms raw={data[:16]}...")
                continue

            rid, fid, dpart = pp
            
            # If we successfully parsed a payload from them, update their lease!
            last_heard_frame[src] = frame_id
            if joined_at_frame[src] == -1:
                joined_at_frame[src] = frame_id
            
            # Store data for next beacon relay
            relay_pool[src] = dpart
            
            # 8-bit sequence number rollover sync
            if fid == (frame_id % 256):
                received.add(src)
                jitter_stats[src].add(jitter)
                rtt_stats[src].add(t)
                if joined_at_frame[src] != -1 and frame_id > joined_at_frame[src] + args.warmup:
                    per_ok[src] += 1
                if args.verbose_log:
                    print(f"[RX OK] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms rid={rid}")
            else:
                if joined_at_frame[src] != -1 and frame_id > joined_at_frame[src] + args.warmup:
                    per_mismatch[src] += 1
                if args.verbose_log:
                    print(f"[RX MISMATCH] expect={frame_id % 256} got={fid} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms rid={rid}")

        for r in robots:
            if r != args.robotid and joined_at_frame[r] != -1:
                # Per-robot warmup: start counting expected after joined_at_frame + warmup
                if frame_id > joined_at_frame[r] + args.warmup:
                    per_expected[r] = per_expected.get(r, 0) + 1

        if frame_id % args.print_interval == 0:
            print("\n====== PER SUMMARY ======")
            total_ok = 0
            total_e = 0
            for r in robots:
                if joined_at_frame[r] == -1 or r == args.robotid:
                    continue
                e = per_expected[r]
                ok = per_ok[r]
                per = (e-ok)/e if e else 0
                print(f"Robot {r}: OK={ok}/{e} PER={per*100:.3f}% mismatch={per_mismatch[r]}")
                print("  RTT   :", rtt_stats[r].fmt())
                print("  JITTER:", jitter_stats[r].fmt())
                total_ok += ok
                total_e += e

            gper = 1 - total_ok/total_e if total_e else 0
            print(f"GLOBAL PER = {gper*100:.3f}%")

            if args.auto_calc:
                rec_slot = auto_slot(args.frame, args.base_delay, args.tx_offset, robots, args.margin, jitter_stats)
                rec_frame = auto_frame(args.slot, args.base_delay, args.tx_offset, robots, args.margin, jitter_stats)
                rec_max = auto_max_robots(args.frame, args.slot, args.base_delay, args.tx_offset, args.margin, jitter_stats)

                print("\n---- AUTO CALC ----")
                print(f"Recommended slot <= {rec_slot*1000:.1f} ms")
                print(f"Minimum frame needed ≈ {rec_frame*1000:.1f} ms")
                print(f"Max robots supportable ≈ {rec_max}")

                bw_hz = 500000 if str(args.bw) in ("9", "500", "500k", "500000") else \
                        250000 if str(args.bw) in ("8", "250", "250k", "250000") else \
                        125000

                airtime_s = lora_airtime_seconds(
                    sf=args.sf, bw_hz=bw_hz, cr=args.cr, preamble=args.preamble, payload_bytes=args.payload_bytes
                )

                jitter_span_s = 0.0
                # Using overall jitter range since auto minimum
                jitter_min_s = float("inf")
                jitter_max_s = float("-inf")
                for r in robots:
                    if jitter_stats[r].n > 0:
                        if jitter_stats[r].min_v < jitter_min_s: jitter_min_s = jitter_stats[r].min_v
                        if jitter_stats[r].max_v > jitter_max_s: jitter_max_s = jitter_stats[r].max_v
                try:
                    if math.isfinite(jitter_max_s) and math.isfinite(jitter_min_s):
                        jitter_span_s = max(0.0, (jitter_max_s - jitter_min_s)/1000.0)
                except:
                    jitter_span_s = 0.0

                guard_s = 0.010
                min_slot_s = airtime_s + jitter_span_s + guard_s

                print(f"Minimum slot needed ≥ {min_slot_s*1000:.1f} ms")
                print("-------------------")
            print("========================\n")


# ============================================================
# Robot Utilities & Logic
# ============================================================

def parse_rcv(line: str) -> Optional[Tuple[int, int, str, int, int]]:
    if not line.startswith("+RCV="):
        return None
    try:
        p = line[5:].split(",", 4)
        src = int(p[0]); ln = int(p[1]); data = p[2]
        rssi = int(p[3]); snr = int(p[4])
        return src, ln, data, rssi, snr
    except:
        return None

def make_payload_ascii(my_id: int, frame_id: int, payload_bytes: int) -> str:
    if payload_bytes < 3:
        raise ValueError("--payload-bytes must be >= 3.")
    # ID is 1 char (0-15), Seq is 2 chars (0-255 rollover)
    id_char = f"{(my_id & 0xF):1x}"
    header = f"{id_char}{(frame_id % 256):02x}"
    dummy_len = payload_bytes - 3
    return header + (id_char * dummy_len)

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

def init_radio_robot(ser, args):
    verbose = not args.quiet
    at_collect(ser, "AT", 0.3, verbose)
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
    at_collect(ser, f"AT+ADDRESS={args.robotid}", 0.5, verbose)
    at_collect(ser, f"AT+BAND={args.band}", 0.5, verbose)
    at_collect(ser, f"AT+PARAMETER={args.sf},{bw_code},{args.cr},{args.preamble}", 0.6, verbose)
    at_collect(ser, f"AT+CRFOP={args.crfop}", 0.5, verbose)
    at_collect(ser, "AT+PARAMETER=?", 0.8, verbose)
    drain_uart(ser, 0.8, verbose)

def run_client(args):
    verbose = not args.quiet
    
    # If robots provided without frame, auto-calculate it to stay in sync with server
    if args.frame is None:
        if args.robots:
            robots = parse_robots(args.robots)
            max_id = max(robots) if robots else 1
            args.frame = args.base_delay + (max_id - 1) * args.slot + args.tx_offset + 0.0018 + args.margin
            if verbose:
                print(f"[AUTO-FRAME] Calculated frame duration: {args.frame:.4f}s for N={max_id} robots")
        else:
            args.frame = 1.5 # Legacy default fallback
            if verbose:
                print(f"[WRN] --frame not provided and --robots missing. Using default {args.frame}s")

    port = resolve_port(args.port)
    bw_code = parse_bw_to_code(args.bw)

    if verbose:
        print(f"[CFG] PORT={port} BAUD={args.baud} MY_ID={args.robotid} MASTER={args.master} "
              f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
              f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
              f"payload={args.payload_bytes}B rx_delay={args.rx_delay_ms:.1f}ms busy_tail={args.busy_tail_ms:.1f}ms")

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_robot(ser, args)

    if verbose:
        print("TDMA Robot (calibrated) running...")

    last_frame = None
    rx_delay_s = args.rx_delay_ms / 1000.0
    busy_tail_s = max(0.0, args.busy_tail_ms / 1000.0)
    
    # Store the timeout threshold (e.g. 5 missed frames)
    # To prevent multiple robots from becoming Server at the exact same moment,
    # we add a staggered delay based on their robotid. 
    # Robot 1 waits base + 0s. Robot 2 waits base + 6s. Robot 3 waits base + 12s, etc.
    # We need a large stagger because the LoRa module takes ~5 seconds to Soft Reset (ATZ) 
    # and reconfigure itself as a Master before it can send the first beacon!
    
    # Normally master=1. If ID 1 is currently a client, it should failover first.
    # If ID 1 goes down, ID 2 failovers next.
    base_failover_s = args.frame * 5.0
    stagger_delay_s = max(0.0, (args.robotid - 1) * 6.0)
    failover_timeout_s = base_failover_s + stagger_delay_s
    
    last_beacon_mono = time.monotonic()

    while True:
        # Check for failover if acting as a client under --auto-role
        if args.auto_role and (time.monotonic() - last_beacon_mono) > failover_timeout_s:
            if verbose:
                print(f"[!] No beacons for {failover_timeout_s:.1f}s (Staggered for ID {args.robotid}). Server died! Commencing Failover...")
            
            # Flush any pending data before we switch roles
            ser.reset_input_buffer()
            ser.close()
            print("====== Switching to SERVER mode (Failover) ======")
            
            # When we take over, we MUST act as the new MASTER (address 1)
            # so the remaining clients don't have to change their configuration.
            # We rewrite args.master to be 1, but we still broadcast to the original robots list.
            args.master = 1
            
            run_server(args)
            return  # run_server handles the infinite loop from here

        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue

        r = parse_rcv(line)
        if not r:
            continue

        src, ln, data, rssi, snr = r

        if not (data.startswith("BCN") and len(data) >= 7):
            continue

        # If we reach here, we got a valid beacon!
        # Reset our failover watchdog so we don't promote ourselves
        last_beacon_mono = time.monotonic()

        try:
            frame = int(data[3:7])
        except:
            continue

        if frame == last_frame:
            continue
        last_frame = frame

        read_m = time.monotonic()
        beacon_rx_m = read_m - rx_delay_s
        tx_time = beacon_rx_m + args.base_delay + (args.robotid - 1) * args.slot + args.tx_offset
        now_m = time.monotonic()
        
        # Safety check: If the target TX time is already in the past relative to NOW (read_m),
        # it means the serial latency (rx_delay) is longer than the reserved base_delay + offsets.
        if read_m > tx_time:
            if verbose:
                diff_ms = (read_m - tx_time) * 1000.0
                print(f"[!] WARNING: Slot is unreachable! Processing finished {diff_ms:.1f}ms AFTER target TX time.")
                print(f"    Possible fixes: Increase --base-delay (current: {args.base_delay}s) or decrease --rx-delay-ms (current: {args.rx_delay_ms}ms)")

        if now_m > tx_time + 0.03:
            if verbose:
                late_ms = (now_m - beacon_rx_m) * 1000.0
                print(f"[SKIP] late frame={frame} now-beacon={late_ms:.1f}ms (rx_delay={args.rx_delay_ms:.1f}ms)")
            continue

        sleep_until(tx_time, busy_tail_s=busy_tail_s)

        payload_ascii = make_payload_ascii(args.robotid, frame, args.payload_bytes)
        write_cmd(ser, f"AT+SEND={args.master},{len(payload_ascii)},{payload_ascii}", verbose)

        if verbose:
            after_ms = (time.monotonic() - beacon_rx_m) * 1000.0
            print(f"[SENT] frame={frame} after_beacon={after_ms:.1f}ms RSSI={rssi} SNR={snr} (rx_delay={args.rx_delay_ms:.1f}ms)")


# ============================================================
# Main Entry Point
# ============================================================

def parse_args():
    ap = argparse.ArgumentParser("TDMA System (Master/Robot)")

    # Execution Mode
    group = ap.add_mutually_exclusive_group(required=False)
    group.add_argument("--server", action="store_true", help="Run as TDMA Master")
    group.add_argument("--client", action="store_true", help="Run as TDMA Robot")
    group.add_argument("--auto-role", action="store_true", help="Listen for beacons. If none found, become server. Otherwise, become client.")
    group.add_argument("--auto-id", action="store_true", help="Start as unassigned client, request ID dynamically from Master.")

    # Common Settings
    ap.add_argument("--port", default=None, help="Serial port: 0->/dev/ttyUSB0 or /dev/ttyUSBX")
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--master", type=int, default=1, help="Network ID of the Master")
    ap.add_argument("--band", type=int, default=915000000)
    ap.add_argument("--crfop", type=int, default=22)
    ap.add_argument("--sf", type=int, default=5)
    ap.add_argument("--bw", default="500")
    ap.add_argument("--cr", type=int, default=1)
    ap.add_argument("--preamble", type=int, default=12)
    # ap.add_argument("--slot", type=float, default=0.1)
    # ap.add_argument("--base-delay", type=float, default=0.25)
    # ap.add_argument("--tx-offset", type=float, default=0.02)
    ap.add_argument("--slot", type=float, default=0.08)
    ap.add_argument("--base-delay", type=float, default=0.2)
    ap.add_argument("--tx-offset", type=float, default=0.016)
    ap.add_argument("--quiet", action="store_true", help="Suppress verbose logging")

    # Server Settings
    ap.add_argument("--robots", default="1-10", help="Comma-separated list (e.g. 2-5) Required for --server or --auto-role or --auto-id")
    ap.add_argument("--frame", type=float, default=None, help="Frame duration (seconds). If None and --robots set, calculated automatically.")
    ap.add_argument("--warmup", type=int, default=10, help="Warmup frame ignore count (server)")
    ap.add_argument("--print-interval", type=int, default=10, help="Print summary every X frames (server)")
    ap.add_argument("--margin", type=float, default=0.03, help="Time margin for auto calculations")
    ap.add_argument("--auto-calc", action="store_true", help="Auto parameter recommendations (server)")
    ap.add_argument("--verbose-log", action="store_true", help="Verbose RX debug logging (server)")

    # Client/Auto Settings
    ap.add_argument("--robotid", type=int, required=False, help="Node ID. Required for --client or --auto-role")
    ap.add_argument("--payload-bytes", type=int, default=24, help="TX payload size in BYTES (client)")
    ap.add_argument("--rx-delay-ms", type=float, default=160.0, help="RF to serial latency compensation (client)")
    ap.add_argument("--busy-tail-ms", type=float, default=2.0, help="Precise timing busy-wait tail (client)")
    ap.add_argument("--listen-timeout", type=float, default=4.0, help="Wait time in seconds to detect an existing master for --auto-role")

    return ap.parse_args()

def listen_for_beacon(ser: serial.Serial, timeout_s: float, verbose: bool) -> bool:
    # Clear the buffer first to ensure we aren't processing stale responses
    drain_uart(ser, 0.5, verbose)
    
    # Add random jitter to the timeout so multiple nodes starting at once
    # don't all finish discovery at the exact same millisecond.
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
        if data.startswith("BCN") and len(data) >= 7:
            if verbose:
                print(f"[!] Master detected! (Beacon from {src}: {data} RSSI={rssi})")
            return True
    
    if verbose:
        print(f"[*] No master detected after {actual_timeout:.1f}s.")
    return False

def dynamic_run(args):
    port = resolve_port(args.port)
    ser = serial.Serial(port, args.baud, timeout=0.1)
    
    # Needs a generic init first so it can listen
    init_radio_robot(ser, args)  # robot init is the safest base config for listening
    
    heard_beacon = listen_for_beacon(ser, args.listen_timeout, not args.quiet)
    
    # close the temp serial so the actual run_server/run_client can open it normally and cleanly
    ser.close()
    
    if heard_beacon:
        print("====== Switching to CLIENT mode ======")
        run_client(args)
    else:
        print("====== Switching to SERVER mode ======")
        run_server(args)

def run_auto_id(args):
    # Use existing UUID or generate a new one
    my_uuid = getattr(args, 'my_uuid', None)
    if my_uuid is None:
        my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        setattr(args, 'my_uuid', my_uuid)
    
    port = resolve_port(args.port)
    
    # Temporarily set robotid to an unassigned very high value (e.g., 255)
    # just so we can initialize the radio without conflicting with normal robots.
    # The actual network allows ID 0-65535.
    original_id = args.robotid
    args.robotid = random.randint(30000, 60000) 
    
    if not args.quiet:
        print(f"[*] Starting Auto-ID process. My UUID is {my_uuid}")
        print(f"[*] Initial radio setup with temporary ID {args.robotid}")

    ser = serial.Serial(port, args.baud, timeout=0.1)
    init_radio_robot(ser, args)

    # First Phase: Auto-Role detection
    # We listen for a beacon just like --auto-role to see if a Master already exists.
    heard_beacon = listen_for_beacon(ser, args.listen_timeout, not args.quiet)
    
    if not heard_beacon:
        # NO MASTER DETECTED!
        # We must become the Master ourselves.
        # Master should always be ID 1.
        args.robotid = 1
        ser.close()
        if not args.quiet:
            print(f"====== Switching to SERVER mode (ID 1) ======")
        run_server(args)
        return

    # Second Phase: A Master exists. We must JOIN.
    assigned_id = None
    rx_delay_s = args.rx_delay_ms / 1000.0
    busy_tail_s = max(0.0, args.busy_tail_ms / 1000.0)

    # To avoid all new robots hitting the JOIN slot at the exact same microsecond, 
    # we use a simple exponential backoff or randomized wait frame count.
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

        src, ln, data, rssi, snr = r
        
        # Look for Beacon
        if not (data.startswith("BCN") and len(data) >= 7):
            continue

        # Check if the beacon contains an offer for our UUID!
        # Beacon format if offering: BCNxxxx_UUID:ID
        # e.g., BCN0012_A3F9:4
        if "_" in data and f"{my_uuid}:" in data.split("_")[1]:
            try:
                offer_part = data.split("_")[1]
                # Clean the offer part: e.g. "67A5:3+222..." -> "67A5:3"
                if "+" in offer_part:
                    offer_part = offer_part.split("+")[0]
                
                offered_id = int(offer_part.split(":")[1])
                assigned_id = offered_id
                if not args.quiet:
                    print(f"[!] SUCCESS! Master assigned me ID {assigned_id}. Applying configuration...")
                break
            except Exception as e:
                if not args.quiet:
                    print(f"[WRN] Failed to parse ID offer: {e}")
                pass
               
        # If we reach here, we heard a beacon but no offer for us.
        # It's time to send our JOIN request in the designated JOIN slot.
        # The JOIN slot is dynamically calculated as the slot AFTER the last active robot.
        if join_cooldown_frames > 0:
            join_cooldown_frames = join_cooldown_frames - 1
            continue
            
        try:
            frame_str = str(data)[3:7]
            frame = int(frame_str)
        except:
            continue
            
        read_m = time.monotonic()
        beacon_rx_m = read_m - rx_delay_s
        
        # We assume the Master expects the "JOIN" slot to be at the index of (max_robots + 1)
        # So wait until all robots are done.
        robot_list = parse_robots(args.robots)
        max_id = max(robot_list) if robot_list else 1
        
        # The JOIN slot time:
        join_tx_time = beacon_rx_m + args.base_delay + (max_id) * args.slot + args.tx_offset
        
        # Wait for the Join slot
        sleep_until(join_tx_time, busy_tail_s=busy_tail_s)
        
        # Fire the JOIN request! Format: JOIN:UUID
        join_payload = f"JOIN:{my_uuid}"
        # We send it to Master (args.master)
        write_cmd(ser, f"AT+SEND={args.master},{len(join_payload)},{join_payload}", not args.quiet)
        
        # Cooldown before trying again to avoid spamming / colliding constantly
        join_cooldown_frames = random.randint(2, 5)

    # We have an assigned ID! Apply it and switch to normal client mode.
    # Note: Since we are starting via python args, we must technically mark auto_role = True 
    # so that the failover logic inside run_client() still applies to us.
    args.auto_role = True
    ser.close()
    
    args.robotid = assigned_id
    print(f"====== Transitioning to Normal CLIENT Mode as ID {assigned_id} ======")
    run_client(args)

def main():
    args = parse_args()
    
    # Generate a unique session UUID for tie-breaking and DHCP
    setattr(args, 'my_uuid', f"{random.randint(0, 0xFFFF):04X}")
    
    if args.server:
        if not args.robots:
            print("[ERR] --robots is required when running as --server")
            sys.exit(2)
        run_server(args)
    elif args.client:
        if not args.robotid:
            print("[ERR] --robotid is required when running as --client")
            sys.exit(2)
        if args.robotid <= 0 or args.robotid > 15:
            print("[ERR] --robotid must be 1..15 for the 1-character HEX ID format.")
            sys.exit(2)
        if args.payload_bytes < 3:
            print("[ERR] --payload-bytes must be >= 3")
            sys.exit(2)
        run_client(args)
    elif args.auto_role:
        if not args.robotid or not args.robots:
            print("[ERR] Both --robotid and --robots are required when running as --auto-role")
            sys.exit(2)
        dynamic_run(args)
    elif args.auto_id:
        if not args.robots:
            print("[ERR] --robots is required when running as --auto-id, so the bot knows the network size bounds.")
            sys.exit(2)
        run_auto_id(args)
    else:
        # Default behavior: auto-id
        if not args.quiet:
            print("[INFO] No mode specified. Defaulting to --auto-id")
        run_auto_id(args)

if __name__ == "__main__":
    main()
