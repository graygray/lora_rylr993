#!/usr/bin/env python3
import argparse
import serial
import time
import math
import sys
from dataclasses import dataclass
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
    if not data_hex or len(data_hex) < 6:
        return None
    try:
        rid = int(data_hex[0:2], 16)
        fid = int(data_hex[2:6], 16)
        return rid, fid
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
    port = resolve_port(args.port)
    bw_code = parse_bw_to_code(args.bw)

    if verbose:
        print(f"[CFG] PORT={port} BAUD={args.baud} MASTER={args.master} "
              f"sf={args.sf} bw_code={bw_code} cr={args.cr} pre={args.preamble} "
              f"slot={args.slot:.3f}s base={args.base_delay:.3f}s off={args.tx_offset:.3f}s "
              f"frame={args.frame:.3f}s robots={args.robots}")

    ser = serial.Serial(port, args.baud, timeout=0.1)

    init_radio_master(ser, args)

    per_expected: Dict[int, int] = {r: 0 for r in robots}
    per_ok: Dict[int, int] = {r: 0 for r in robots}
    per_mismatch: Dict[int, int] = {r: 0 for r in robots}

    jitter_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    rtt_stats: Dict[int, Stats] = {r: Stats() for r in robots}

    frame_id = 0
    print("TDMA MASTER START")

    while True:
        frame_id += 1
        start = time.monotonic()

        if frame_id > 9999:
            frame_id = frame_id % 10000
        beacon = f"BCN{frame_id:04d}"
        write_cmd(ser, f"AT+SEND=0,{len(beacon)},{beacon}", verbose=not args.quiet)

        listen_end = start + args.frame - 0.01
        received = set()

        while time.monotonic() < listen_end:
            line = read_line(ser)
            if not line.startswith("+RCV="):
                continue

            parts = line[5:].split(",")
            src = int(parts[0])
            data = parts[2]

            # Dual-server conflict resolution:
            # If we are the Server but we hear a Beacon from someone else...
            if data.startswith("BCN") and len(data) >= 7:
                try:
                    other_frame = int(data[3:7])
                    if verbose:
                        print(f"[!] WARNING: Heard BCN{other_frame:04d} from ID {src}! Dual-server conflict detected.")
                    
                    # If the other server has a LOWER ID than us (higher priority), we yield.
                    # As a fail-safe, if IDs are the same (shouldn't happen), we just yield to stop the jam.
                    # Exception: If we are ID 1, we NEVER yield. ID 1 is the supreme master.
                    if args.robotid != 1 and (src <= args.robotid or src == 1):
                        print(f"====== Yielding Master Role to ID {src}. Switching to CLIENT mode ======")
                        ser.close()
                        run_client(args)
                        return
                    else:
                        if verbose:
                            print(f"[*] I have higher priority (my ID: {args.robotid}). Ignoring imposter ID {src}.")
                except ValueError:
                    pass
                continue

            if src not in robots:
                continue

            t = now_ms(start)
            expected = (args.base_delay + (src-1)*args.slot + args.tx_offset) * 1000.0
            jitter = t - expected

            jitter_stats[src].add(jitter)
            rtt_stats[src].add(t)

            pp = parse_payload_hex(data)
            if not pp:
                if args.verbose_log:
                    print(f"[RX BAD] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms raw={data[:16]}...")
                continue

            rid, fid = pp
            if fid == frame_id:
                received.add(src)
                if frame_id > args.warmup:
                    per_ok[src] += 1
                if args.verbose_log:
                    print(f"[RX OK] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms rid={rid}")
            else:
                if frame_id > args.warmup:
                    per_mismatch[src] += 1
                if args.verbose_log:
                    print(f"[RX MISMATCH] expect={frame_id} got={fid} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms rid={rid}")

        if frame_id > args.warmup:
            for r in robots:
                per_expected[r] += 1

        if frame_id % args.print_interval == 0:
            print("\n====== PER SUMMARY ======")
            total_ok = 0
            total_e = 0
            for r in robots:
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

            if args.auto:
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
                    sf=args.sf, bw_hz=bw_hz, cr=args.cr, preamble=args.preamble, payload_bytes=3
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
    if payload_bytes < 6:
        raise ValueError("--payload-bytes must be >= 6.")
    header = f"{my_id:02x}{frame_id:04x}"
    dummy_len = payload_bytes - 6
    return header + ("a" * dummy_len)

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
    bw_code = parse_bw_to_code(args.bw)
    port = resolve_port(args.port)

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
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--server", action="store_true", help="Run as TDMA Master")
    group.add_argument("--client", action="store_true", help="Run as TDMA Robot")
    group.add_argument("--auto-role", action="store_true", help="Listen for beacons. If none found, become server. Otherwise, become client.")

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
    ap.add_argument("--slot", type=float, default=0.1)
    ap.add_argument("--base-delay", type=float, default=0.25)
    ap.add_argument("--tx-offset", type=float, default=0.02)
    ap.add_argument("--quiet", action="store_true", help="Suppress verbose logging")

    # Server Settings
    ap.add_argument("--robots", required=False, help="Comma-separated list (e.g. 2-5) Required for --server or --auto-role")
    ap.add_argument("--frame", type=float, default=1.5, help="Frame duration in seconds")
    ap.add_argument("--warmup", type=int, default=8, help="Warmup frame ignore count (server)")
    ap.add_argument("--print-interval", type=int, default=20, help="Print summary every X frames (server)")
    ap.add_argument("--margin", type=float, default=0.03, help="Time margin for auto calculations")
    ap.add_argument("--auto", action="store_true", help="Auto parameter recommendations (server)")
    ap.add_argument("--verbose-log", action="store_true", help="Verbose RX debug logging (server)")

    # Client/Auto Settings
    ap.add_argument("--robotid", type=int, required=False, help="Node ID. Required for --client or --auto-role")
    ap.add_argument("--payload-bytes", type=int, default=32, help="TX payload size in BYTES (client)")
    ap.add_argument("--rx-delay-ms", type=float, default=175.0, help="RF to serial latency compensation (client)")
    ap.add_argument("--busy-tail-ms", type=float, default=2.0, help="Precise timing busy-wait tail (client)")
    ap.add_argument("--listen-timeout", type=float, default=4.0, help="Wait time in seconds to detect an existing master for --auto-role")

    return ap.parse_args()

def listen_for_beacon(ser: serial.Serial, timeout_s: float, verbose: bool) -> bool:
    if verbose:
        print(f"[*] Listening for existing master beacons for {timeout_s:.1f}s...")
    
    end = time.time() + timeout_s
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
        print(f"[*] No master detected after {timeout_s:.1f}s.")
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

def main():
    args = parse_args()
    
    if args.server:
        if not args.robots:
            print("[ERR] --robots is required when running as --server")
            sys.exit(2)
        run_server(args)
    elif args.client:
        if not args.robotid:
            print("[ERR] --robotid is required when running as --client")
            sys.exit(2)
        if args.robotid <= 0 or args.robotid > 65535:
            print("[ERR] --robotid must be 1..65535")
            sys.exit(2)
        if args.payload_bytes < 6:
            print("[ERR] --payload-bytes must be >= 6")
            sys.exit(2)
        run_client(args)
    elif args.auto_role:
        if not args.robotid or not args.robots:
            print("[ERR] Both --robotid and --robots are required when running as --auto-role")
            sys.exit(2)
        dynamic_run(args)

if __name__ == "__main__":
    main()
