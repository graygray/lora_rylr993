#!/usr/bin/env python3
import argparse
import serial
import time
import math
from dataclasses import dataclass
from typing import Dict, List

# ============================================================
# Utility
# ============================================================

# ============================================================
# Shared Utilities & Beacon Parsing
# ============================================================

def parse_beacon(data: str):
    """
    Parses structured beacon: BCN,frame,uuid,offer,relay...
    Returns: {frame, uuid, offer_uuid, offer_id, relay_str} or None
    """
    if not data.startswith("BCN,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) < 2:
            return None
            
        res = {
            "frame": int(parts[1]),
            "uuid": parts[2] if len(parts) > 2 else "",
            "offer_uuid": None,
            "offer_id": None,
            "relay_str": ""
        }
        
        # Parse offer: uuid:id
        if len(parts) > 3 and ":" in parts[3]:
            o_parts = parts[3].split(":", 1)
            res["offer_uuid"] = o_parts[0]
            res["offer_id"] = int(o_parts[1])
            
        # Relays: everything from index 4 onwards
        if len(parts) > 4:
            # We keep relay_str as a delimiter-separated string for compatibility
            res["relay_str"] = "+".join(parts[4:])
            
        return res
    except:
        return None

def make_beacon(frame_id: int, uuid: str = "", offer_uuid: str = None, offer_id: int = None, relay_map: Dict[int, str] = None, max_len: int = 242) -> str:
    """
    Constructs a structured beacon: BCN,frame,uuid,offer_uuid:offer_id,relay_id:data,relay_id:data...
    Iteratively adds relay items while within max_len.
    """
    offer_str = ""
    if offer_uuid and offer_id is not None:
        offer_str = f"{offer_uuid}:{offer_id}"
    
    parts = ["BCN", f"{frame_id:04d}", uuid or "", offer_str]
    current_beacon = ",".join(parts)
    
    if relay_map:
        for rid in sorted(relay_map.keys()):
            item = f"{rid:x}:{relay_map[rid]}"
            potential_len = len(current_beacon) + 1 + len(item)
            if potential_len <= max_len:
                parts.append(item)
                current_beacon = ",".join(parts)
            else:
                break
                
    return current_beacon

def parse_rcv(line: str):
    if not line.startswith("+RCV="):
        return None
    try:
        p = line[5:].split(",", 4)
        src = int(p[0]); ln = int(p[1]); data = p[2]
        rssi = int(p[3]); snr = int(p[4])
        return src, ln, data, rssi, snr
    except:
        return None

def parse_payload_hex(data_hex: str):
    if not data_hex or len(data_hex) < 2:
        return None
    try:
        # Format: 2 hex char Seq (00-FF)
        fid = int(data_hex[0:2], 16)
        return fid
    except:
        return None

def parse_bw_to_code(bw: str) -> int:
    s = str(bw).lower().strip()
    if s in ("7", "8", "9"):
        return int(s)
    if s.endswith("k"):
        s = s[:-1]
    if s in ("125", "125000"):
        return 7
    if s in ("250", "250000"):
        return 8
    if s in ("500", "500000"):
        return 9
    raise ValueError("Invalid BW")

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

def resolve_port(port):
    if port is None:
        return "/dev/ttyUSB0"
    if port.isdigit():
        return f"/dev/ttyUSB{port}"
    return port

def write_cmd(ser, cmd, verbose=True):
    if verbose:
        print(f"[TX] {cmd}")
    ser.write((cmd + "\r\n").encode())

def read_line(ser):
    return ser.readline().decode(errors="ignore").strip()

def now_ms(start):
    return (time.monotonic() - start) * 1000.0

# ============================================================
# Stats
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

# ============================================================
# Auto calculations
# ============================================================

def auto_slot(frame, base, offset, robots, margin, slot_offset_stats, jitter_s=0.0018):
    vals = [slot_offset_stats[r].max_v for r in robots if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else jitter_s
    
    num_robots = len(robots)
    denom = num_robots - 1
    if denom <= 0:
        return 0
    budget = frame - margin - base - offset - worst_offset
    if budget <= 0:
        return 0
    return budget / denom

def auto_frame(slot, base, offset, robots, margin, slot_offset_stats, jitter_s=0.0018):
    vals = [slot_offset_stats[r].max_v for r in robots if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else jitter_s
    num_robots = len(robots)
    return base + (num_robots - 1)*slot + offset + worst_offset + margin

def auto_max_robots(frame, slot, base, offset, margin, slot_offset_stats, jitter_s=0.0018):
    vals = [slot_offset_stats[r].max_v for r in slot_offset_stats if math.isfinite(slot_offset_stats[r].max_v)]
    worst_offset = max(vals) / 1000.0 if vals else jitter_s
    budget = frame - margin - base - offset - worst_offset
    if budget <= 0:
        return 1
    return int(budget / slot) + 1

def lora_airtime_seconds(sf, bw_hz, cr, preamble, payload_bytes):
    """
    Precise LoRa airtime calculation (seconds)
    """

    bw = bw_hz
    tsym = (2 ** sf) / bw

    # Low data rate optimization
    de = 1 if (sf >= 11 and bw == 125000) else 0
    ih = 0   # implicit header disabled
    crc = 1  # CRC enabled

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

# ============================================================
# Radio Init (same behavior)
# ============================================================

def wait_ready(ser: serial.Serial, timeout_s: float = 4.0, verbose: bool = True):
    start_t = time.monotonic()
    while (time.monotonic() - start_t) < timeout_s:
        l = ser.readline().decode(errors="ignore").strip()
        if not l: continue
        if verbose: print("[RADIO]", l)
        if "+READY" in l: return True
    return False

def init_radio(ser, args):
    verbose = True
    write_cmd(ser, "AT")
    time.sleep(0.3)
    write_cmd(ser, "AT+OPMODE=1")
    time.sleep(0.5)

    need_reset = False
    # Check for "Need RESET"
    ser.timeout = 0.5
    for _ in range(5):
        line = ser.readline().decode(errors="ignore").strip()
        if not line: break
        if verbose: print("[RADIO]", line)
        if "Need RESET" in line:
            need_reset = True
            break
    
    if need_reset:
        write_cmd(ser, "ATZ")
        wait_ready(ser, 4.0, verbose)

    write_cmd(ser, f"AT+ADDRESS={args.master}")
    time.sleep(0.5)
    write_cmd(ser, f"AT+BAND={args.band}")
    time.sleep(0.5)
    bw_code = parse_bw_to_code(args.bw)
    write_cmd(ser, f"AT+PARAMETER={args.sf},{bw_code},{args.cr},{args.preamble}")
    time.sleep(0.5)
    write_cmd(ser, f"AT+CRFOP={args.crfop}")
    time.sleep(0.5)
    # Drain any leftovers
    ser.reset_input_buffer()

# ============================================================
# Main
# ============================================================

def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--port", default=None)
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--master", type=int, default=1)
    ap.add_argument("--band", type=int, default=915000000)
    ap.add_argument("--crfop", type=int, default=22)

    ap.add_argument("--sf", type=int, default=5)
    ap.add_argument("--bw", default="500")
    ap.add_argument("--cr", type=int, default=1)
    ap.add_argument("--preamble", type=int, default=12)

    ap.add_argument("--robots", required=True)

    ap.add_argument("--frame", type=float, default=1.5)
    ap.add_argument("--slot", type=float, default=0.1)
    ap.add_argument("--base-delay", type=float, default=0.25)
    ap.add_argument("--tx-offset", type=float, default=0.02)
    ap.add_argument("--payload-bytes", type=int, default=32, help="Payload size for airtime estimation")

    ap.add_argument("--warmup", type=int, default=8)
    ap.add_argument("--print-interval", type=int, default=20)
    ap.add_argument("--margin", type=float, default=0.03)
    ap.add_argument("--assumed-jitter-ms", type=float, default=1.8, help="Expected timing jitter/error (ms)")
    ap.add_argument("--auto", action="store_true")
    ap.add_argument("--verbose-log", action="store_true")

    args = ap.parse_args()

    robots = parse_robots(args.robots)
    port = resolve_port(args.port)
    ser = serial.Serial(port, args.baud, timeout=0.1)

    init_radio(ser, args)

    per_expected: Dict[int, int] = {r: 0 for r in robots}
    per_ok: Dict[int, int] = {r: 0 for r in robots}
    per_mismatch: Dict[int, int] = {r: 0 for r in robots}

    robot_order = {rid: i for i, rid in enumerate(sorted(robots))}
    slot_offset_stats: Dict[int, Stats] = {r: Stats() for r in robots}
    beacon_to_rx_stats: Dict[int, Stats] = {r: Stats() for r in robots}

    frame_id = 0

    print("TDMA MASTER START")

    while True:
        frame_id += 1
        start = time.monotonic()

        if frame_id > 9999:
            frame_id = frame_id % 10000
        beacon = make_beacon(frame_id)
        write_cmd(ser, f"AT+SEND=0,{len(beacon)},{beacon}")

        listen_end = start + args.frame - 0.01
        received = set()

        while time.monotonic() < listen_end:
            line = read_line(ser)
            r = parse_rcv(line)
            if not r:
                continue

            src, ln, data, rssi, snr = r

            if src not in robots:
                continue

            slot_index = robot_order[src]
            t = now_ms(start)
            expected = (args.base_delay + slot_index * args.slot + args.tx_offset) * 1000.0
            slot_offset = t - expected

            # Stats collection
            slot_offset_stats[src].add(slot_offset)
            beacon_to_rx_stats[src].add(t)

            fid = parse_payload_hex(data)
            if fid is None:
                if args.verbose_log:
                    print(f"[RX BAD] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms slot_offset={slot_offset:.1f}ms raw={data[:16]}...")
                continue

            # NOTE: src is LoRa ADDRESS; use src for routing (keep your PER structure)
            # NOTE: robots send an 8-bit frame sequence number (fid)
            if fid == (frame_id % 256):
                received.add(src)
                if frame_id > args.warmup:
                    per_ok[src] += 1
                if args.verbose_log:
                    print(f"[RX OK] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms slot_offset={slot_offset:.1f}ms")
            else:
                if frame_id > args.warmup:
                    per_mismatch[src] += 1
                if args.verbose_log:
                    print(f"[RX MISMATCH] expect={frame_id % 256} got={fid} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms slot_offset={slot_offset:.1f}ms")

        if frame_id > args.warmup:
            for r in robots:
                per_expected[r] += 1

        if frame_id % args.print_interval == 0:
            print("\n====== PER SUMMARY ======")
            total_ok = 0
            total_e = 0
            
            jitter_s = args.assumed_jitter_ms / 1000.0

            for r in robots:
                e = per_expected[r]
                ok = per_ok[r]
                per = (e-ok)/e if e else 0
                print(f"Robot {r}: OK={ok}/{e} PER={per*100:.3f}% mismatch={per_mismatch[r]}")
                print("  BEACON_TO_RX:", beacon_to_rx_stats[r].fmt())
                print("  SLOT_OFFSET :", slot_offset_stats[r].fmt())
                total_ok += ok
                total_e += e

            gper = 1 - total_ok/total_e if total_e else 0
            print(f"GLOBAL PER = {gper*100:.3f}%")

            if args.auto:

                rec_slot = auto_slot(args.frame, args.base_delay,
                                     args.tx_offset, robots,
                                     args.margin, slot_offset_stats, jitter_s)

                rec_frame = auto_frame(args.slot, args.base_delay,
                                       args.tx_offset, robots,
                                       args.margin, slot_offset_stats, jitter_s)

                rec_max = auto_max_robots(args.frame, args.slot,
                                          args.base_delay,
                                          args.tx_offset,
                                          args.margin,
                                          slot_offset_stats, jitter_s)

                print("\n---- AUTO CALC ----")
                print(f"Recommended slot <= {rec_slot*1000:.1f} ms")
                print(f"Minimum frame needed ≈ {rec_frame*1000:.1f} ms")
                print(f"Max robots supportable ≈ {rec_max}")

                # =========================
                # NEW: Minimum slot estimation
                # =========================

                # Estimate airtime precisely
                bw_hz = 500000 if str(args.bw) in ("9", "500", "500k", "500000") else \
                        250000 if str(args.bw) in ("8", "250", "250k", "250000") else \
                        125000

                airtime_s = lora_airtime_seconds(
                    sf=args.sf,
                    bw_hz=bw_hz,
                    cr=args.cr,
                    preamble=args.preamble,
                    payload_bytes=args.payload_bytes
                )

                # ----- derive offset span safely from existing stats -----
                offset_span_s = 0.0
                offset_min_s = float("inf")
                offset_max_s = float("-inf")
                for r in robots:
                    if slot_offset_stats[r].n > 0:
                        if slot_offset_stats[r].min_v < offset_min_s: offset_min_s = slot_offset_stats[r].min_v
                        if slot_offset_stats[r].max_v > offset_max_s: offset_max_s = slot_offset_stats[r].max_v

                try:
                    if math.isfinite(offset_max_s) and math.isfinite(offset_min_s):
                        offset_span_s = max(0.0, (offset_max_s - offset_min_s)/1000.0)
                except:
                    offset_span_s = 0.0

                guard_s = 0.010  # 10ms safety margin

                min_slot_s = airtime_s + offset_span_s + guard_s

                print(f"Minimum slot needed ≥ {min_slot_s*1000:.1f} ms")

                print("-------------------")

            print("========================\n")

if __name__ == "__main__":
    main()