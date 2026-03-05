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

def parse_payload_hex(data_hex: str):
    """
    robot payload hex format:
      rid: 1 byte  (2 hex chars)
      fid: 2 bytes (4 hex chars, big-endian)
    total >= 6 hex chars
    """
    if not data_hex or len(data_hex) < 6:
        return None
    try:
        rid = int(data_hex[0:2], 16)
        fid = int(data_hex[2:6], 16)
        return rid, fid
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

def auto_slot(frame, base, offset, robots, margin, jitter_stats):
    worst_j = max(jitter_stats[r].max_v for r in robots) / 1000.0
    max_id = max(robots)
    denom = max_id - 1
    budget = frame - margin - base - offset - worst_j
    if budget <= 0:
        return 0
    return budget / denom

def auto_frame(slot, base, offset, robots, margin, jitter_stats):
    worst_j = max(jitter_stats[r].max_v for r in robots) / 1000.0
    max_id = max(robots)
    return base + (max_id - 1)*slot + offset + worst_j + margin

def auto_max_robots(frame, slot, base, offset, margin, jitter_stats):
    worst_j = max(jitter_stats[r].max_v for r in jitter_stats) / 1000.0
    budget = frame - margin - base - offset - worst_j
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

def init_radio(ser, args):
    write_cmd(ser, "AT")
    time.sleep(0.3)

    write_cmd(ser, "AT+OPMODE=1")
    time.sleep(0.5)

    write_cmd(ser, "ATZ")
    time.sleep(2.0)

    write_cmd(ser, f"AT+ADDRESS={args.master}")
    time.sleep(0.5)

    write_cmd(ser, f"AT+BAND={args.band}")
    time.sleep(0.5)

    bw_code = parse_bw_to_code(args.bw)
    write_cmd(ser, f"AT+PARAMETER={args.sf},{bw_code},{args.cr},{args.preamble}")
    time.sleep(0.5)

    write_cmd(ser, f"AT+CRFOP={args.crfop}")
    time.sleep(0.5)

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

    ap.add_argument("--warmup", type=int, default=8)
    ap.add_argument("--print-interval", type=int, default=20)
    ap.add_argument("--margin", type=float, default=0.03)
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
        write_cmd(ser, f"AT+SEND=0,{len(beacon)},{beacon}")

        listen_end = start + args.frame - 0.01
        received = set()

        while time.monotonic() < listen_end:
            line = read_line(ser)
            if not line.startswith("+RCV="):
                continue

            parts = line[5:].split(",")
            src = int(parts[0])
            data = parts[2]

            if src not in robots:
                continue

            t = now_ms(start)
            expected = (args.base_delay + (src-1)*args.slot + args.tx_offset) * 1000.0
            jitter = t - expected

            # RTT/JITTER always collect (same as now)
            jitter_stats[src].add(jitter)
            rtt_stats[src].add(t)

            pp = parse_payload_hex(data)
            if not pp:
                if args.verbose_log:
                    print(f"[RX BAD] frame={frame_id} from={src} "
                          f"t={t:.1f}ms exp={expected:.1f}ms jitter={jitter:.1f}ms raw={data[:16]}...")
                continue

            rid, fid = pp

            # NOTE: src is LoRa ADDRESS; use src for routing (keep your PER structure)
            # rid is embedded by robot; we can sanity-check but don't rely on it
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

                rec_slot = auto_slot(args.frame, args.base_delay,
                                     args.tx_offset, robots,
                                     args.margin, jitter_stats)

                rec_frame = auto_frame(args.slot, args.base_delay,
                                       args.tx_offset, robots,
                                       args.margin, jitter_stats)

                rec_max = auto_max_robots(args.frame, args.slot,
                                          args.base_delay,
                                          args.tx_offset,
                                          args.margin,
                                          jitter_stats)

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
                    payload_bytes=3
                )

                # ----- derive jitter span safely from existing stats -----

                jitter_span_s = 0.0

                try:
                    if math.isfinite(jitter_max_s) and math.isfinite(jitter_min_s):
                        jitter_span_s = max(0.0, jitter_max_s - jitter_min_s)
                except:
                    jitter_span_s = 0.0

                guard_s = 0.010  # 10ms safety margin

                min_slot_s = airtime_s + jitter_span_s + guard_s

                print(f"Minimum slot needed ≥ {min_slot_s*1000:.1f} ms")

                print("-------------------")

            print("========================\n")

if __name__ == "__main__":
    main()